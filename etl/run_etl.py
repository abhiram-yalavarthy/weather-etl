import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import sys
import time
import logging
import csv

# ---- Logging ----
log_path = os.path.join(os.path.dirname(__file__), "etl.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ---- Config ----
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
SOURCE_NAME = os.getenv("SOURCE_NAME", "open-meteo")
LATITUDE = float(os.getenv("LATITUDE", "40.7128"))
LONGITUDE = float(os.getenv("LONGITUDE", "-74.0060"))
START_DAYS_AGO = int(os.getenv("START_DAYS_AGO", "0"))
END_DAYS_AGO = int(os.getenv("END_DAYS_AGO", "0"))

if not DATABASE_URL:
    logger.error("DATABASE_URL not set. Copy .env.example -> .env and edit.")
    sys.exit(1)

# ---- Helpers ----
def build_open_meteo_url(lat, lon, start_date, end_date):
    base = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,precipitation,windspeed_10m",
        "timezone": "UTC",
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d")
    }
    q = "&".join(f"{k}={params[k]}" for k in params)
    return f"{base}?{q}"

def extract(lat, lon, start_date, end_date, retries=3, backoff=2):
    url = build_open_meteo_url(lat, lon, start_date, end_date)
    logger.info(f"Extracting data from Open-Meteo: {url}")
    attempt = 0
    while attempt < retries:
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            attempt += 1
            logger.warning(f"Attempt {attempt} failed: {e}")
            time.sleep(backoff * attempt)
    logger.error("Failed to fetch data after retries")
    raise RuntimeError("Extraction failed")

def transform(raw_json, lat, lon, source):
    hourly = raw_json.get("hourly", {})
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    precips = hourly.get("precipitation", [])
    wind = hourly.get("windspeed_10m", [])

    rows = []
    for i, t in enumerate(times):
        temp = temps[i] if i < len(temps) else None
        precip = precips[i] if i < len(precips) else None
        windspeed = wind[i] if i < len(wind) else None
        rows.append({
            "observation_time": pd.to_datetime(t, utc=True),
            "latitude": float(lat),
            "longitude": float(lon),
            "temperature_c": None if temp is None else float(temp),
            "precipitation_mm": None if precip is None else float(precip),
            "windspeed_mps": None if windspeed is None else float(windspeed),
            "source": source
        })

    df = pd.DataFrame(rows)
    df = df.dropna(subset=["observation_time"])
    df["observation_time"] = df["observation_time"].dt.tz_convert("UTC").dt.tz_localize(None)
    logger.info(f"Transformed {len(df)} rows")
    return df

# stronger data-quality checks
def data_quality_check(df):
    issues = []
    if df.empty:
        issues.append("Transformed dataframe is empty")
        return issues

    # duplicates in transformed
    dup_count = df.duplicated(subset=["observation_time", "latitude", "longitude", "source"]).sum()
    if dup_count > 0:
        issues.append(f"{dup_count} duplicate rows in transformed data")

    # reasonable temperature & precipitation ranges
    if df["temperature_c"].dropna().apply(lambda x: not (-90 <= x <= 60)).any():
        issues.append("Temperature outside plausible range (-90C to 60C)")
    if df["precipitation_mm"].dropna().apply(lambda x: x < 0 or x > 1000).any():
        issues.append("Precipitation outside plausible range (0-1000 mm)")

    # monotonic timestamps and continuity
    times = df["observation_time"].sort_values()
    if not times.is_monotonic_increasing:
        issues.append("Observation times are not strictly increasing")

    # small completeness check: at least 18 hours of 24 present
    if len(df) < 18:
        issues.append(f"Only {len(df)} rows; expected ~24 hourly rows")

    return issues

# create table if not exists (supports sqlite and postgres)
def ensure_table_exists(engine):
    dialect = engine.dialect.name
    if dialect.startswith("postgres"):
        create_sql = """
        CREATE TABLE IF NOT EXISTS weather_observations (
          id SERIAL PRIMARY KEY,
          observation_time TIMESTAMP NOT NULL,
          latitude DOUBLE PRECISION NOT NULL,
          longitude DOUBLE PRECISION NOT NULL,
          temperature_c DOUBLE PRECISION,
          windspeed_mps DOUBLE PRECISION,
          precipitation_mm DOUBLE PRECISION,
          source TEXT,
          ingestion_time TIMESTAMP NOT NULL DEFAULT now(),
          UNIQUE (observation_time, latitude, longitude, source)
        );
        """
    else:
        create_sql = """
        CREATE TABLE IF NOT EXISTS weather_observations (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          observation_time TIMESTAMP NOT NULL,
          latitude DOUBLE PRECISION NOT NULL,
          longitude DOUBLE PRECISION NOT NULL,
          temperature_c DOUBLE PRECISION,
          windspeed_mps DOUBLE PRECISION,
          precipitation_mm DOUBLE PRECISION,
          source TEXT,
          ingestion_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    with engine.begin() as conn:
        conn.execute(text(create_sql))
        if engine.dialect.name == "sqlite":
            conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_weather_obs_unique
            ON weather_observations (observation_time, latitude, longitude, source);
            """))

def load_to_db(df, engine):
    if df.empty:
        logger.info("No rows to load")
        return 0

    dialect = engine.dialect.name
    count = 0
    if dialect.startswith("postgres"):
        insert_sql = """
        INSERT INTO weather_observations
          (observation_time, latitude, longitude, temperature_c, windspeed_mps, precipitation_mm, source, ingestion_time)
        VALUES (:observation_time, :latitude, :longitude, :temperature_c, :windspeed_mps, :precipitation_mm, :source, now())
        ON CONFLICT (observation_time, latitude, longitude, source)
        DO UPDATE SET
          temperature_c = EXCLUDED.temperature_c,
          windspeed_mps = EXCLUDED.windspeed_mps,
          precipitation_mm = EXCLUDED.precipitation_mm,
          ingestion_time = now()
        """
        with engine.begin() as conn:
            for _, row in df.iterrows():
                params = {
                    "observation_time": row["observation_time"].to_pydatetime(),
                    "latitude": float(row["latitude"]),
                    "longitude": float(row["longitude"]),
                    "temperature_c": None if pd.isna(row["temperature_c"]) else float(row["temperature_c"]),
                    "windspeed_mps": None if pd.isna(row["windspeed_mps"]) else float(row["windspeed_mps"]),
                    "precipitation_mm": None if pd.isna(row["precipitation_mm"]) else float(row["precipitation_mm"]),
                    "source": row["source"]
                }
                conn.execute(text(insert_sql), params)
                count += 1
    else:
        insert_sql = """
        INSERT OR REPLACE INTO weather_observations
          (id, observation_time, latitude, longitude, temperature_c, windspeed_mps, precipitation_mm, source, ingestion_time)
        VALUES (
          (SELECT id FROM weather_observations WHERE observation_time=:observation_time AND latitude=:latitude AND longitude=:longitude AND source=:source),
          :observation_time, :latitude, :longitude, :temperature_c, :windspeed_mps, :precipitation_mm, :source, CURRENT_TIMESTAMP
        );
        """
        with engine.begin() as conn:
            for _, row in df.iterrows():
                params = {
                    "observation_time": row["observation_time"].to_pydatetime(),
                    "latitude": float(row["latitude"]),
                    "longitude": float(row["longitude"]),
                    "temperature_c": None if pd.isna(row["temperature_c"]) else float(row["temperature_c"]),
                    "windspeed_mps": None if pd.isna(row["windspeed_mps"]) else float(row["windspeed_mps"]),
                    "precipitation_mm": None if pd.isna(row["precipitation_mm"]) else float(row["precipitation_mm"]),
                    "source": row["source"]
                }
                conn.execute(text(insert_sql), params)
                count += 1

    logger.info(f"Loaded {count} rows")
    return count

def save_snapshot_csv(df):
    snapshot_dir = os.path.join(os.path.dirname(__file__), "snapshots")
    os.makedirs(snapshot_dir, exist_ok=True)
    fname = os.path.join(snapshot_dir, f"snapshot_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv")
    df.to_csv(fname, index=False)
    logger.info(f"Saved CSV snapshot: {fname}")
    return fname

def main():
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    ensure_table_exists(engine)

    end_date = datetime.utcnow().date() - timedelta(days=END_DAYS_AGO)
    start_date = datetime.utcnow().date() - timedelta(days=START_DAYS_AGO)
    logger.info(f"Fetching data from {start_date} to {end_date}")

    raw = extract(LATITUDE, LONGITUDE, start_date, end_date)
    df = transform(raw, LATITUDE, LONGITUDE, SOURCE_NAME)

    issues = data_quality_check(df)
    if issues:
        logger.warning("Data quality issues found:")
        for it in issues:
            logger.warning(" - " + it)
    else:
        logger.info("Basic data quality checks passed")

    # Save a snapshot for audit
    save_snapshot_csv(df)

    loaded = load_to_db(df, engine)
    logger.info("ETL finished")

if __name__ == "__main__":
    main()