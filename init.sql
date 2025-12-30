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
