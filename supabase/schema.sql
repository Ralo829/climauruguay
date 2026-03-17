-- Tabla pronósticos
CREATE TABLE IF NOT EXISTS nubel_pronosticos (
  id              BIGSERIAL PRIMARY KEY,
  fecha_dia       DATE        NOT NULL,
  fecha_pronostico DATE       NOT NULL,
  zona            TEXT        NOT NULL,
  max_nubel       NUMERIC(4,1),
  min_nubel       NUMERIC(4,1),
  url_fuente      TEXT,
  created_at      TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE (fecha_dia, zona)
);

-- Tabla datos reales
CREATE TABLE IF NOT EXISTS datos_reales (
  id         BIGSERIAL PRIMARY KEY,
  fecha      DATE        NOT NULL,
  zona       TEXT        NOT NULL,
  max_real   NUMERIC(4,1),
  min_real   NUMERIC(4,1),
  fuente     TEXT DEFAULT 'AccuWeather',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE (fecha, zona)
);

-- Tabla log del scraper
CREATE TABLE IF NOT EXISTS scraper_log (
  id           BIGSERIAL PRIMARY KEY,
  ejecutado_en TIMESTAMPTZ DEFAULT NOW(),
  estado       TEXT,
  pronosticos_insertados INT DEFAULT 0,
  reales_insertados      INT DEFAULT 0,
  detalle      TEXT
);
