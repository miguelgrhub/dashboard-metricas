-- Tablas de agregados recomendadas

CREATE TABLE IF NOT EXISTS metrics_daily (
  metric_date DATE PRIMARY KEY,
  total_rows BIGINT NOT NULL,
  with_email BIGINT NOT NULL,
  valid_emails BIGINT NOT NULL,
  invalid_emails BIGINT NOT NULL,
  duplicates_extra_rows BIGINT NOT NULL,
  unique_valid_emails BIGINT NOT NULL,
  sendable_emails BIGINT NOT NULL,
  total_opens BIGINT DEFAULT 0,
  total_clicks BIGINT DEFAULT 0,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS metrics_top_domains_daily (
  metric_date DATE NOT NULL,
  domain VARCHAR(255) NOT NULL,
  cnt BIGINT NOT NULL,
  PRIMARY KEY (metric_date, domain),
  INDEX idx_mtd_date (metric_date)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS metrics_repeated_emails (
  email VARCHAR(320) PRIMARY KEY,
  occurrences BIGINT NOT NULL,
  first_seen DATE NULL,
  last_seen DATE NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;
