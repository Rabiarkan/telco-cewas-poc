CREATE SCHEMA IF NOT EXISTS fs;

DROP TABLE IF EXISTS fs.customer_features_snapshot;

CREATE TABLE fs.customer_features_snapshot (
  as_of_date DATE NOT NULL,
  customer_id TEXT NOT NULL,

  -- label
  churn INT,

  -- numeric features
  tenure INT,
  monthly_charges DOUBLE PRECISION,
  total_charges DOUBLE PRECISION,

  -- customer demographics
  gender TEXT,
  senior_citizen INT,
  partner TEXT,
  dependents TEXT,

  -- contract & billing
  contract TEXT,
  paperless_billing TEXT,
  payment_method TEXT,

  -- services
  internet_service TEXT,
  phone_service TEXT,
  multiple_lines TEXT,
  online_security TEXT,
  online_backup TEXT,
  device_protection TEXT,
  tech_support TEXT,
  streaming_tv TEXT,
  streaming_movies TEXT,

  PRIMARY KEY (as_of_date, customer_id)
);

CREATE INDEX IF NOT EXISTS ix_fs_customer_features_as_of_date
  ON fs.customer_features_snapshot (as_of_date);

CREATE INDEX IF NOT EXISTS ix_fs_customer_features_churn
  ON fs.customer_features_snapshot (churn);
