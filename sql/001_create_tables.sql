CREATE SCHEMA IF NOT EXISTS dw;

DROP TABLE IF EXISTS dw.fact_customer_monthly_snapshot;
DROP TABLE IF EXISTS dw.dim_service;
DROP TABLE IF EXISTS dw.dim_contract;
DROP TABLE IF EXISTS dw.dim_customer;

CREATE TABLE dw.dim_customer (
  customer_id TEXT PRIMARY KEY,
  gender TEXT,
  senior_citizen INT,
  partner TEXT,
  dependents TEXT
);

CREATE TABLE dw.dim_contract (
  customer_id TEXT PRIMARY KEY REFERENCES dw.dim_customer(customer_id),
  contract TEXT,
  paperless_billing TEXT,
  payment_method TEXT
);

CREATE TABLE dw.dim_service (
  customer_id TEXT PRIMARY KEY REFERENCES dw.dim_customer(customer_id),
  phone_service TEXT,
  multiple_lines TEXT,
  internet_service TEXT,
  online_security TEXT,
  online_backup TEXT,
  device_protection TEXT,
  tech_support TEXT,
  streaming_tv TEXT,
  streaming_movies TEXT
);

CREATE TABLE dw.fact_customer_monthly_snapshot (
  customer_id TEXT PRIMARY KEY REFERENCES dw.dim_customer(customer_id),
  tenure INT,
  monthly_charges DOUBLE PRECISION,
  total_charges DOUBLE PRECISION,
  churn INT
);


