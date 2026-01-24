CREATE SCHEMA IF NOT EXISTS dw;

DROP TABLE IF EXISTS dw.fact_customer_churn;
DROP TABLE IF EXISTS dw.dim_service_bundle;
DROP TABLE IF EXISTS dw.dim_internet_service;
DROP TABLE IF EXISTS dw.dim_payment_method;
DROP TABLE IF EXISTS dw.dim_contract;
DROP TABLE IF EXISTS dw.dim_customer;


CREATE TABLE dw.dim_customer (
  customer_id     TEXT PRIMARY KEY,   -- customerID
  gender          TEXT,
  senior_citizen  INT,
  partner         TEXT,
  dependents      TEXT
);

CREATE TABLE dw.dim_contract (
  contract_sk       BIGSERIAL PRIMARY KEY,
  contract          TEXT NOT NULL,
  paperless_billing TEXT NOT NULL,
  UNIQUE (contract, paperless_billing)
);

CREATE TABLE dw.dim_payment_method (
  payment_method_sk BIGSERIAL PRIMARY KEY,
  payment_method    TEXT NOT NULL,
  UNIQUE (payment_method)
);

CREATE TABLE dw.dim_internet_service (
  internet_service_sk BIGSERIAL PRIMARY KEY,
  internet_service    TEXT NOT NULL,
  UNIQUE (internet_service)
);

CREATE TABLE dw.dim_service_bundle (
  service_bundle_sk  BIGSERIAL PRIMARY KEY,

  phone_service      TEXT NOT NULL,
  multiple_lines     TEXT NOT NULL,
  online_security    TEXT NOT NULL,
  online_backup      TEXT NOT NULL,
  device_protection  TEXT NOT NULL,
  tech_support       TEXT NOT NULL,
  streaming_tv       TEXT NOT NULL,
  streaming_movies   TEXT NOT NULL,

  UNIQUE (
    phone_service, multiple_lines,
    online_security, online_backup, device_protection, tech_support,
    streaming_tv, streaming_movies
  )
);

-- Fact (1 customer = 1 line) 
CREATE TABLE dw.fact_customer_churn (
  customer_id         TEXT PRIMARY KEY REFERENCES dw.dim_customer(customer_id),

  contract_sk         BIGINT NOT NULL REFERENCES dw.dim_contract(contract_sk),
  payment_method_sk   BIGINT NOT NULL REFERENCES dw.dim_payment_method(payment_method_sk),
  internet_service_sk BIGINT NOT NULL REFERENCES dw.dim_internet_service(internet_service_sk),
  service_bundle_sk   BIGINT NOT NULL REFERENCES dw.dim_service_bundle(service_bundle_sk),

  tenure              INT,
  monthly_charges     DOUBLE PRECISION,
  total_charges       DOUBLE PRECISION,
  churn               INT               -- Yes/No -> 1/0
);

CREATE INDEX IF NOT EXISTS ix_dim_contract_nk
  ON dw.dim_contract (contract, paperless_billing);

CREATE INDEX IF NOT EXISTS ix_dim_payment_method_nk
  ON dw.dim_payment_method (payment_method);

CREATE INDEX IF NOT EXISTS ix_dim_internet_service_nk
  ON dw.dim_internet_service (internet_service);

CREATE INDEX IF NOT EXISTS ix_dim_service_bundle_nk
  ON dw.dim_service_bundle (
    phone_service, multiple_lines,
    online_security, online_backup, device_protection, tech_support,
    streaming_tv, streaming_movies
  );

CREATE INDEX IF NOT EXISTS ix_fact_churn
  ON dw.fact_customer_churn (churn);
