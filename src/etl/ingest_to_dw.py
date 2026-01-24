import pandas as pd
from sqlalchemy import text
from src.utils.db import get_engine

RAW_PATH = "data/raw/telco_customer_churn.csv"


def clean_total_charges(s: pd.Series) -> pd.Series:
    # Kaggle dataset often has blanks in TotalCharges
    s = s.astype(str).str.strip().replace({"": None, "nan": None})
    return pd.to_numeric(s, errors="coerce")


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # customerID -> customer_id (keep others as Kaggle names; we map explicitly)
    if "customerID" in df.columns:
        df = df.rename(columns={"customerID": "customer_id"})
    return df


def main():
    df = pd.read_csv(RAW_PATH)
    df = normalize_columns(df)

    # Clean / Cast
    df["TotalCharges"] = clean_total_charges(df["TotalCharges"])
    df["Churn"] = (df["Churn"].astype(str).str.strip().str.lower() == "yes").astype(int)

    dim_customer = (
        df[["customer_id", "gender", "SeniorCitizen", "Partner", "Dependents"]]
        .copy()
        .rename(columns={
            "SeniorCitizen": "senior_citizen",
            "Partner": "partner",
            "Dependents": "dependents",
        })
        .drop_duplicates(subset=["customer_id"])
    )

    dim_contract = (
        df[["Contract", "PaperlessBilling"]]
        .copy()
        .rename(columns={
            "Contract": "contract",
            "PaperlessBilling": "paperless_billing",
        })
        .drop_duplicates()
    )

    dim_payment_method = (
        df[["PaymentMethod"]]
        .copy()
        .rename(columns={"PaymentMethod": "payment_method"})
        .drop_duplicates()
    )

    dim_internet_service = (
        df[["InternetService"]]
        .copy()
        .rename(columns={"InternetService": "internet_service"})
        .drop_duplicates()
    )

    dim_service_bundle = (
        df[[
            "PhoneService", "MultipleLines",
            "OnlineSecurity", "OnlineBackup", "DeviceProtection", "TechSupport",
            "StreamingTV", "StreamingMovies"
        ]]
        .copy()
        .rename(columns={
            "PhoneService": "phone_service",
            "MultipleLines": "multiple_lines",
            "OnlineSecurity": "online_security",
            "OnlineBackup": "online_backup",
            "DeviceProtection": "device_protection",
            "TechSupport": "tech_support",
            "StreamingTV": "streaming_tv",
            "StreamingMovies": "streaming_movies",
        })
        .drop_duplicates()
    )

    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS dw;"))

        # truncate & reload everything for poc
        conn.execute(text("""
            TRUNCATE
              dw.fact_customer_churn,
              dw.dim_service_bundle,
              dw.dim_internet_service,
              dw.dim_payment_method,
              dw.dim_contract,
              dw.dim_customer
            RESTART IDENTITY CASCADE;
        """))

        # Load dims (serial SKs will be generated)
        dim_customer.to_sql("dim_customer", conn, schema="dw", if_exists="append", index=False)
        dim_contract.to_sql("dim_contract", conn, schema="dw", if_exists="append", index=False)
        dim_payment_method.to_sql("dim_payment_method", conn, schema="dw", if_exists="append", index=False)
        dim_internet_service.to_sql("dim_internet_service", conn, schema="dw", if_exists="append", index=False)
        dim_service_bundle.to_sql("dim_service_bundle", conn, schema="dw", if_exists="append", index=False)

        contract_map = pd.read_sql(
            "SELECT contract_sk, contract, paperless_billing FROM dw.dim_contract",
            conn
        )
        payment_map = pd.read_sql(
            "SELECT payment_method_sk, payment_method FROM dw.dim_payment_method",
            conn
        )
        internet_map = pd.read_sql(
            "SELECT internet_service_sk, internet_service FROM dw.dim_internet_service",
            conn
        )
        bundle_map = pd.read_sql(
            """
            SELECT
              service_bundle_sk,
              phone_service, multiple_lines,
              online_security, online_backup, device_protection, tech_support,
              streaming_tv, streaming_movies
            FROM dw.dim_service_bundle
            """,
            conn
        )

        work = df.copy()

        # Rename to align with dim keys
        work = work.rename(columns={
            "Contract": "contract",
            "PaperlessBilling": "paperless_billing",
            "PaymentMethod": "payment_method",
            "InternetService": "internet_service",
            "PhoneService": "phone_service",
            "MultipleLines": "multiple_lines",
            "OnlineSecurity": "online_security",
            "OnlineBackup": "online_backup",
            "DeviceProtection": "device_protection",
            "TechSupport": "tech_support",
            "StreamingTV": "streaming_tv",
            "StreamingMovies": "streaming_movies",
            "MonthlyCharges": "monthly_charges",
            "TotalCharges": "total_charges",
            "Churn": "churn",
        })

        work = work.merge(contract_map, on=["contract", "paperless_billing"], how="left")
        work = work.merge(payment_map, on=["payment_method"], how="left")
        work = work.merge(internet_map, on=["internet_service"], how="left")
        work = work.merge(
            bundle_map,
            on=[
                "phone_service", "multiple_lines",
                "online_security", "online_backup", "device_protection", "tech_support",
                "streaming_tv", "streaming_movies"
            ],
            how="left"
        )

        missing = work[
            work["contract_sk"].isna()
            | work["payment_method_sk"].isna()
            | work["internet_service_sk"].isna()
            | work["service_bundle_sk"].isna()
        ]
        if len(missing) > 0:
            raise ValueError(
                f"SK mapping failed for {len(missing)} rows. Check dim load and join keys."
            )

        fact = work[[
            "customer_id",
            "contract_sk",
            "payment_method_sk",
            "internet_service_sk",
            "service_bundle_sk",
            "tenure",
            "monthly_charges",
            "total_charges",
            "churn",
        ]].copy()

        fact["contract_sk"] = fact["contract_sk"].astype("int64")
        fact["payment_method_sk"] = fact["payment_method_sk"].astype("int64")
        fact["internet_service_sk"] = fact["internet_service_sk"].astype("int64")
        fact["service_bundle_sk"] = fact["service_bundle_sk"].astype("int64")

        fact.to_sql("fact_customer_churn", conn, schema="dw", if_exists="append", index=False)

    print("Loaded rows:", len(df))


if __name__ == "__main__":
    main()
