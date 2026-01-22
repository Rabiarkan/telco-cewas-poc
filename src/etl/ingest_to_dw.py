import pandas as pd
from sqlalchemy import text
from src.utils.db import get_engine

RAW_PATH = "data/raw/telco_customer_churn.csv"

def clean_total_charges(s: pd.Series) -> pd.Series:
    # Kaggle dataset often has blanks in TotalCharges
    s = s.astype(str).str.strip().replace({"": None})
    return pd.to_numeric(s, errors="coerce")

def main():
    df = pd.read_csv(RAW_PATH)
    # Normalize column names (Kaggle: customerID)
    if "customerID" in df.columns:
        df = df.rename(columns={"customerID": "customer_id"})
    df["TotalCharges"] = clean_total_charges(df["TotalCharges"])
    df["Churn"] = (df["Churn"].astype(str).str.lower() == "yes").astype(int)

    dim_customer = df[["customer_id","gender","SeniorCitizen","Partner","Dependents"]].copy()
    dim_customer = dim_customer.rename(columns={"SeniorCitizen":"senior_citizen","Partner":"partner","Dependents":"dependents"})

    dim_contract = df[["customer_id","Contract","PaperlessBilling","PaymentMethod"]].copy()
    dim_contract = dim_contract.rename(columns={"Contract":"contract","PaperlessBilling":"paperless_billing","PaymentMethod":"payment_method"})

    dim_service = df[["customer_id","PhoneService","MultipleLines","InternetService",
                      "OnlineSecurity","OnlineBackup","DeviceProtection","TechSupport",
                      "StreamingTV","StreamingMovies"]].copy()
    dim_service = dim_service.rename(columns={
        "PhoneService":"phone_service","MultipleLines":"multiple_lines","InternetService":"internet_service",
        "OnlineSecurity":"online_security","OnlineBackup":"online_backup","DeviceProtection":"device_protection",
        "TechSupport":"tech_support","StreamingTV":"streaming_tv","StreamingMovies":"streaming_movies"
    })

    fact = df[["customer_id","tenure","MonthlyCharges","TotalCharges","Churn"]].copy()
    fact = fact.rename(columns={"MonthlyCharges":"monthly_charges","TotalCharges":"total_charges","Churn":"churn"})

    engine = get_engine()
    with engine.begin() as conn:
        # Upsert style: truncate then load for PoC simplicity
        conn.execute(text("TRUNCATE dw.fact_customer_monthly_snapshot, dw.dim_service, dw.dim_contract, dw.dim_customer CASCADE;"))
        dim_customer.to_sql("dim_customer", conn, schema="dw", if_exists="append", index=False)
        dim_contract.to_sql("dim_contract", conn, schema="dw", if_exists="append", index=False)
        dim_service.to_sql("dim_service", conn, schema="dw", if_exists="append", index=False)
        fact.to_sql("fact_customer_monthly_snapshot", conn, schema="dw", if_exists="append", index=False)

    print("Loaded rows:", len(df))

if __name__ == "__main__":
    main()


