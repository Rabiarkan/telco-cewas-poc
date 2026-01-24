import os
import pandas as pd
from sqlalchemy import text
from src.utils.db import get_engine


def main():
    # as_of_date param: default = today (container time)
    # örn: AS_OF_DATE=2026-01-25
    as_of_date = os.getenv("AS_OF_DATE")  # YYYY-MM-DD or None

    engine = get_engine()

    # 1) Feature view SQL (DW -> wide)
    feature_sql = """
    SELECT
      f.customer_id,
      f.churn,
      f.tenure,
      f.monthly_charges,
      f.total_charges,

      c.gender,
      c.senior_citizen,
      c.partner,
      c.dependents,

      dc.contract,
      dc.paperless_billing,
      pm.payment_method,
      ins.internet_service,

      sb.phone_service,
      sb.multiple_lines,
      sb.online_security,
      sb.online_backup,
      sb.device_protection,
      sb.tech_support,
      sb.streaming_tv,
      sb.streaming_movies

    FROM dw.fact_customer_churn f
    JOIN dw.dim_customer c
      ON c.customer_id = f.customer_id
    JOIN dw.dim_contract dc
      ON dc.contract_sk = f.contract_sk
    JOIN dw.dim_payment_method pm
      ON pm.payment_method_sk = f.payment_method_sk
    JOIN dw.dim_internet_service ins
      ON ins.internet_service_sk = f.internet_service_sk
    JOIN dw.dim_service_bundle sb
      ON sb.service_bundle_sk = f.service_bundle_sk
    """

    with engine.begin() as conn:
        # sql/002_create_feature_store.sql
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS fs;"))

        df = pd.read_sql(feature_sql, conn)

        if not as_of_date:
            as_of_date = conn.execute(text("SELECT CURRENT_DATE")).scalar()

        df.insert(0, "as_of_date", pd.to_datetime(as_of_date).date())

        conn.execute(
            text("DELETE FROM fs.customer_features_snapshot WHERE as_of_date = :d"),
            {"d": pd.to_datetime(as_of_date).date()},
        )

        df.to_sql(
            "customer_features_snapshot",
            conn,
            schema="fs",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=2000,
        )

    print(f"Feature store rows written: {len(df)} | as_of_date={as_of_date}")


if __name__ == "__main__":
    main()
