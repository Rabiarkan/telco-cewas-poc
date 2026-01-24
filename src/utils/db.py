import os
from sqlalchemy import create_engine, text

def get_engine():
    user = os.getenv("POSTGRES_USER", "cewas")
    pwd  = os.getenv("POSTGRES_PASSWORD", "cewas")
    db   = os.getenv("POSTGRES_DB", "cewas_dw")

    host = os.getenv("PGHOST") or os.getenv("POSTGRES_HOST") or "postgres"
    port = os.getenv("PGPORT") or os.getenv("POSTGRES_PORT") or "5432"

    uri = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(uri, future=True, pool_pre_ping=True)

def run_sql_file(engine, path: str):
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()

    statements = [s.strip() for s in sql.split(";") if s.strip()]

    with engine.begin() as conn:
        for stmt in statements:
            conn.exec_driver_sql(stmt)
