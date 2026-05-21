"""Create warehouse schemas and seed reference tables."""

import os
import sqlalchemy as sa
from dotenv import load_dotenv

load_dotenv()

SCHEMAS = ["raw", "staging", "marts"]


def _build_url() -> sa.engine.URL:
    # Constructed inside a function so that missing env vars raise at call time,
    # not at module import time. Previously this was module-level, which caused
    # KeyError whenever the module was imported without a fully configured .env.
    missing = [v for v in ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD")
               if not os.getenv(v)]
    if missing:
        raise EnvironmentError(f"Missing env vars: {', '.join(missing)}")
    return sa.engine.URL.create(
        drivername="postgresql+psycopg2",
        host=os.environ["DB_HOST"],
        port=int(os.environ["DB_PORT"]),
        database=os.environ["DB_NAME"],
        username=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )


def main() -> None:
    engine = sa.create_engine(_build_url())
    with engine.begin() as conn:
        for schema in SCHEMAS:
            conn.execute(sa.text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            print(f"  schema '{schema}' ready")


if __name__ == "__main__":
    main()
