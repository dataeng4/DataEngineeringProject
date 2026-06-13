# ============================================================
# database.py
# Project : Configuration-Driven SQLite Data Generation
# Version : 1.0.0
#
# Responsibilities:
#   - Parse config.yaml to define the user_profiles table
#   - Define the pipeline_logs table for workflow orchestration
#   - Provide helper functions for duplicate checking, batch
#     insertion, and pipeline status logging
# ============================================================

import yaml
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    String,
    Text,
    create_engine,
    inspect,
    text,
)
from sqlalchemy.orm import declarative_base, Session, sessionmaker

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert


# ─────────────────────────────────────────────
# 1. Engine & Session Factory
# ─────────────────────────────────────────────
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv()

def get_engine():
    """Universal Engine Factory driven by the .env file."""
    db_type = os.getenv("DB_TYPE", "sqlite").lower()
    
    if db_type == "postgres":
        db_pass = os.getenv("DB_PASSWORD")
        # Read the host from the environment, default to localhost for local testing
        db_host = os.getenv("DB_HOST", "localhost") 
        return create_engine(f'postgresql+psycopg2://postgres:{db_pass}@{db_host}:5432/project_data', echo=False)
        
    elif db_type == "aws":
        db_pass = os.getenv("DB_PASSWORD")
        # Add fallback to 'postgres_db' so Tasks 1 & 3 don't crash in Airflow
        aws_host = os.getenv("AWS_HOST", "postgres_db") 
        return create_engine(f'postgresql+psycopg2://postgres:{db_pass}@{aws_host}:5432/project_data', echo=False)
        
    else:
        # Fallback to local SQLite (requires check_same_thread)
        return create_engine(
            'sqlite:///project_data.db', 
            connect_args={"check_same_thread": False}, 
            echo=False
        )

# Initialize the global engine and session using the factory
engine = get_engine()
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)

# ─────────────────────────────────────────────
# 2. Declarative Base
# ─────────────────────────────────────────────

Base = declarative_base()


# ─────────────────────────────────────────────
# 3. YAML → SQLAlchemy type mapping
# ─────────────────────────────────────────────

def _resolve_sa_type(sql_type: str):
    """
    Map a YAML sql_type string to a SQLAlchemy column type.

    Supported inputs (case-insensitive):
      INTEGER, VARCHAR(n), TEXT, BOOLEAN, DATETIME
    Falls back to Text for any unrecognised type so the schema
    never fails silently with a hard crash.
    """
    sql_type_upper = sql_type.upper().strip()

    if sql_type_upper == "INTEGER":
        return Integer
    if sql_type_upper.startswith("VARCHAR"):
        # Extract the length from VARCHAR(n); default to 255 if absent
        try:
            length = int(sql_type_upper.split("(")[1].rstrip(")"))
        except (IndexError, ValueError):
            length = 255
        return String(length)
    if sql_type_upper == "TEXT":
        return Text
    if sql_type_upper == "BOOLEAN":
        return Boolean
    if sql_type_upper == "DATETIME":
        return DateTime

    # Graceful fallback — log a warning so engineers notice the gap
    import warnings
    warnings.warn(
        f"Unrecognised sql_type '{sql_type}' — falling back to Text.",
        stacklevel=2,
    )
    return Text


# ─────────────────────────────────────────────
# 4. Dynamic table class built from YAML config
# ─────────────────────────────────────────────

def _build_user_profiles_class(config_path: str = "config.yaml") -> type:
    """
    Parse the YAML configuration file and construct a SQLAlchemy
    ORM class for the first table whose name is 'user_profiles'.

    The class is attached to the shared ``Base`` metadata so that
    ``Base.metadata.create_all(engine)`` picks it up automatically.

    Args:
        config_path: Path to the master YAML configuration file.

    Returns:
        A SQLAlchemy ORM class (mapped to the 'user_profiles' table).

    Raises:
        FileNotFoundError: If config_path does not exist.
        ValueError: If no 'user_profiles' table is found in the config.
    """
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_file.open("r") as fh:
        config = yaml.safe_load(fh)

    # Locate the user_profiles table definition
    table_def = next(
        (t for t in config.get("tables", []) if t["name"] == "user_profiles"),
        None,
    )
    if table_def is None:
        raise ValueError("No 'user_profiles' table found in the YAML configuration.")

    # Build the column dictionary for type()
    attrs: dict = {"__tablename__": "user_profiles"}

    for field in table_def.get("fields", []):
        fname = field["name"]
        is_pk = field.get("primary_key", False)
        is_unique = field.get("unique", False)
        is_nullable = field.get("nullable", True)
        autoincrement = field.get("autoincrement", False)

        sa_type = _resolve_sa_type(field["sql_type"])

        col = Column(
            sa_type,
            primary_key=is_pk,
            autoincrement=autoincrement if is_pk else False,
            unique=is_unique,
            nullable=is_nullable,
        )
        attrs[fname] = col

    # Append the audit timestamp — not in the YAML, always present
    attrs["created_at"] = Column(
        DateTime,
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    # Dynamically create and register the ORM class with Base
    UserProfilesModel = type("UserProfiles", (Base,), attrs)
    return UserProfilesModel


# Build the class at import time so other modules can import it directly
UserProfiles = _build_user_profiles_class()


# ─────────────────────────────────────────────
# 5. Pipeline Logs table (static definition)
# ─────────────────────────────────────────────

class PipelineLog(Base):
    """
    Workflow orchestration log.

    One row per pipeline batch execution. The primary key is a
    caller-supplied batch_id (e.g. a UUID or "batch_001") so that
    upsert semantics are straightforward.
    """

    __tablename__ = "pipeline_logs"

    batch_id = Column(String(64), primary_key=True, nullable=False)
    records_processed = Column(Integer, nullable=True)
    status = Column(
        String(16),
        nullable=False,
        default="RUNNING",
        # Valid values: 'RUNNING' | 'SUCCESS' | 'FAILED'
    )
    error_message = Column(Text, nullable=True)
    timestamp = Column(
        DateTime,
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )


# ─────────────────────────────────────────────
# 6. Table initialisation
# ─────────────────────────────────────────────

def init_db() -> None:
    """
    Create all tables that do not yet exist in the database.

    Safe to call on every application start — SQLAlchemy uses
    CREATE TABLE IF NOT EXISTS semantics under the hood.
    """
    Base.metadata.create_all(bind=engine)


# ─────────────────────────────────────────────
# 7. Helper functions
# ─────────────────────────────────────────────

def check_duplicate(email: str, mobile: str) -> bool:
    """
    Return True if *either* the email or mobile number already
    exists in the user_profiles table.

    This is intentionally strict: a record is considered a duplicate
    if it would violate *any* unique constraint, not only an exact
    full-record match.

    Args:
        email:  The email address to look up.
        mobile: The mobile number to look up.

    Returns:
        True  → at least one matching record exists (skip insert).
        False → neither value is present (safe to insert).
    """
    with SessionLocal() as session:
        exists = (
            session.query(UserProfiles)
            .filter(
                (UserProfiles.email == email) | (UserProfiles.mobile == mobile)
            )
            .first()
        )
    return exists is not None


def insert_profiles_batch(data_list: list[dict]) -> int:
    """Bulk-insert profile dictionaries or upload to AWS S3 Data Lake."""
    if not data_list:
        return 0

    now = datetime.now(timezone.utc)
    stamped = [{**row, "created_at": now} for row in data_list]
    
    db_type = os.getenv("DB_TYPE", "sqlite").lower()

    # ── THE CLOUD DATA LAKEHOUSE ROUTE (AWS S3) ──────────────────────
    if db_type == "aws":
        import pandas as pd
        import boto3
        import io
        import uuid
        import pyarrow as pa
        import pyarrow.parquet as pq

        # 1. Convert the batch dictionary to a Pandas DataFrame
        df = pd.DataFrame(stamped)
        
        # 2. Convert to PyArrow Table
        table = pa.Table.from_pandas(df)
        
        # 3. The Data Contract Fix: Safely cast ALL timestamps to microseconds
        #    This permanently resolves the Databricks TIMESTAMP(NANOS) crash
        new_schema = pa.schema([
            pa.field(f.name, pa.timestamp('us', tz=f.type.tz)) if pa.types.is_timestamp(f.type) else f
            for f in table.schema
        ])
        table = table.cast(new_schema)
        
        # 4. Compress the safely-cast table into Parquet
        parquet_buffer = io.BytesIO()
        pq.write_table(table, parquet_buffer)
        parquet_buffer.seek(0)
        
        # 5. Generate partition path and upload to S3
        batch_id = str(uuid.uuid4())[:8]
        timestamp_str = now.strftime('%Y%m%d_%H%M%S')
        file_key = f"bronze/user_profiles/batch_{timestamp_str}_{batch_id}.parquet"
        
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_DEFAULT_REGION")
        )
        s3_client.upload_fileobj(parquet_buffer, os.getenv("AWS_BUCKET_NAME"), file_key)
        
        return len(stamped)
    # ── THE LOCAL RELATIONAL ROUTE (PostgreSQL / SQLite) ──────────────
    else:
        with engine.begin() as conn:
            if db_type == "postgres":
                stmt = pg_insert(UserProfiles).values(stamped)
                upsert_stmt = stmt.on_conflict_do_nothing()
                conn.execute(upsert_stmt)
            else:
                stmt = sqlite_insert(UserProfiles).values(stamped)
                upsert_stmt = stmt.on_conflict_do_nothing()
                conn.execute(upsert_stmt)

        return len(stamped)


def log_pipeline_status(
    batch_id: str,
    records_processed: int | None,
    status: str,
    error_message: str | None = None,
) -> None:
    """
    Upsert a pipeline execution record into pipeline_logs.

    If a row with the given batch_id already exists it is updated
    in place; otherwise a new row is inserted. This makes the
    function safe to call multiple times for the same batch
    (e.g. once at RUNNING, once at SUCCESS / FAILED).

    Args:
        batch_id:           Unique identifier for the batch run.
        records_processed:  Count of records handled in this batch.
                            Pass None when status is 'RUNNING'.
        status:             One of 'RUNNING', 'SUCCESS', or 'FAILED'.
        error_message:      Optional error detail; populated on FAILED.

    Example::

        log_pipeline_status("batch_001", None, "RUNNING")
        # … do work …
        log_pipeline_status("batch_001", 5000, "SUCCESS")
    """
    valid_statuses = {"RUNNING", "SUCCESS", "FAILED"}
    if status not in valid_statuses:
        raise ValueError(f"Invalid status '{status}'. Must be one of {valid_statuses}.")

    now = datetime.now(timezone.utc)

    with SessionLocal() as session:
        existing: PipelineLog | None = session.get(PipelineLog, batch_id)

        if existing:
            # Update the existing log row
            existing.records_processed = records_processed
            existing.status = status
            existing.error_message = error_message
            existing.timestamp = now
        else:
            # Insert a fresh log row
            log_entry = PipelineLog(
                batch_id=batch_id,
                records_processed=records_processed,
                status=status,
                error_message=error_message,
                timestamp=now,
            )
            session.add(log_entry)

        session.commit()


# ─────────────────────────────────────────────
# 8. Entry point — create tables on direct run
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("Initialising database …")
    init_db()

    # Confirm which tables were created
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    print(f"Tables available: {tables}")
    print("Done.")


