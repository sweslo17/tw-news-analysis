"""CLI entry point for pipeline commands."""

from app.database import create_db_and_tables
from .pipeline import app

create_db_and_tables()

if __name__ == "__main__":
    app()
