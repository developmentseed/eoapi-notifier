#!/usr/bin/env python3
"""
Consolidated pgSTAC setup utility for integration testing.

Single source for all database initialization, notification triggers, and test data.
"""

import json
import os
import sys

import psycopg
from pypgstac.db import PgstacDB
from pypgstac.migrate import Migrate

try:
    from .constants import SQL, get_postgres_config
    from .test_data import get_test_collections
except ImportError:
    from constants import SQL, get_postgres_config
    from test_data import get_test_collections


class PgstacSetup:
    """Handles pgSTAC setup for integration testing."""

    def __init__(self, platform: str = "docker-compose"):
        self.platform = platform
        self.config = get_postgres_config(platform)

    def get_connection(self) -> psycopg.Connection:
        """Get database connection."""
        print(
            f"Connecting to PostgreSQL at {self.config['host']}:"
            f"{self.config['port']}/{self.config['dbname']}"
        )
        return psycopg.connect(**self.config)

    def setup_notification_triggers(self, conn: psycopg.Connection) -> None:
        """Set up notification triggers."""
        print("Setting up notification triggers...")

        with conn.cursor() as cur:
            # Create notification function
            cur.execute(SQL.NOTIFICATION_FUNCTION)

            # Create triggers
            for _trigger_name, trigger_sql in SQL.NOTIFICATION_TRIGGERS.items():
                cur.execute(trigger_sql)

        print("✅ Notification triggers created")

    def setup_test_collections(self, conn: psycopg.Connection) -> None:
        """Set up test collections."""
        print("Setting up test collections...")

        collections = get_test_collections()

        with conn.cursor() as cur:
            for collection in collections:
                cur.execute(
                    "INSERT INTO pgstac.collections (content) VALUES (%s) "
                    "ON CONFLICT (id) DO NOTHING",
                    [json.dumps(collection)],
                )
                print(f"  ✓ Collection '{collection['id']}' added")

        conn.commit()
        print(f"✅ {len(collections)} collections set up")

    def setup_test_items(self, conn: psycopg.Connection) -> None:
        """Set up test items using pypgstac loader."""
        print("Setting up test items...")
        print(
            "  ⚠️  Skipping item loading (partitioning system needs collections first)"
        )
        print("✅ Item loading skipped for basic test setup")

    def run_migrations(self) -> None:
        """Run pypgstac migrations to create schema."""
        print("Running pypgstac migrations...")

        dsn = f"postgresql://{self.config['user']}:{self.config['password']}@{self.config['host']}:{self.config['port']}/{self.config['dbname']}"

        try:
            db = PgstacDB(dsn=dsn)
            migrate = Migrate(db)
            migrate.run_migration()
            print("✅ pgSTAC migrations completed")
        except Exception as e:
            print(f"❌ Migration failed: {e}")
            raise
        finally:
            try:
                db.close()
            except Exception:
                pass

    def verify_setup(self, conn: psycopg.Connection) -> None:
        """Verify the database setup."""
        print("Verifying database setup...")

        with conn.cursor() as cur:
            # Check collections
            cur.execute("SELECT count(*) FROM pgstac.collections")
            collection_count = cur.fetchone()[0]
            print(f"  ✓ Collections: {collection_count}")

            # Check items
            cur.execute("SELECT count(*) FROM pgstac.items")
            item_count = cur.fetchone()[0]
            print(f"  ✓ Items: {item_count}")

            # Check triggers
            cur.execute("""
                SELECT trigger_name FROM information_schema.triggers
                WHERE event_object_schema = 'pgstac'
                  AND event_object_table = 'items'
                  AND trigger_name LIKE 'notify_items_change%'
                ORDER BY trigger_name
            """)
            triggers = [row[0] for row in cur.fetchall()]
            print(f"  ✓ Triggers: {', '.join(triggers)}")

        print("✅ Database verification completed")

    def run_full_setup(self) -> bool:
        """Run complete pgSTAC setup."""
        conn = None
        try:
            print("🚀 Starting pgSTAC integration setup...")

            conn = self.get_connection()
            print("✅ Database connection established")

            self.run_migrations()

            self.setup_notification_triggers(conn)
            conn.commit()

            self.setup_test_collections(conn)
            self.setup_test_items(conn)

            conn.commit()
            print("✅ All setup operations completed")

            self.verify_setup(conn)
            print("🎉 pgSTAC setup completed successfully!")
            return True

        except Exception as e:
            print(f"❌ Setup failed: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()


def main():
    """Main entry point for standalone execution."""
    platform = os.getenv("INTEGRATION_PLATFORM", "docker-compose")
    setup = PgstacSetup(platform)

    if not setup.run_full_setup():
        sys.exit(1)


if __name__ == "__main__":
    main()
