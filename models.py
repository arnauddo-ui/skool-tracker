"""Database models for Skool Tracker."""
import os
import sqlite3
from flask import g

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "tracker.db")


def get_db():
    if "db" not in g:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA journal_mode=WAL")
    return g.db


def close_db(e=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db(app):
    with app.app_context():
        db = get_db()
        db.executescript("""
            CREATE TABLE IF NOT EXISTS members (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                first_name TEXT,
                last_name TEXT,
                email TEXT UNIQUE,
                invited_by TEXT DEFAULT '',
                joined_at TEXT NOT NULL,
                price REAL DEFAULT 0,
                recurring_interval TEXT DEFAULT '',
                tier TEXT DEFAULT '',
                ltv REAL DEFAULT 0,
                status TEXT DEFAULT 'active',
                churned_at TEXT DEFAULT '',
                first_seen_at TEXT DEFAULT '',
                last_seen_at TEXT DEFAULT '',
                upload_batch TEXT DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS clicks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL,
                clicked_at TEXT NOT NULL,
                ip_hash TEXT DEFAULT '',
                user_agent TEXT DEFAULT '',
                referer TEXT DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS custom_channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT DEFAULT 'viewer',
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_members_joined ON members(joined_at);
            CREATE INDEX IF NOT EXISTS idx_members_email ON members(email);
            CREATE INDEX IF NOT EXISTS idx_clicks_channel ON clicks(channel);
            CREATE INDEX IF NOT EXISTS idx_clicks_date ON clicks(clicked_at);

            CREATE TABLE IF NOT EXISTS tracking_links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL UNIQUE,
                platform TEXT DEFAULT '',
                destination_url TEXT NOT NULL,
                utm_source TEXT DEFAULT '',
                utm_campaign TEXT DEFAULT '',
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS upload_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch TEXT NOT NULL,
                uploaded_at TEXT NOT NULL,
                total_members INTEGER DEFAULT 0,
                active_members INTEGER DEFAULT 0,
                new_members INTEGER DEFAULT 0,
                updated_members INTEGER DEFAULT 0,
                churned_members INTEGER DEFAULT 0,
                reactivated_members INTEGER DEFAULT 0,
                paid_members INTEGER DEFAULT 0,
                free_members INTEGER DEFAULT 0,
                mrr REAL DEFAULT 0,
                total_ltv REAL DEFAULT 0,
                avg_ltv REAL DEFAULT 0
            );
        """)
        db.commit()

        # Migration: add platform column if missing (for existing databases)
        try:
            db.execute("SELECT platform FROM tracking_links LIMIT 1")
        except Exception:
            db.execute("ALTER TABLE tracking_links ADD COLUMN platform TEXT DEFAULT ''")
            # Backfill platform from channel name for existing links
            for row in db.execute("SELECT id, channel FROM tracking_links").fetchall():
                ch = row["channel"].split("-")[0] if "-" in row["channel"] else row["channel"]
                db.execute("UPDATE tracking_links SET platform = ? WHERE id = ?", (ch, row["id"]))
            db.commit()

        close_db()
    app.teardown_appcontext(close_db)
