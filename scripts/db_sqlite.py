import sqlite3
import json
from datetime import datetime, timezone

def init_sqlite(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # pragmas that help concurrency + speed for this workflow
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA foreign_keys=ON;")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS reddit_posts (
      post_id       TEXT PRIMARY KEY,
      subreddit     TEXT NOT NULL,
      title         TEXT NOT NULL,
      selftext      TEXT,
      clean_text    TEXT,
      url           TEXT,
      permalink     TEXT,
      is_self       INTEGER NOT NULL,
      score         INTEGER NOT NULL,
      num_comments  INTEGER NOT NULL,
      author_mask   TEXT,
      created_utc   INTEGER NOT NULL,
      created_at    TEXT NOT NULL,
      keywords_json TEXT,
      topic         TEXT
    );
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_sub_time ON reddit_posts(subreddit, created_utc);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_time ON reddit_posts(created_utc);")

    conn.commit()
    conn.close()


def upsert_posts_sqlite(db_path: str, rows: list[dict]) -> int:
    """
    rows: list of DB-ready dicts with keys matching columns.
    """
    if not rows:
        return 0

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")

    sql = """
    INSERT INTO reddit_posts
    (post_id, subreddit, title, selftext, clean_text, url, permalink, is_self,
     score, num_comments, author_mask, created_utc, created_at, keywords_json, topic)
    VALUES
    (:post_id, :subreddit, :title, :selftext, :clean_text, :url, :permalink, :is_self,
     :score, :num_comments, :author_mask, :created_utc, :created_at, :keywords_json, :topic)
    ON CONFLICT(post_id) DO UPDATE SET
      score=excluded.score,
      num_comments=excluded.num_comments,
      clean_text=excluded.clean_text,
      keywords_json=excluded.keywords_json,
      topic=excluded.topic;
    """

    cur.executemany(sql, rows)
    conn.commit()
    affected = cur.rowcount
    conn.close()
    return affected


def quick_stats(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM reddit_posts;")
    print("Total posts:", cur.fetchone()[0])

    cur.execute("""
        SELECT subreddit, COUNT(*) as c
        FROM reddit_posts
        GROUP BY subreddit
        ORDER BY c DESC
        LIMIT 10;
    """)
    print("Top subreddits:", cur.fetchall())

    conn.close()
