import mysql.connector
import time
import os

POSTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS reddit_posts (
  post_id        VARCHAR(32)  NOT NULL PRIMARY KEY,
  subreddit      VARCHAR(64)  NOT NULL,
  title          TEXT         NOT NULL,
  author         VARCHAR(128) NULL,
  url            TEXT         NULL,
  permalink      TEXT         NULL,
  data_domain    VARCHAR(255) NULL,
  score          INT          NULL,
  created_utc    BIGINT       NULL,
  fetched_at_utc BIGINT       NOT NULL,
  UNIQUE KEY uniq_permalink (permalink(255)),
  INDEX idx_sub_created (subreddit, created_utc)
);
"""

COMMENTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS reddit_comments (
  comment_id     VARCHAR(32) NOT NULL PRIMARY KEY,
  post_id        VARCHAR(32) NOT NULL,
  parent_id      VARCHAR(32) NULL,
  author         VARCHAR(128) NULL,
  body           MEDIUMTEXT   NULL,
  score          INT          NULL,
  created_utc    BIGINT       NULL,
  fetched_at_utc BIGINT       NOT NULL,
  INDEX idx_post (post_id),
  INDEX idx_parent (parent_id),
  CONSTRAINT fk_comments_post FOREIGN KEY (post_id) REFERENCES reddit_posts(post_id)
    ON DELETE CASCADE
);
"""

def get_mysql_cfg() -> dict:
    return {
        "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
        "port": int(os.getenv("MYSQL_PORT", "3306")),
        "user": os.getenv("MYSQL_USER", "appuser"),
        "password": os.getenv("MYSQL_PASSWORD", "apppass"),
        "database": os.getenv("MYSQL_DB", "reddit_app"),
    }

def connect_mysql():
    cfg = get_mysql_cfg()
    return mysql.connector.connect(**cfg)

def init_schema(cnx) -> None:
    cur = cnx.cursor()
    cur.execute(POSTS_SCHEMA)
    cur.execute(COMMENTS_SCHEMA)
    cnx.commit()
    cur.close()

# inserts posts into the database, updating existing records if post_id already exists
# comments added to the post are added to the existing comment count, and score is updated to the latest value from reddit by post_id
def upsert_posts(cnx, posts: list[dict]) -> int:
    if not posts:
        return 0
    now = int(time.time())
    for p in posts:
        p["fetched_at_utc"] = now

    sql = """
    INSERT INTO reddit_posts
      (post_id, subreddit, title, author, url, permalink, data_domain, score, created_utc, fetched_at_utc)
    VALUES
      (%(post_id)s, %(subreddit)s, %(title)s, %(author)s, %(url)s, %(permalink)s,
       %(data_domain)s, %(score)s, %(created_utc)s, %(fetched_at_utc)s)
    ON DUPLICATE KEY UPDATE
      title=VALUES(title),
      author=VALUES(author),
      url=VALUES(url),
      permalink=VALUES(permalink),
      data_domain=VALUES(data_domain),
      score=VALUES(score),
      created_utc=VALUES(created_utc),
      fetched_at_utc=VALUES(fetched_at_utc);
    """
    cur = cnx.cursor()
    cur.executemany(sql, posts)
    cnx.commit()
    n = cur.rowcount
    cur.close()
    return n

def upsert_comments(cnx, comments: list[dict]) -> int:
    if not comments:
        return 0
    now = int(time.time())
    for c in comments:
        c["fetched_at_utc"] = now

    sql = """
    INSERT INTO reddit_comments
      (comment_id, post_id, parent_id, author, body, score, created_utc, fetched_at_utc)
    VALUES
      (%(comment_id)s, %(post_id)s, %(parent_id)s, %(author)s, %(body)s,
       %(score)s, %(created_utc)s, %(fetched_at_utc)s)
    ON DUPLICATE KEY UPDATE
      author=VALUES(author),
      body=VALUES(body),
      score=VALUES(score),
      created_utc=VALUES(created_utc),
      fetched_at_utc=VALUES(fetched_at_utc);
    """
    cur = cnx.cursor()
    cur.executemany(sql, comments)
    cnx.commit()
    n = cur.rowcount
    cur.close()
    return n