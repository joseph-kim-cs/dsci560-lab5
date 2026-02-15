import mysql.connector
import time

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
      data_domain=VALUES(data_domain),
      score=VALUES(score),
      created_utc=VALUES(created_utc),
      fetched_at_utc=VALUES(fetched_at_utc);
    """

    cur = cnx.cursor()
    cur.executemany(sql, posts)
    cnx.commit()
    return cur.rowcount


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
    return cur.rowcount
