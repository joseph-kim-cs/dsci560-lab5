# main.py
import argparse
import time
from dotenv import load_dotenv

from scripts.bs4_scraper import scrape_posts_bs4
from scripts.comment_scraper import fetch_comments_flat
from scripts.database_pipeline import connect_mysql, init_schema, upsert_posts, upsert_comments


def to_post_row(p: dict) -> dict:
    return {
        "post_id": p.get("post_id") or "", # ideally t3_xxxxx from html data-fullname
        "subreddit": p.get("subreddit") or "",
        "title": p.get("title") or "",
        "author": p.get("author") or None,
        "url": p.get("url") or None,
        "permalink": p.get("permalink") or None,
        "data_domain": p.get("data_domain") or None,
        "score": p.get("score"),
        "created_utc": p.get("created_utc"),
    }


def main():
    load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument("--subreddit", default="tech")
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--max-pages", type=int, default=5)
    parser.add_argument("--poll-seconds", type=int, default=0, help="0 means run once else loop")
    parser.add_argument("--comments-sleep", type=float, default=1.5)
    parser.add_argument("--max-posts-for-comments", type=int, default=50)
    args = parser.parse_args()

    cnx = connect_mysql()
    init_schema(cnx)

    seen_comment_posts = set()

    def one_cycle():
        posts = scrape_posts_bs4(args.subreddit, limit=args.limit, max_pages=args.max_pages)
        post_rows = [to_post_row(p) for p in posts if p.get("permalink")]

        # important if post_id missing for some reason
        # you can still use permalink as uniq key but post_id is primary key here
        # so we skip any missing post_id
        post_rows = [r for r in post_rows if r["post_id"]]

        print(f"scraped {len(posts)} posts and prepared {len(post_rows)} rows")
        upsert_posts(cnx, post_rows)

        # fetch comments for only the newest subset per cycle
        for r in post_rows[: args.max_posts_for_comments]:
            if r["post_id"] in seen_comment_posts:
                continue
            try:
                comments = fetch_comments_flat(r["permalink"], sleep_s=args.comments_sleep)
                if comments:
                    upsert_comments(cnx, comments)
                seen_comment_posts.add(r["post_id"])
                print(f"saved comments post {r['post_id']} count {len(comments)}")
            except Exception as e:
                print(f"comment fetch failed post {r['post_id']} err {e}")

        print("cycle done")

    if args.poll_seconds <= 0:
        one_cycle()
    else:
        while True:
            one_cycle()
            time.sleep(args.poll_seconds)


if __name__ == "__main__":
    main()
