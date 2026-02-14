import os
import time
import argparse
from datetime import datetime, timezone

import praw
from praw.models import MoreComments
from dotenv import load_dotenv


load_dotenv()

# client credentials are loaded from environment variables for security and flexibility
client_id = os.getenv("REDDIT_CLIENT_ID")
client_secret = os.getenv("REDDIT_CLIENT_SECRET")
user_agent = os.getenv("REDDIT_USER_AGENT")


def fetch_posts(
    reddit: praw.Reddit, subreddit_name: str, total_limit: int, sort: str = "new",
    batch_size: int = 500, polite_sleep: float = 0.5, max_empty_pages: int = 3,
) -> list[dict]:
    if total_limit <= 0:
        return []

    sr = reddit.subreddit(subreddit_name)

    collected: list[dict] = []
    seen_ids: set[str] = set()

    # cursor: fetch posts created before this timestamp
    before = None
    empty_pages = 0

    while len(collected) < total_limit:
        remaining = total_limit - len(collected)
        limit = min(batch_size, remaining)

        if sort.lower() == "new":
            params = {"limit": limit}
            if before is not None:
                params["before"] = before  # fullname of thing in some endpoints; not always supported
            listing = sr.new(limit=limit)
        elif sort.lower() == "hot":
            listing = sr.hot(limit=limit)
        elif sort.lower() == "top":
            listing = sr.top(limit=limit, time_filter="all")
        else:
            raise ValueError("sort must be one of: new, hot, top")

        page_items = []
        try:
            for submission in listing:
                # if we already have it, skip
                if submission.id in seen_ids:
                    continue

                seen_ids.add(submission.id)
                page_items.append(
                    {
                        "id": submission.id,
                        "fullname": submission.name,  # e.g., t3_xxxxx
                        "subreddit": str(submission.subreddit),
                        "title": submission.title,
                        "selftext": submission.selftext or "",
                        "url": submission.url,
                        "is_self": submission.is_self,
                        "score": submission.score,
                        "num_comments": submission.num_comments,
                        "created_utc": int(submission.created_utc),
                        "created_iso": datetime.fromtimestamp(
                            submission.created_utc, tz=timezone.utc
                        ).isoformat(),
                        "author": str(submission.author) if submission.author else "[deleted]",
                        "permalink": submission.permalink,
                    }
                )

                if len(collected) + len(page_items) >= total_limit:
                    break

        except Exception as e:
            # If Reddit throttles or transient error happens, back off and retry
            print(f"[WARN] Error while fetching listing: {e}. Backing off 3s...")
            time.sleep(3)
            continue

        if not page_items:
            empty_pages += 1
            if empty_pages >= max_empty_pages:
                # We probably hit the practical listing depth limit (~1000)
                break
        else:
            empty_pages = 0

        collected.extend(page_items)

        # Update cursor: last item's fullname for "before" isn't reliably supported for sr.new.
        # But we can still sleep politely and keep going until listing yields no new items.
        time.sleep(polite_sleep)

        # If we're stuck (not getting new posts anymore), break
        if len(collected) >= total_limit:
            break

    # If user requested > ~1000, this loop may stall because listing endpoints usually cap depth.
    # For true deep fetch, switch to search-based paging by created_utc.
    if len(collected) < total_limit and sort.lower() == "new":
        collected = _fetch_posts_via_search_time_paging(
            reddit=reddit,
            subreddit_name=subreddit_name,
            already=collected,
            total_limit=total_limit,
            batch_size=batch_size,
            polite_sleep=polite_sleep,
        )

    return collected[:total_limit]


# helper function for deeper paging using search with timestamp windows, since listing endpoints often cap at ~1000 items
def _fetch_posts_via_search_time_paging(
    reddit: praw.Reddit, subreddit_name: str, already: list[dict], total_limit: int,
    batch_size: int, polite_sleep: float,
) -> list[dict]:
    
    sr = reddit.subreddit(subreddit_name)
    seen_ids = {p["id"] for p in already}
    collected = list(already)

    # Start from "now" and walk backward
    upper = int(time.time())
    # Use a wide lower bound and only constrain upper
    # (You can also do timestamp:LOW..HIGH windows if you want)
    while len(collected) < total_limit:
        remaining = total_limit - len(collected)
        limit = min(batch_size, remaining)

        query = f"timestamp:{0}..{upper}"
        try:
            results = sr.search(
                query=query,
                sort="new",
                syntax="cloudsearch",
                limit=limit,
            )
        except Exception as e:
            print(f"[WARN] Search paging error: {e}. Backing off 3s...")
            time.sleep(3)
            continue

        batch = []
        oldest_ts = None

        for submission in results:
            if submission.id in seen_ids:
                continue
            seen_ids.add(submission.id)

            ts = int(submission.created_utc)
            if oldest_ts is None or ts < oldest_ts:
                oldest_ts = ts

            batch.append(
                {
                    "id": submission.id,
                    "fullname": submission.name,
                    "subreddit": str(submission.subreddit),
                    "title": submission.title,
                    "selftext": submission.selftext or "",
                    "url": submission.url,
                    "is_self": submission.is_self,
                    "score": submission.score,
                    "num_comments": submission.num_comments,
                    "created_utc": ts,
                    "created_iso": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
                    "author": str(submission.author) if submission.author else "[deleted]",
                    "permalink": submission.permalink,
                }
            )

            if len(collected) + len(batch) >= total_limit:
                break

        if not batch:
            break

        collected.extend(batch)

        # Move upper bound backward to continue paging
        if oldest_ts is None:
            break
        upper = oldest_ts - 1

        time.sleep(polite_sleep)

    return collected


def main():
    parser = argparse.ArgumentParser(description="Reddit Scraper")
    parser.add_argument("--subreddit", required=True, help="e.g., tech, cybersecurity")
    parser.add_argument("--limit", type=int, default=200, help="number of posts to fetch")
    parser.add_argument("--sort", choices=["new", "hot", "top"], default="new")
    parser.add_argument("--batch", type=int, default=500, help="API batch size per call")
    args = parser.parse_args()

    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent,
        check_for_async=False,  # helps avoid async warnings in some environments
    )
    posts = fetch_posts(
        reddit=reddit,
        subreddit_name=args.subreddit,
        total_limit=args.limit,
        sort=args.sort,
        batch_size=args.batch,
    )

    print(f"Fetched {len(posts)} posts from r/{args.subreddit}. Sample:")
    for p in posts[:3]:
        print("-", p["created_iso"], p["title"][:80])


if __name__ == "__main__":
    main()