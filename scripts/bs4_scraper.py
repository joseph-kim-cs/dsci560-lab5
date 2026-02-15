# bs4_scraper.py
# Example:
# python scripts/bs4_scraper.py --subreddit tech --limit 500 --out data/output.csv --max-pages 10

import argparse
import csv
import re
import time
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup


USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) Chrome/120 Safari/537.36"

# convert reddit score text into an integer value
def parse_score(text: str) -> int | None:
    if not text:
        return None
    t = text.strip().lower()
    if t in {"•", "score hidden", "hidden"}:
        return None
    m = re.match(r"^(\d+(?:\.\d+)?)k$", t)
    if m:
        return int(float(m.group(1)) * 1000)
    m = re.match(r"^\d+$", t)
    if m:
        return int(t)
    return None

# convert reddit comment text into an integer count
def parse_comments(text: str) -> int:
    if not text:
        return 0
    m = re.search(r"(\d+)", text.replace(",", ""))
    return int(m.group(1)) if m else 0

# send http request to reddit and return parsed html
def fetch_page(url: str, session: requests.Session) -> BeautifulSoup:
    r = session.get(url, timeout=30, allow_redirects=True)
    r.raise_for_status()
    time.sleep(2)  # polite delay
    return BeautifulSoup(r.text, "html.parser")

# extract post information from a single reddit listing page
def extract_posts(soup: BeautifulSoup, subreddit: str) -> list[dict]:
    results = []

    for post in soup.find_all("div", class_="thing"):

        # Skip promoted
        if "promotedlink" in post.get("class", []) or post.get("data-promoted") in {"true", "1"}:
            continue

        post_id = post.get("data-fullname") or post.get("data-name") or ""
        author = post.get("data-author") or "[deleted]"
        data_domain = post.get("data-domain", "")

        title_tag = post.find("a", class_="title")
        if not title_tag:
            continue

        title = title_tag.get_text(strip=True)
        url = title_tag.get("href", "")

        comments_tag = post.find("a", class_="comments")
        comments_text = comments_tag.get_text(" ", strip=True) if comments_tag else ""
        comments = parse_comments(comments_text)
        permalink = comments_tag.get("href", "") if comments_tag else ""

        score_tag = (
            post.find("div", class_=re.compile(r"\bscore\b"))
            or post.find("span", class_=re.compile(r"\bscore\b"))
        )
        score_text = score_tag.get_text(strip=True) if score_tag else ""
        score = parse_score(score_text)

        created_utc = None
        time_tag = post.find("time")
        if time_tag and time_tag.has_attr("datetime"):
            try:
                dt = datetime.fromisoformat(time_tag["datetime"].replace("Z", "+00:00"))
                created_utc = int(dt.replace(tzinfo=timezone.utc).timestamp())
            except Exception:
                pass

        results.append(
            {
                "post_id": post_id,
                "subreddit": subreddit,
                "data_domain": data_domain,
                "title": title,
                "author": author,
                "score": score,
                "comments": comments,
                "url": url,
                "permalink": permalink,
                "created_utc": created_utc,
            }
        )

    return results

# find the url for the next page in pagination
def get_next_page_url(soup: BeautifulSoup) -> str | None:
    next_button = soup.find("span", class_="next-button")
    if not next_button:
        return None
    a = next_button.find("a")
    return a.get("href") if a else None


def write_csv(path: str, rows: list[dict]) -> None:
    fieldnames = [
        "idx",
        "post_id",
        "subreddit",
        "data_domain",
        "title",
        "author",
        "score",
        "comments",
        "url",
        "permalink",
        "created_utc",
    ]

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for i, r in enumerate(rows, start=1):
            w.writerow({"idx": i, **r})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--subreddit", default="tech")
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--out", default="output.csv")
    parser.add_argument("--max-pages", type=int, default=20)
    args = parser.parse_args()

    subreddit = args.subreddit.strip().lstrip("r/").lstrip("/")
    url = f"https://old.reddit.com/r/{subreddit}/"

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    collected = []
    seen = set()
    pages = 0

    while len(collected) < args.limit and pages < args.max_pages:
        pages += 1
        soup = fetch_page(url, session)
        page_posts = extract_posts(soup, subreddit)

        for p in page_posts:
            key = p["post_id"] or p["permalink"] or (p["title"] + p["author"])
            if key in seen:
                continue
            seen.add(key)
            collected.append(p)
            if len(collected) >= args.limit:
                break

        next_url = get_next_page_url(soup)
        if not next_url:
            break
        url = next_url

    write_csv(args.out, collected[: args.limit])
    print(f"Saved {min(len(collected), args.limit)} posts to {args.out}")


if __name__ == "__main__":
    main()
