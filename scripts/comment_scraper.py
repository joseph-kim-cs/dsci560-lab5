import os
import json
import time
import requests
from bs4 import BeautifulSoup
# the important function here is fetch_comments_json which takes a reddit post permalink and returns the full reddit json for that post including comments, which can be used to extract comment information and insert into our database using the functions in database_pipeline.py

USER_AGENT = "lab-reddit-scraper/1.0 (contact: dsci560-group19)"

DATA_DIR = "data"


def fetch_top_post_permalink(subreddit: str) -> str | None:
    # scrape first page of old reddit and return permalink of first non promoted post

    url = f"https://old.reddit.com/r/{subreddit}/"
    headers = {"User-Agent": USER_AGENT}

    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    for post in soup.find_all("div", class_="thing"):
        if "promotedlink" in post.get("class", []):
            continue

        comments_tag = post.find("a", class_="comments")
        if comments_tag:
            permalink = comments_tag.get("href")
            if permalink:
                print("top post permalink found:", permalink)
                return permalink

    return None


def fetch_comments_json(permalink: str) -> dict:
    # fetch full reddit json for post including comments
    headers = {"User-Agent": USER_AGENT}

    # ensure proper prefix
    if not permalink.startswith("http"):
        url = f"https://www.reddit.com{permalink}.json"
    else:
        url = permalink + ".json"

    print("fetching json from:", url)
    
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    time.sleep(1.5)

    return r.json()

def fetch_comments_flat(permalink: str, sleep_s: float = 1.5) -> list[dict]:
    """
    returns a flat list of comment dicts with ids and bodies
    skips kind more placeholders for simplicity
    """
    headers = {"User-Agent": USER_AGENT}

    if permalink.startswith("http"):
        url = permalink.rstrip("/") + ".json"
    else:
        url = f"https://www.reddit.com{permalink.rstrip('/')}.json"

    r = requests.get(url, headers=headers, params={"limit": 500, "sort": "top"}, timeout=30)
    r.raise_for_status()
    time.sleep(sleep_s)

    data = r.json()
    if not isinstance(data, list) or len(data) < 2:
        return []

    post_listing = data[0]["data"]["children"]
    if not post_listing:
        return []
    post_fullname = post_listing[0]["data"]["name"]  # t3_xxxxx

    out = []

    def walk(children):
        for child in children:
            kind = child.get("kind")
            cdata = child.get("data", {})
            if kind != "t1":
                continue

            out.append({
                "comment_id": cdata.get("name"),        # t1_xxxxx
                "post_id": post_fullname,               # t3_xxxxx
                "parent_id": cdata.get("parent_id"),    # t1_... or t3_...
                "author": cdata.get("author"),
                "body": cdata.get("body"),
                "score": cdata.get("score"),
                "created_utc": cdata.get("created_utc"),
            })

            replies = cdata.get("replies")
            if isinstance(replies, dict):
                walk(replies.get("data", {}).get("children", []))

    walk(data[1]["data"]["children"])
    return out


def main():
    subreddit = "tech" # quick example call

    permalink = fetch_top_post_permalink(subreddit)
    if not permalink:
        print("no permalink found")
        return

    data = fetch_comments_json(permalink)

    output_path = os.path.join(DATA_DIR, "top_post_comments.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print("json saved to:", output_path)

    # quick sanity summary
    if isinstance(data, list) and len(data) >= 2:
        comments = data[1]["data"]["children"]
        print("top level comment objects:", len(comments))


if __name__ == "__main__":
    main()
