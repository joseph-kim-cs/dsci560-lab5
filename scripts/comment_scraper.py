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
