#python scripts/bs4_scraper.py --subreddit tech --out data/output.csv --max-pages 8

import argparse
import csv
import re
import time
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup


USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"


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


def parse_comments(text: str) -> int:
    if not text:
        return 0
    t = text.strip().lower()
    if t in {"comment", "comments"}:
        return 0
    m = re.search(r"(\d+)", t.replace(",", ""))
    return int(m.group(1)) if m else 0


def fetch_page(url: str, session: requests.Session, debug: bool = False) -> BeautifulSoup:
    r = session.get(url, timeout=30, allow_redirects=True)

    if debug:
        print("\n" + "=" * 80)
        print("[FETCH] Requested URL:", url)
        print("[FETCH] Final URL:", r.url)
        print("[FETCH] Status code:", r.status_code)
        print("[FETCH] Content length:", len(r.text))
        # quick sanity checks
        print("[FETCH] Contains 'old.reddit.com':", "old.reddit.com" in r.url)
        print("[FETCH] Contains 'thing' keyword:", "thing" in r.text)
        print("[FETCH] First 200 chars:\n", r.text[:200].replace("\n", " ")[:200])
        print("=" * 80)

    r.raise_for_status()
    time.sleep(2)
    soup = BeautifulSoup(r.text, "html.parser")

    if debug:
        title = soup.title.get_text(strip=True) if soup.title else None
        print("[PARSE] Page <title>:", title)
        # show some markers that confirm we’re on the listing page
        site_table = soup.find("div", id="siteTable")
        print("[PARSE] Found div#siteTable:", bool(site_table))
        next_button = soup.find("span", class_="next-button")
        print("[PARSE] Found span.next-button:", bool(next_button))

    return soup


def extract_posts(
    soup: BeautifulSoup,
    subreddit: str,
    self_only: bool,
    debug: bool = False,
    debug_limit: int = 5
) -> list[dict]:
    results = []

    all_things = soup.find_all("div", class_="thing")
    if debug:
        print(f"[EXTRACT] Total div.thing found: {len(all_things)}")
        if len(all_things) == 0:
            # Print useful hints when nothing is found
            print("[EXTRACT] WARNING: No div.thing elements found.")
            print("[EXTRACT] First few div class attributes on page:")
            divs = soup.find_all("div", limit=10)
            for i, d in enumerate(divs, start=1):
                print(f"  div#{i} classes={d.get('class')} id={d.get('id')}")

    skipped_promoted = 0
    skipped_self_mismatch = 0
    skipped_no_title = 0
    kept = 0

    for idx, post in enumerate(all_things, start=1):
        classes = post.get("class", [])
        data_domain = post.get("data-domain", "")
        data_promoted = post.get("data-promoted")
        fullname = post.get("data-fullname") or post.get("data-name") or ""
        author = post.get("data-author") or "[deleted]"

        # Debug-print a few raw items to verify HTML structure
        if debug and idx <= debug_limit:
            print("\n[POST RAW] idx:", idx)
            print("  classes:", classes)
            print("  data-domain:", data_domain)
            print("  data-promoted:", data_promoted)
            print("  data-fullname/data-name:", fullname)
            print("  data-author:", author)

        # Skip promoted
        if "promotedlink" in classes or data_promoted in {"true", "1"}:
            skipped_promoted += 1
            if debug and idx <= debug_limit:
                print("  -> SKIP: promoted")
            continue

        # Self-only filter
        if self_only:
            data_type = post.get("data-type", "")
            # Accept either explicit self type or self.* domain
            if not (data_type == "self" or data_domain.startswith("self.")):
                continue


        title_tag = post.find("a", class_="title")
        if not title_tag:
            skipped_no_title += 1
            if debug and idx <= debug_limit:
                print("  -> SKIP: missing a.title")
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
                created_utc = None

        results.append(
            {
                "post_id": fullname,
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
        kept += 1

        if debug and idx <= debug_limit:
            print("  -> KEEP")
            print("     title:", title[:90])
            print("     url:", url[:90])
            print("     comments_text:", comments_text)
            print("     score_text:", score_text)

    if debug:
        print("\n[EXTRACT SUMMARY]")
        print("  kept:", kept)
        print("  skipped_promoted:", skipped_promoted)
        print("  skipped_self_mismatch:", skipped_self_mismatch)
        print("  skipped_no_title:", skipped_no_title)

    return results


def get_next_page_url(soup: BeautifulSoup, debug: bool = False) -> str | None:
    next_button = soup.find("span", class_="next-button")
    if not next_button:
        if debug:
            print("[PAGINATION] No span.next-button found.")
        return None
    a = next_button.find("a")
    next_url = a.get("href") if a else None
    if debug:
        print("[PAGINATION] next_url:", next_url)
    return next_url


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
    parser.add_argument("--subreddit", default="tech", help="e.g., tech, cybersecurity")
    parser.add_argument("--limit", type=int, default=200, help="number of posts to fetch")
    parser.add_argument("--self-only", action="store_true", help="only keep self.<subreddit> posts")
    parser.add_argument("--out", default="data/output.csv", help="output CSV path")

    # Debug flags
    parser.add_argument("--debug", action="store_true", help="print debug info")
    parser.add_argument("--debug-limit", type=int, default=5, help="how many raw posts to print per page")
    parser.add_argument("--max-pages", type=int, default=10, help="stop after this many pages (debug safety)")

    args = parser.parse_args()

    subreddit = args.subreddit.strip().lstrip("r/").lstrip("/")
    url = f"https://old.reddit.com/r/{subreddit}/"

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    collected: list[dict] = []
    seen: set[str] = set()

    pages = 0
    while len(collected) < args.limit:
        pages += 1
        if args.debug:
            print(f"\n########## PAGE {pages} ##########")

        soup = fetch_page(url, session, debug=args.debug)
        page_posts = extract_posts(
            soup,
            subreddit=subreddit,
            self_only=args.self_only,
            debug=args.debug,
            debug_limit=args.debug_limit,
        )

        if args.debug:
            print(f"[PAGE] extracted posts this page: {len(page_posts)}")

        for p in page_posts:
            key = p["post_id"] or p["permalink"] or (p["title"] + p["author"])
            if key in seen:
                continue
            seen.add(key)
            collected.append(p)
            if len(collected) >= args.limit:
                break

        if args.debug:
            print(f"[PROGRESS] collected total: {len(collected)} / {args.limit}")

        if len(collected) >= args.limit:
            break

        next_url = get_next_page_url(soup, debug=args.debug)
        if not next_url:
            break
        url = next_url

        if pages >= args.max_pages:
            if args.debug:
                print(f"[STOP] Reached --max-pages={args.max_pages}")
            break

    write_csv(args.out, collected[: args.limit])
    print(f"\nSaved {min(len(collected), args.limit)} posts to {args.out}")


if __name__ == "__main__":
    main()