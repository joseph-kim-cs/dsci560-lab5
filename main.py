from scripts.database_pipeline import connect_mysql, init_schema, upsert_posts, upsert_comments
from scripts.comment_scraper import fetch_comments_json_flat 

# main function to run the full pipeline once, fetching posts and comments and inserting into mysql database once
def run_iteration(posts: list[dict]):
    cnx = connect_mysql()
    init_schema(cnx)

    upsert_posts(cnx, posts)

    for p in posts:
        if not p.get("permalink"):
            continue
        comments = fetch_comments_json_flat(p["permalink"])
        upsert_comments(cnx, comments)

    cnx.close()
