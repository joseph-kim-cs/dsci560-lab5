import argparse
import csv
import re
from typing import List, Dict, Any

import numpy as np
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.feature_extraction.text import TfidfVectorizer
from dotenv import load_dotenv


URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
NON_ALNUM_RE = re.compile(r"[^a-zA-Z0-9\s]+")


def clean_text(t: str) -> str:
    if not t:
        return ""
    t = URL_RE.sub(" ", t)
    t = NON_ALNUM_RE.sub(" ", t)
    t = t.lower()
    t = " ".join(t.split())
    return t


def read_posts_csv(path: str) -> List[Dict[str, Any]]:
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    return rows


def fetch_posts_mysql(subreddit: str, limit: int) -> List[Dict[str, Any]]:
    # Lazy import so CSV mode works without DB
    from scripts.database_pipeline import connect_mysql

    cnx = connect_mysql()
    cur = cnx.cursor(dictionary=True)

    if subreddit:
        cur.execute(
            """
            SELECT post_id, title, author, score, permalink, created_utc
            FROM reddit_posts
            WHERE subreddit = %s
            ORDER BY created_utc DESC
            LIMIT %s
            """,
            (subreddit, limit),
        )
    else:
        cur.execute(
            """
            SELECT post_id, title, author, score, permalink, created_utc
            FROM reddit_posts
            ORDER BY created_utc DESC
            LIMIT %s
            """,
            (limit,),
        )

    rows = cur.fetchall()
    cur.close()
    cnx.close()
    return rows


def top_keywords_by_cluster(texts: List[str], labels: np.ndarray, top_k: int = 8):
    results = {}
    for cluster_id in sorted(set(labels.tolist())):
        cluster_docs = [texts[i] for i in range(len(texts)) if labels[i] == cluster_id]
        if len(cluster_docs) < 2:
            results[cluster_id] = []
            continue

        vec = TfidfVectorizer(stop_words="english", max_features=5000)
        X = vec.fit_transform(cluster_docs)
        terms = vec.get_feature_names_out()

        avg = np.asarray(X.mean(axis=0)).ravel()
        top_idx = avg.argsort()[-top_k:][::-1]
        results[cluster_id] = [terms[i] for i in top_idx if avg[i] > 0]

    return results


def closest_to_centroid(embeddings: np.ndarray, labels: np.ndarray, centers: np.ndarray, top_n: int = 5):
    E = embeddings / (np.linalg.norm(embeddings, axis=1, keepdims=True) + 1e-12)
    C = centers / (np.linalg.norm(centers, axis=1, keepdims=True) + 1e-12)

    out = {}
    for c in range(C.shape[0]):
        idxs = np.where(labels == c)[0]
        if len(idxs) == 0:
            out[c] = []
            continue

        sims = E[idxs] @ C[c]
        best = idxs[np.argsort(-sims)[:top_n]]
        out[c] = best.tolist()
    return out


def plot_clusters(embeddings: np.ndarray, labels: np.ndarray, out_path: str):
    pca = PCA(n_components=2, random_state=0)
    pts = pca.fit_transform(embeddings)

    plt.figure()
    plt.scatter(pts[:, 0], pts[:, 1], c=labels)
    plt.title("Reddit Post Clusters (PCA Projection)")
    plt.xlabel("PC1")
    plt.ylabel("PC2")
    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close()


def main():
    load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["csv", "mysql"], default="csv")
    parser.add_argument("--csv", default="data/output.csv")
    parser.add_argument("--textcol", default="title")
    parser.add_argument("--subreddit", default="tech", help="only used for mysql source; empty means all")
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--k", type=int, default=5)
    parser.add_argument("--plot", default="data/cluster_plot.png")
    args = parser.parse_args()

    if args.source == "csv":
        rows = read_posts_csv(args.csv)[: args.limit]
        if not rows:
            print(f"No rows found in CSV: {args.csv}")
            return
        raw_texts = [(r.get(args.textcol) or "") for r in rows]
    else:
        rows = fetch_posts_mysql(args.subreddit if args.subreddit else "", args.limit)
        if not rows:
            print("No posts found in MySQL. Run main.py first to load posts.")
            return
        raw_texts = [r.get("title") or "" for r in rows]

    texts = [clean_text(t) for t in raw_texts]

    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer("all-MiniLM-L6-v2")
    embeddings = model.encode(texts, show_progress_bar=True)
    embeddings = np.array(embeddings)

    k = max(2, args.k)
    km = KMeans(n_clusters=k, random_state=0, n_init="auto")
    labels = km.fit_predict(embeddings)

    kw = top_keywords_by_cluster(texts, labels, top_k=8)
    closest = closest_to_centroid(embeddings, labels, km.cluster_centers_, top_n=5)

    print("\n=== Cluster Report ===")
    for c in range(k):
        idxs = np.where(labels == c)[0]
        print(f"\nCluster {c} | size={len(idxs)} | keywords={kw.get(c, [])}")
        for rank, i in enumerate(closest[c], start=1):
            title = raw_texts[i]
            post_id = rows[i].get("post_id") or rows[i].get("post_id", "")
            permalink = rows[i].get("permalink") or ""
            print(f"  {rank}. post_id={post_id} title={title}")
            if permalink:
                print(f"     permalink={permalink}")

    plot_clusters(embeddings, labels, args.plot)
    print(f"\nSaved cluster visualization to: {args.plot}")


if __name__ == "__main__":
    main()
