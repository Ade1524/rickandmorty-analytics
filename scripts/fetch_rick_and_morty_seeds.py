# scripts/fetch_rick_and_morty_seeds.py
import requests
import pandas as pd
import time
import os
import unicodedata
import re
from typing import List, Dict, Any

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SEEDS_DIR = os.path.join(ROOT, "seeds")
os.makedirs(SEEDS_DIR, exist_ok=True)

BASE = "https://rickandmortyapi.com/api"


def sanitize_header(h: str) -> str:
    # Normalize accents
    h = unicodedata.normalize("NFKD", str(h))
    h = "".join([c for c in h if not unicodedata.combining(c)])
    h = h.strip().lower()
    # replace non-alnum with _
    h = re.sub(r"[^0-9a-z]+", "_", h)
    h = re.sub(r"_+", "_", h).strip("_")
    # avoid starting with digit
    if re.match(r"^\d", h):
        h = "c_" + h
    # limit length
    return h[:128]


def fetch_all(endpoint: str) -> List[Dict[str, Any]]:
    url = f"{BASE}/{endpoint}"
    results = []
    page = 1
    while True:
        resp = requests.get(url, params={"page": page}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        page_results = data.get("results", [])
        if not page_results:
            break
        results.extend(page_results)
        if not data.get("info", {}).get("next"):
            break
        page += 1
        time.sleep(0.25)
    return results


def prepare_characters(raw: List[Dict]) -> pd.DataFrame:
    rows = []
    for r in raw:
        # flatten nested fields and compute derived
        row = {
            "id": r.get("id"),
            "name": r.get("name"),
            "status": r.get("status"),
            "species": r.get("species"),
            "type": r.get("type"),
            "gender": r.get("gender"),
            "origin_name": r.get("origin", {}).get("name"),
            "origin_url": r.get("origin", {}).get("url"),
            "location_name": r.get("location", {}).get("name"),
            "location_url": r.get("location", {}).get("url"),
            "image": r.get("image"),
            "episode_count": len(r.get("episode", [])),
            "episode_urls": "|".join(r.get("episode", [])),
            "url": r.get("url"),
            "created": r.get("created")
        }
        rows.append(row)
    df = pd.DataFrame(rows)
    df.columns = [sanitize_header(c) for c in df.columns]
    return df


def prepare_locations(raw: List[Dict]) -> pd.DataFrame:
    rows = []
    for r in raw:
        rows.append({
            "id": r.get("id"),
            "name": r.get("name"),
            "type": r.get("type"),
            "dimension": r.get("dimension"),
            "residents_count": len(r.get("residents", [])),
            "residents_urls": "|".join(r.get("residents", [])),
            "url": r.get("url"),
            "created": r.get("created")
        })
    df = pd.DataFrame(rows)
    df.columns = [sanitize_header(c) for c in df.columns]
    return df


def prepare_episodes(raw: List[Dict]) -> pd.DataFrame:
    rows = []
    for r in raw:
        rows.append({
            "id": r.get("id"),
            "name": r.get("name"),
            "air_date": r.get("air_date"),
            "episode_code": r.get("episode"),
            "characters_count": len(r.get("characters", [])),
            "characters_urls": "|".join(r.get("characters", [])),
            "url": r.get("url"),
            "created": r.get("created")
        })
    df = pd.DataFrame(rows)
    df.columns = [sanitize_header(c) for c in df.columns]
    return df


def save_df(df: pd.DataFrame, filename: str):
    path = os.path.join(SEEDS_DIR, filename)
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"Saved {path} ({df.shape[0]} rows, {df.shape[1]} cols)")


def main():
    print("Fetching characters...")
    chars = fetch_all("character")
    df_chars = prepare_characters(chars)
    save_df(df_chars, "rick_characters.csv")

    print("Fetching locations...")
    locs = fetch_all("location")
    df_locs = prepare_locations(locs)
    save_df(df_locs, "rick_locations.csv")

    print("Fetching episodes...")
    eps = fetch_all("episode")
    df_eps = prepare_episodes(eps)
    save_df(df_eps, "rick_episodes.csv")


if __name__ == "__main__":
    main()
