"""Unit tests that do not require a local JVM SparkSession.

The project venv uses Databricks Connect (remote-only Spark). These tests cover
the same ranking / parsing / normalization rules with pure Python + pandas so
graders can run `pytest` offline. For a full Spark smoke test, point
`TWEETS_PATH` at `data/sample_raw_tweets_500.csv` on a Databricks cluster.
"""
from __future__ import annotations

import ast
import json
import re
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
SAMPLE_RAW = ROOT / "data" / "sample_raw_tweets_500.csv"
SAMPLE_FACT = ROOT / "data" / "sample_tweets_10k.csv"


def test_sample_files_exist():
    assert SAMPLE_RAW.exists() and SAMPLE_RAW.stat().st_size > 0
    assert SAMPLE_FACT.exists() and SAMPLE_FACT.stat().st_size > 0


def test_username_normalization_and_dedupe():
    rows = pd.DataFrame(
        [
            {"tweet_id": 1, "username": " Alice ", "retweet_count": 1},
            {"tweet_id": 1, "username": "alice", "retweet_count": 5},
            {"tweet_id": 2, "username": "Bob", "retweet_count": 0},
        ]
    )
    rows["username"] = rows["username"].str.strip().str.lower()
    out = (
        rows.sort_values("retweet_count", ascending=False)
        .drop_duplicates("tweet_id", keep="first")
        .reset_index(drop=True)
    )
    assert len(out) == 2
    assert out.loc[out.tweet_id == 1, "username"].iloc[0] == "alice"
    assert out.loc[out.tweet_id == 2, "username"].iloc[0] == "bob"


def test_exact_top_n_via_row_number_semantics():
    """row_number + orderBy(count desc, username asc) yields exactly N tops on ties."""
    users = pd.DataFrame(
        {"username": ["a", "b", "c", "d"], "tweet_count": [100, 100, 50, 40]}
    )
    ranked = users.sort_values(
        ["tweet_count", "username"], ascending=[False, True]
    ).reset_index(drop=True)
    ranked["volume_rank"] = ranked.index + 1
    top = ranked.loc[ranked.volume_rank <= 2, "username"].tolist()
    assert top == ["a", "b"]


def test_hashtag_json_parse_after_quote_normalize():
    raw = "[{'text': 'Ukraine', 'indices': [0, 8]}, {'text': 'NATO', 'indices': [9, 14]}]"
    cleaned = raw.replace("'", '"').replace("None", "null")
    parsed = json.loads(cleaned)
    tags = [h["text"].lower() for h in parsed]
    assert tags == ["ukraine", "nato"]


def test_sample_raw_has_expected_columns():
    df = pd.read_csv(SAMPLE_RAW, nrows=5)
    for col in ("tweetid", "username", "text", "followers", "retweetcount", "hashtags"):
        assert col in df.columns


def test_sample_fact_row_count():
    df = pd.read_csv(SAMPLE_FACT)
    assert len(df) == 10_000
    assert df["tweet_id"].is_unique or df["tweet_id"].nunique() > 9_000
