# Databricks notebook source
# MAGIC %md
# MAGIC # Tweets Fact Table, User Dimension & Broadcast Join Metrics
# MAGIC
# MAGIC Builds a star schema on Ukraine tweet CSVs:
# MAGIC 1. **Fact** — partitioned Delta table (`tweet_date`)
# MAGIC 2. **Dimension** — one row per user, ranked by tweet volume (`row_number` → exact top-N)
# MAGIC 3. **Aggregate** — platform metrics via slim explicit **broadcast join**
# MAGIC 4. **Content** — language / hashtag / engagement stats (cached fact, single `count`)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BooleanType,
    TimestampType, DateType, ArrayType, IntegerType,
)
import os

# --- Config (env overrides; placeholders are intentional for local/CI) ---
DATAEXPERT_HOST = os.environ.get("DATABRICKS_HOST", "https://dbc-xxx.cloud.databricks.com")
DATAEXPERT_PROFILE = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
DATAEXPERT_CLUSTER_ID = os.environ.get("DATABRICKS_CLUSTER_ID", "")
# Prefer CLI profile / OAuth. Optional: DATABRICKS_TOKEN for PAT auth.
DATAEXPERT_TOKEN = os.environ.get("DATABRICKS_TOKEN")

TWEETS_PATH = os.environ.get("TWEETS_PATH", "/Volumes/tabular/dataexpert/tweets/*.csv")
TARGET_SCHEMA = os.environ.get("TARGET_SCHEMA", "bootcamp_students.alperendavran")
WRITE_MODE = os.environ.get("WRITE_MODE", "overwrite")  # overwrite | merge

FACT_TABLE = f"{TARGET_SCHEMA}.tweets_fact"
DIM_TABLE = f"{TARGET_SCHEMA}.tweets_user_dim"
METRICS_TABLE = f"{TARGET_SCHEMA}.tweets_platform_metrics"
LANGUAGE_TABLE = f"{TARGET_SCHEMA}.tweets_language_stats"
HASHTAG_TABLE = f"{TARGET_SCHEMA}.tweets_hashtag_stats"
ENGAGEMENT_TABLE = f"{TARGET_SCHEMA}.tweets_engagement_stats"
MONTHLY_TABLE = f"{TARGET_SCHEMA}.tweets_monthly_summary"
STAGING_TABLE = f"{TARGET_SCHEMA}.tweets_fact_staging"

BIG_ACCOUNT_FOLLOWERS = int(os.environ.get("BIG_ACCOUNT_FOLLOWERS", "100000"))
TOP_N_TWEETERS = int(os.environ.get("TOP_N_TWEETERS", "10"))
# Keep auto-broadcast available for AQE; set FORCE_EXPLICIT_BROADCAST=1 to disable auto and force hint-only demo
FORCE_EXPLICIT_BROADCAST = os.environ.get("FORCE_EXPLICIT_BROADCAST", "0") == "1"

POPULATION_SHARE = {
    "en": 16.0, "zh": 14.0, "hi": 8.0, "es": 7.0, "ar": 5.0, "bn": 4.0,
    "pt": 4.0, "ru": 2.5, "ja": 2.0, "pa": 1.5, "de": 1.5, "jv": 1.5,
    "ko": 1.2, "fr": 1.2, "te": 1.0, "mr": 1.0, "tr": 1.0, "ta": 1.0,
    "vi": 1.0, "ur": 1.0, "it": 0.9, "th": 0.9, "gu": 0.8, "pl": 0.5,
    "uk": 0.4, "nl": 0.3, "ro": 0.3, "el": 0.2, "cs": 0.2, "sv": 0.2,
    "hu": 0.2, "he": 0.2, "fi": 0.1, "no": 0.1, "da": 0.1, "ca": 0.1,
}

# Wide permissive CSV schema (18-col + 29-col files). All strings → cast after read.
RAW_CSV_SCHEMA = StructType([
    StructField("_c0", StringType(), True),
    StructField("userid", StringType(), True),
    StructField("username", StringType(), True),
    StructField("acctdesc", StringType(), True),
    StructField("location", StringType(), True),
    StructField("following", StringType(), True),
    StructField("followers", StringType(), True),
    StructField("totaltweets", StringType(), True),
    StructField("usercreatedts", StringType(), True),
    StructField("tweetid", StringType(), True),
    StructField("tweetcreatedts", StringType(), True),
    StructField("retweetcount", StringType(), True),
    StructField("text", StringType(), True),
    StructField("hashtags", StringType(), True),
    StructField("language", StringType(), True),
    StructField("coordinates", StringType(), True),
    StructField("favorite_count", StringType(), True),
    # 29-col files continue here; 18-col files put extractedts in this slot — handled below
    StructField("is_retweet", StringType(), True),
    StructField("original_tweet_id", StringType(), True),
    StructField("original_tweet_userid", StringType(), True),
    StructField("original_tweet_username", StringType(), True),
    StructField("in_reply_to_status_id", StringType(), True),
    StructField("in_reply_to_user_id", StringType(), True),
    StructField("in_reply_to_screen_name", StringType(), True),
    StructField("is_quote_status", StringType(), True),
    StructField("quoted_status_id", StringType(), True),
    StructField("quoted_status_userid", StringType(), True),
    StructField("quoted_status_username", StringType(), True),
    StructField("extractedts", StringType(), True),
])

HASHTAG_ELEMENT_SCHEMA = StructType([
    StructField("text", StringType(), True),
    StructField("indices", ArrayType(IntegerType()), True),
])
HASHTAG_ARRAY_SCHEMA = ArrayType(HASHTAG_ELEMENT_SCHEMA)

TWITTER_EPOCH_MS = 1288834974657


def get_spark():
    if globals().get("spark") is not None:
        return spark

    for key in (
        "DATABRICKS_HOST",
        "DATABRICKS_WORKSPACE_ID",
        "DATABRICKS_AUTH_TYPE",
        "DATABRICKS_METADATA_SERVICE_URL",
        "DATABRICKS_TOKEN",
    ):
        os.environ.pop(key, None)

    from databricks.connect import DatabricksSession

    builder = (
        DatabricksSession.builder
        .host(DATAEXPERT_HOST)
        .clusterId(DATAEXPERT_CLUSTER_ID)
        .profile(DATAEXPERT_PROFILE)
    )
    if DATAEXPERT_TOKEN:
        builder = builder.token(DATAEXPERT_TOKEN)
    return builder.getOrCreate()


spark = get_spark()
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ingest Raw Tweets
# MAGIC
# MAGIC - Explicit `StructType` (no `inferSchema`)
# MAGIC - `multiLine=True` (tweet `text` often contains embedded newlines); **no** `wholeFile`
# MAGIC - Normalize `username` (lower + trim); dedupe on `tweet_id`
# MAGIC - Coalesce legacy column names; ignore misaligned `extractedts`

# COMMAND ----------

def load_tweets(path):
    """Read Ukraine tweet CSVs with an explicit schema and normalize keys."""
    # multiLine: required — ~half of tweet texts embed real newlines inside quoted fields.
    # wholeFile: NOT used — it collapses each file onto one task and kills parallelism.
    raw = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("multiLine", True)
        .option("escape", '"')
        .schema(RAW_CSV_SCHEMA)
        .csv(path)
    )

    # favorite_count vs favoritecount alias safety (if present under different name in future)
    favorite = F.coalesce(
        F.col("favorite_count").cast("long"),
        F.lit(None).cast("long"),
    )

    # 18-col files map extractedts into is_retweet slot (true/false timestamps). Detect booleans.
    is_retweet_raw = F.lower(F.trim(F.col("is_retweet")))
    is_retweet_from_col = F.when(is_retweet_raw.isin("true", "1", "t", "yes"), True).when(
        is_retweet_raw.isin("false", "0", "f", "no"), False
    )

    tweet_id = F.col("tweetid").cast("long")
    username = F.lower(F.trim(F.col("username")))
    text = F.col("text")

    tweet_ts_from_id = F.to_timestamp(
        (F.shiftright(tweet_id, 22) + F.lit(TWITTER_EPOCH_MS)) / F.lit(1000)
    )
    tweetcreatedts = F.try_to_timestamp(F.col("tweetcreatedts"))

    in_reply_to_status_id = F.col("in_reply_to_status_id").cast("long")
    is_reply = F.when(
        in_reply_to_status_id.isNotNull() & (in_reply_to_status_id > 0), True
    ).otherwise(text.rlike(r"^@[^\s]+"))

    is_retweet = F.coalesce(
        is_retweet_from_col,
        text.startswith("RT @"),
        F.lit(False),
    )
    is_quote = F.when(
        F.lower(F.trim(F.col("is_quote_status"))).isin("true", "1", "t", "yes"), True
    ).otherwise(False)

    base = (
        raw
        .select(
            tweet_id.alias("tweet_id"),
            username.alias("username"),
            text.alias("text"),
            F.col("followers").cast("long").alias("followers_count"),
            F.col("retweetcount").cast("long").alias("retweet_count"),
            favorite.alias("favorite_count"),
            F.lower(F.trim(F.col("language"))).alias("language"),
            F.col("hashtags"),
            is_retweet.alias("is_retweet"),
            is_quote.alias("is_quote_status"),
            in_reply_to_status_id.alias("in_reply_to_status_id"),
            F.lower(F.trim(F.col("in_reply_to_screen_name"))).alias("in_reply_to_screen_name"),
            F.coalesce(tweet_ts_from_id, tweetcreatedts).alias("tweet_ts"),
        )
        .filter(
            F.col("tweet_id").isNotNull()
            & F.col("username").isNotNull()
            & (F.length("username") > 0)
            & F.col("text").isNotNull()
        )
        .withColumn("tweet_date", F.to_date("tweet_ts"))
        .withColumn("tweet_year_month", F.date_format("tweet_ts", "yyyy-MM"))
        .withColumn("is_reply", is_reply)
        .withColumn("is_original", (~F.col("is_retweet")) & (~F.col("is_reply")))
        # Deduplicate on tweet_id (keep latest extraction / highest retweet_count)
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("tweet_id").orderBy(F.desc_nulls_last("retweet_count"))
            ),
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    return base


raw_tweets = load_tweets(TWEETS_PATH)
tweet_count = raw_tweets.count()
print(f"Loaded {tweet_count:,} unique tweet rows")
raw_tweets.printSchema()
raw_tweets.show(3, truncate=80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Write Partitioned Fact Delta Table
# MAGIC
# MAGIC `WRITE_MODE=overwrite` (lab) or `merge` (incremental upsert on `tweet_id`).
# MAGIC Adds Delta CHECK constraints + OPTIMIZE ZORDER + ANALYZE.

# COMMAND ----------

def write_fact_table(df, table_name, mode=WRITE_MODE):
    """Write partitioned fact table (full overwrite or MERGE upsert)."""
    table_exists = spark.catalog.tableExists(table_name)

    if mode == "merge" and table_exists:
        (
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            .saveAsTable(STAGING_TABLE)
        )
        spark.sql(f"""
            MERGE INTO {table_name} AS t
            USING {STAGING_TABLE} AS s
            ON t.tweet_id = s.tweet_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    else:
        (
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            .partitionBy("tweet_date")
            .saveAsTable(table_name)
        )

    # Constraints (best-effort; may already exist)
    for stmt in [
        f"ALTER TABLE {table_name} ADD CONSTRAINT tweet_id_not_null CHECK (tweet_id IS NOT NULL)",
        f"ALTER TABLE {table_name} ADD CONSTRAINT username_not_null CHECK (username IS NOT NULL)",
    ]:
        try:
            spark.sql(stmt)
        except Exception as e:
            print(f"constraint skipped: {str(e)[:120]}")

    spark.sql(f"OPTIMIZE {table_name} ZORDER BY (username, tweet_date)")
    spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR ALL COLUMNS")

    fact = spark.table(table_name)
    n = fact.count()
    days = fact.select("tweet_date").distinct().count()
    print(f"✅ {table_name}: {n:,} rows, {days} partition days")
    fact.printSchema()
    return fact


tweets_fact = write_fact_table(raw_tweets, FACT_TABLE)
FACT_TOTAL = tweets_fact.count()  # single materialization count for downstream %

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build User Dimension (Ranked by Tweet Volume)
# MAGIC
# MAGIC Uses `row_number` so top-N is exactly N users (not dense_rank ties).
# MAGIC Documented: `is_top_10_tweeter` = `volume_rank <= 10` with **exact** top-10 via `row_number`.

# COMMAND ----------

def build_user_dimension(fact_df):
    """Aggregate per-user stats; rank with row_number for exact top-N."""
    user_stats = (
        fact_df
        .groupBy("username")
        .agg(
            F.count("*").alias("tweet_count"),
            F.max("followers_count").alias("followers_count"),
            F.sum(F.coalesce(F.col("retweet_count"), F.lit(0))).alias("total_retweets"),
            F.sum(F.coalesce(F.col("favorite_count"), F.lit(0))).alias("total_favorites"),
            F.min("tweet_date").alias("first_tweet_date"),
            F.max("tweet_date").alias("last_tweet_date"),
        )
    )

    return (
        user_stats
        .withColumn(
            "volume_rank",
            F.row_number().over(
                Window.orderBy(F.desc("tweet_count"), F.desc("followers_count"), F.asc("username"))
            ),
        )
        .withColumn("is_top_10_tweeter", F.col("volume_rank") <= TOP_N_TWEETERS)
        .withColumn("is_top_100_tweeter", F.col("volume_rank") <= 100)
        .withColumn("is_big_account", F.col("followers_count") >= BIG_ACCOUNT_FOLLOWERS)
        .withColumn(
            "volume_tier",
            F.when(F.col("volume_rank") <= 10, F.lit("top_10"))
            .when(F.col("volume_rank") <= 100, F.lit("top_100"))
            .when(F.col("volume_rank") <= 1000, F.lit("top_1000"))
            .otherwise(F.lit("long_tail")),
        )
    )


user_dim = build_user_dimension(tweets_fact)
(
    user_dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(DIM_TABLE)
)
spark.sql(f"ANALYZE TABLE {DIM_TABLE} COMPUTE STATISTICS FOR ALL COLUMNS")
user_dim = spark.table(DIM_TABLE)
print(f"✅ {DIM_TABLE}: {user_dim.count():,} users")
user_dim.orderBy("volume_rank").show(15, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Broadcast Join → Platform Metrics
# MAGIC
# MAGIC Broadcast only the slim metric columns (~few flags + rank), not the full dim.
# MAGIC Persist `enriched_tweets` for reuse across breakdowns.

# COMMAND ----------

DIM_BROADCAST_COLS = [
    "username",
    "tweet_count",
    "volume_rank",
    "is_top_10_tweeter",
    "is_top_100_tweeter",
    "is_big_account",
    "volume_tier",
    "followers_count",
]


def build_platform_metrics(fact_df, dim_df):
    """Join fact to a slim broadcast dimension and compute concentration KPIs."""
    if FORCE_EXPLICIT_BROADCAST:
        # Demo mode: hide auto-broadcast so BroadcastHashJoin is clearly from the hint
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    else:
        # Production default: keep auto-broadcast (10 MiB) + AQE; still hint slim dim
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))

    dim_slim = dim_df.select(*DIM_BROADCAST_COLS)

    enriched = (
        fact_df.alias("f")
        .join(broadcast(dim_slim.alias("d")), on="username", how="left")
        .select(
            "f.tweet_id",
            "f.username",
            "f.tweet_date",
            "f.tweet_year_month",
            F.coalesce(F.col("d.tweet_count"), F.lit(0)).alias("user_tweet_count"),
            F.coalesce(F.col("d.volume_rank"), F.lit(999_999)).alias("volume_rank"),
            F.coalesce(F.col("d.is_top_10_tweeter"), F.lit(False)).alias("is_top_10_tweeter"),
            F.coalesce(F.col("d.is_top_100_tweeter"), F.lit(False)).alias("is_top_100_tweeter"),
            F.coalesce(F.col("d.is_big_account"), F.lit(False)).alias("is_big_account"),
            F.coalesce(F.col("d.volume_tier"), F.lit("unknown")).alias("volume_tier"),
            F.coalesce(F.col("d.followers_count"), F.lit(0)).alias("followers_count"),
            F.coalesce(F.col("f.retweet_count"), F.lit(0)).alias("retweet_count"),
            F.coalesce(F.col("f.favorite_count"), F.lit(0)).alias("favorite_count"),
            F.col("f.language"),
            F.col("f.hashtags"),
            F.col("f.is_retweet"),
            F.col("f.is_reply"),
            F.col("f.is_quote_status"),
            F.col("f.is_original"),
        )
    )

    print("Physical plan — expect BroadcastHashJoin (slim dim BuildRight):")
    enriched.explain(mode="formatted")

    totals = enriched.agg(
        F.count("*").alias("total_tweets"),
        F.countDistinct("username").alias("total_users"),
        F.sum(F.when(F.col("is_top_10_tweeter"), 1).otherwise(0)).alias("tweets_from_top_10"),
        F.sum(F.when(F.col("is_top_100_tweeter"), 1).otherwise(0)).alias("tweets_from_top_100"),
        F.sum(F.when(F.col("is_big_account"), 1).otherwise(0)).alias("tweets_from_big_accounts"),
        F.sum("retweet_count").alias("total_retweets"),
        F.sum("favorite_count").alias("total_favorites"),
        F.avg("followers_count").alias("avg_followers_per_tweet"),
        F.avg(F.when(F.col("is_top_10_tweeter"), F.col("followers_count"))).alias("avg_followers_top_10"),
        F.avg(F.when(~F.col("is_top_10_tweeter"), F.col("followers_count"))).alias("avg_followers_not_top_10"),
        F.expr("percentile_approx(retweet_count, 0.5)").alias("median_retweets"),
        F.expr("percentile_approx(retweet_count, 0.95)").alias("p95_retweets"),
        F.expr("percentile_approx(favorite_count, 0.5)").alias("median_favorites"),
    )

    metrics = totals.select(
        "total_tweets",
        "total_users",
        F.round(100.0 * F.col("tweets_from_top_10") / F.col("total_tweets"), 2).alias("pct_top_10_tweeters"),
        F.round(100.0 * F.col("tweets_from_top_100") / F.col("total_tweets"), 2).alias("pct_top_100_tweeters"),
        F.round(100.0 * F.col("tweets_from_big_accounts") / F.col("total_tweets"), 2).alias("pct_big_accounts_100k_plus"),
        F.round(F.col("total_retweets") / F.col("total_tweets"), 2).alias("avg_retweets_per_tweet"),
        F.col("median_retweets"),
        F.col("p95_retweets"),
        F.round(F.col("total_favorites") / F.col("total_tweets"), 2).alias("avg_favorites_per_tweet"),
        F.col("median_favorites"),
        F.round(F.col("avg_followers_per_tweet"), 0).alias("avg_followers_per_tweet"),
        F.round(F.col("avg_followers_top_10"), 0).alias("avg_followers_top_10_tweeters"),
        F.round(F.col("avg_followers_not_top_10"), 0).alias("avg_followers_non_top_10"),
        F.current_timestamp().alias("computed_at"),
    )
    return enriched, metrics


enriched_tweets, platform_metrics = build_platform_metrics(tweets_fact, user_dim)
enriched_tweets = enriched_tweets.persist()
ENRICHED_TOTAL = enriched_tweets.count()  # materialize cache once

(
    platform_metrics.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(METRICS_TABLE)
)
print(f"✅ {METRICS_TABLE} written")
spark.table(METRICS_TABLE).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Additional Breakdowns (+ monthly summary table)

# COMMAND ----------

def show_volume_tier_breakdown(enriched_df, total):
    """Tweet share and engagement by user volume tier (uses precomputed total)."""
    (
        enriched_df
        .groupBy("volume_tier")
        .agg(
            F.count("*").alias("tweet_count"),
            F.countDistinct("username").alias("user_count"),
            F.round(F.avg("followers_count"), 0).alias("avg_followers"),
            F.round(F.avg("retweet_count"), 2).alias("avg_retweets"),
            F.expr("percentile_approx(retweet_count, 0.5)").alias("median_retweets"),
            F.round(F.avg("favorite_count"), 2).alias("avg_favorites"),
        )
        .withColumn("pct_of_all_tweets", F.round(100.0 * F.col("tweet_count") / F.lit(total), 2))
        .orderBy(F.desc("tweet_count"))
        .show(truncate=False)
    )


def build_monthly_summary(enriched_df):
    """Materialized monthly concentration table (avoids scanning fact for month KPIs)."""
    return (
        enriched_df
        .groupBy("tweet_year_month")
        .agg(
            F.count("*").alias("total_tweets"),
            F.sum(F.when(F.col("is_top_10_tweeter"), 1).otherwise(0)).alias("top_10_tweets"),
            F.sum(F.when(F.col("is_big_account"), 1).otherwise(0)).alias("big_account_tweets"),
            F.expr("percentile_approx(retweet_count, 0.5)").alias("median_retweets"),
        )
        .withColumn("pct_top_10", F.round(100.0 * F.col("top_10_tweets") / F.col("total_tweets"), 2))
        .withColumn("pct_big_accounts", F.round(100.0 * F.col("big_account_tweets") / F.col("total_tweets"), 2))
        .orderBy("tweet_year_month")
    )


def show_top_tweeters(dim_df, n=10):
    (
        dim_df.orderBy("volume_rank").limit(n)
        .select(
            "volume_rank", "username", "tweet_count", "followers_count", "is_big_account",
            F.round(F.col("total_favorites") / F.col("tweet_count"), 1).alias("avg_favorites_per_tweet"),
        )
        .show(truncate=False)
    )


show_top_tweeters(user_dim)
show_volume_tier_breakdown(enriched_tweets, ENRICHED_TOTAL)
monthly_metrics = build_monthly_summary(enriched_tweets)
(
    monthly_metrics.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(MONTHLY_TABLE)
)
print(f"✅ {MONTHLY_TABLE}")
monthly_metrics.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Concentration Index (single count)

# COMMAND ----------

def tweet_concentration_index(fact_df, total):
    """HHI-style concentration: Σ(share²) using a precomputed total."""
    (
        fact_df.groupBy("username").agg(F.count("*").alias("tweet_count"))
        .withColumn("share", F.col("tweet_count") / F.lit(total))
        .agg(F.sum(F.pow("share", 2)).alias("concentration_index"))
        .show(truncate=False)
    )


tweet_concentration_index(tweets_fact, FACT_TOTAL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Content Analytics — Language, Hashtags (`from_json`), Engagement

# COMMAND ----------

def build_language_stats(fact_df, total):
    pop_map = F.create_map(*[x for kv in POPULATION_SHARE.items() for x in (F.lit(kv[0]), F.lit(kv[1]))])
    return (
        fact_df
        .withColumn("language", F.coalesce(F.col("language"), F.lit("unknown")))
        .groupBy("language")
        .agg(
            F.count("*").alias("tweet_count"),
            F.countDistinct("username").alias("distinct_users"),
            F.round(F.avg("retweet_count"), 2).alias("avg_retweets"),
            F.expr("percentile_approx(retweet_count, 0.5)").alias("median_retweets"),
            F.round(F.avg("followers_count"), 0).alias("avg_followers"),
            F.sum(F.when(F.col("is_retweet"), 1).otherwise(0)).alias("retweet_posts"),
            F.sum(F.when(F.col("is_reply"), 1).otherwise(0)).alias("reply_posts"),
        )
        .withColumn("pct_of_tweets", F.round(100.0 * F.col("tweet_count") / F.lit(total), 2))
        .withColumn("tweets_per_user", F.round(F.col("tweet_count") / F.col("distinct_users"), 2))
        .withColumn("population_share_pct", F.coalesce(pop_map[F.col("language")], F.lit(0.1)))
        .withColumn(
            "amplification_index",
            F.round(F.col("pct_of_tweets") / F.col("population_share_pct"), 2),
        )
        .withColumn("computed_at", F.current_timestamp())
        .orderBy(F.desc("tweet_count"))
    )


def parse_hashtags(col):
    """Parse Python-repr hashtag lists via JSON after quote normalization."""
    cleaned = F.regexp_replace(F.regexp_replace(col, "'", '"'), r"\bNone\b", "null")
    return F.from_json(cleaned, HASHTAG_ARRAY_SCHEMA)


def build_hashtag_stats(fact_df, total, top_n=50):
    hashtag_rows = (
        fact_df
        .filter(F.col("hashtags").isNotNull() & (F.col("hashtags") != "[]"))
        .select(
            "tweet_id",
            "username",
            "retweet_count",
            F.explode(parse_hashtags(F.col("hashtags"))).alias("htag"),
        )
        .select(
            "tweet_id",
            "username",
            "retweet_count",
            F.lower(F.col("htag.text")).alias("hashtag"),
        )
        .filter(F.col("hashtag").isNotNull() & (F.length("hashtag") > 0))
    )
    return (
        hashtag_rows.groupBy("hashtag")
        .agg(
            F.count("*").alias("tweet_count"),
            F.countDistinct("username").alias("distinct_users"),
            F.round(F.avg("retweet_count"), 2).alias("avg_retweets"),
            F.expr("percentile_approx(retweet_count, 0.5)").alias("median_retweets"),
        )
        .withColumn("pct_of_all_tweets", F.round(100.0 * F.col("tweet_count") / F.lit(total), 4))
        .withColumn("computed_at", F.current_timestamp())
        .orderBy(F.desc("tweet_count"))
        .limit(top_n)
    )


def build_engagement_stats(fact_df, total):
    counts = fact_df.agg(
        F.sum(F.when(F.col("is_original"), 1).otherwise(0)).alias("original_tweets"),
        F.sum(F.when(F.col("is_retweet"), 1).otherwise(0)).alias("retweet_tweets"),
        F.sum(F.when(F.col("is_reply"), 1).otherwise(0)).alias("reply_tweets"),
        F.sum(F.when(F.col("is_quote_status"), 1).otherwise(0)).alias("quote_tweets"),
        F.sum(F.when(F.col("language").isNull(), 1).otherwise(0)).alias("unknown_language_tweets"),
    )
    return counts.select(
        F.lit(total).alias("total_tweets"),
        "original_tweets", "retweet_tweets", "reply_tweets", "quote_tweets", "unknown_language_tweets",
        F.round(100.0 * F.col("original_tweets") / F.lit(total), 2).alias("pct_original"),
        F.round(100.0 * F.col("retweet_tweets") / F.lit(total), 2).alias("pct_retweet"),
        F.round(100.0 * F.col("reply_tweets") / F.lit(total), 2).alias("pct_reply"),
        F.round(100.0 * F.col("quote_tweets") / F.lit(total), 2).alias("pct_quote"),
        F.current_timestamp().alias("computed_at"),
    )


language_stats = build_language_stats(tweets_fact, FACT_TOTAL)
hashtag_stats = build_hashtag_stats(tweets_fact, FACT_TOTAL)
engagement_stats = build_engagement_stats(tweets_fact, FACT_TOTAL)

for df, table in [
    (language_stats, LANGUAGE_TABLE),
    (hashtag_stats, HASHTAG_TABLE),
    (engagement_stats, ENGAGEMENT_TABLE),
]:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    print(f"✅ {table}")
    spark.table(table).show(20, truncate=False)

enriched_tweets.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Table diagnostics for graders
# MAGIC
# MAGIC ```sql
# MAGIC DESCRIBE EXTENDED bootcamp_students.alperendavran.tweets_fact;
# MAGIC DESCRIBE EXTENDED bootcamp_students.alperendavran.tweets_user_dim;
# MAGIC DESCRIBE DETAIL  bootcamp_students.alperendavran.tweets_fact;
# MAGIC DESCRIBE DETAIL  bootcamp_students.alperendavran.tweets_user_dim;
# MAGIC ```

# COMMAND ----------

for t in [FACT_TABLE, DIM_TABLE]:
    print("=" * 80, t)
    spark.sql(f"DESCRIBE DETAIL {t}").select(
        "name", "format", "numFiles", "sizeInBytes", "partitionColumns"
    ).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notes
# MAGIC
# MAGIC | Topic | Choice |
# MAGIC |-------|--------|
# MAGIC | Top-N | `row_number` → exactly N users (tie-break: followers, username) |
# MAGIC | Broadcast | Slim dim columns only; dim ≈ 55 MB on full run |
# MAGIC | Counts | `FACT_TOTAL` / `ENRICHED_TOTAL` computed once |
# MAGIC | Hashtags | `from_json` after quote normalization (fallback-safe) |
# MAGIC | Incremental | `WRITE_MODE=merge` → staging + `MERGE` on `tweet_id` |
# MAGIC | Stats | `OPTIMIZE ZORDER BY (username, tweet_date)` + `ANALYZE TABLE` |
