# Databricks notebook source
# MAGIC %md
# MAGIC # 🐦 Tweets Fact Table, User Dimension & Broadcast Join Metrics
# MAGIC
# MAGIC Builds a small star schema on the DataExpert tweet CSVs:
# MAGIC 1. **Fact** — partitioned Delta table from `/Volumes/tabular/dataexpert/tweets`
# MAGIC 2. **Dimension** — one row per user, ranked by tweet volume
# MAGIC 3. **Aggregate** — platform metrics via an explicit **broadcast join** of dim → fact

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.window import Window
import os

DATAEXPERT_HOST = os.environ.get("DATABRICKS_HOST", "https://your-workspace.cloud.databricks.com")
DATAEXPERT_PROFILE = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
DATAEXPERT_CLUSTER_ID = os.environ.get("DATABRICKS_CLUSTER_ID", "your-cluster-id")

TWEETS_PATH = os.environ.get("TWEETS_PATH", "/Volumes/tabular/dataexpert/tweets/*.csv")
TARGET_SCHEMA = os.environ.get("TARGET_SCHEMA", "your_catalog.your_schema")
FACT_TABLE = f"{TARGET_SCHEMA}.tweets_fact"
DIM_TABLE = f"{TARGET_SCHEMA}.tweets_user_dim"
METRICS_TABLE = f"{TARGET_SCHEMA}.tweets_platform_metrics"
LANGUAGE_TABLE = f"{TARGET_SCHEMA}.tweets_language_stats"
HASHTAG_TABLE = f"{TARGET_SCHEMA}.tweets_hashtag_stats"
ENGAGEMENT_TABLE = f"{TARGET_SCHEMA}.tweets_engagement_stats"

BIG_ACCOUNT_FOLLOWERS = 100_000
TOP_N_TWEETERS = 10

# Rough global population share by ISO language code (for amplification index)
POPULATION_SHARE = {
    "en": 16.0, "zh": 14.0, "hi": 8.0, "es": 7.0, "ar": 5.0, "bn": 4.0,
    "pt": 4.0, "ru": 2.5, "ja": 2.0, "pa": 1.5, "de": 1.5, "jv": 1.5,
    "ko": 1.2, "fr": 1.2, "te": 1.0, "mr": 1.0, "tr": 1.0, "ta": 1.0,
    "vi": 1.0, "ur": 1.0, "it": 0.9, "th": 0.9, "gu": 0.8, "pl": 0.5,
    "uk": 0.4, "nl": 0.3, "ro": 0.3, "el": 0.2, "cs": 0.2, "sv": 0.2,
    "hu": 0.2, "he": 0.2, "fi": 0.1, "no": 0.1, "da": 0.1, "ca": 0.1,
}


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

    return (
        DatabricksSession.builder
        .host(DATAEXPERT_HOST)
        .clusterId(DATAEXPERT_CLUSTER_ID)
        .profile(DATAEXPERT_PROFILE)
        .getOrCreate()
    )


spark = get_spark()
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ingest Raw Tweets

# COMMAND ----------

def load_tweets(path):
    """Read Ukraine tweet CSVs. Older daily files have 18 cols; newer ones have 29.

    When all files are read with inferSchema, Spark keeps the 18-col schema.
    In 29-col files column 18 is is_retweet (true/false), not extractedts — so
    extractedts becomes boolean for ~30M rows. We ignore extractedts and use
    tweetcreatedts + tweet_id snowflake timestamp instead.
    """
    raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
        .option("wholeFile", True)
        .option("escape", '"')
        .csv(path)
    )

    available = set(raw.columns)

    def col(name, dtype=None):
        if name in available:
            return F.col(name).cast(dtype) if dtype else F.col(name)
        return F.lit(None).cast(dtype or "string")

    twitter_epoch_ms = 1288834974657

    base = (
        raw
        .select(
            col("tweetid", "long").alias("tweet_id"),
            col("username"),
            col("text"),
            col("followers", "long").alias("followers_count"),
            col("retweetcount", "long").alias("retweet_count"),
            col("favorite_count", "long").alias("favorite_count"),
            col("language"),
            col("hashtags"),
            col("tweetcreatedts", "timestamp").alias("tweetcreatedts"),
        )
        .filter(
            F.col("tweet_id").isNotNull()
            & F.col("username").isNotNull()
            & F.col("text").isNotNull()
        )
    )

    tweet_ts_from_id = F.to_timestamp(
        (F.shiftright(F.col("tweet_id").cast("long"), 22) + F.lit(twitter_epoch_ms)) / F.lit(1000)
    )

    is_retweet = (
        F.coalesce(col("is_retweet", "boolean"), F.lit(False))
        if "is_retweet" in available
        else F.col("text").startswith("RT @")
    )
    is_quote_status = (
        col("is_quote_status", "boolean")
        if "is_quote_status" in available
        else F.lit(False)
    )
    in_reply_to_status_id = col("in_reply_to_status_id", "long")
    in_reply_to_screen_name = col("in_reply_to_screen_name")

    is_reply = (
        (in_reply_to_status_id.isNotNull() & (in_reply_to_status_id > 0))
        if "in_reply_to_status_id" in available
        else F.col("text").rlike("^@[^\\s]+")
    )

    return (
        base
        .withColumn("is_retweet", is_retweet)
        .withColumn("is_quote_status", is_quote_status)
        .withColumn("in_reply_to_status_id", in_reply_to_status_id)
        .withColumn("in_reply_to_screen_name", in_reply_to_screen_name)
        .withColumn("tweet_ts", F.coalesce(tweet_ts_from_id, F.col("tweetcreatedts")))
        .withColumn("tweet_date", F.to_date("tweet_ts"))
        .withColumn("tweet_year_month", F.date_format("tweet_ts", "yyyy-MM"))
        .withColumn("is_reply", is_reply)
        .withColumn(
            "is_original",
            (~F.coalesce(F.col("is_retweet"), F.lit(False))) & (~F.col("is_reply")),
        )
        .drop("tweetcreatedts")
    )

raw_tweets = load_tweets(TWEETS_PATH)
print(f"Loaded {raw_tweets.count():,} tweet rows")
raw_tweets.printSchema()
raw_tweets.show(3, truncate=80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Write Partitioned Fact Delta Table
# MAGIC
# MAGIC Partition by `tweet_date` so time-range scans skip irrelevant files.

# COMMAND ----------

def write_fact_table(df, table_name):
    """Overwrite partitioned fact Delta table."""
    (
        df
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("tweet_date")
        .saveAsTable(table_name)
    )
    fact = spark.table(table_name)
    print(f"✅ {table_name}: {fact.count():,} rows, {fact.select('tweet_date').distinct().count()} partition days")
    fact.printSchema()
    return fact

tweets_fact = write_fact_table(raw_tweets, FACT_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build User Dimension (Ranked by Tweet Volume)
# MAGIC
# MAGIC Small table — one row per user with rank, volume tier, and account-size flags.

# COMMAND ----------

def build_user_dimension(fact_df):
    """Aggregate per-user stats and rank by tweet volume (dense rank, ties share rank)."""
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

    ranked = (
        user_stats
        .withColumn("volume_rank", F.dense_rank().over(Window.orderBy(F.desc("tweet_count"))))
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

    return ranked

user_dim = build_user_dimension(tweets_fact)

(
    user_dim
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(DIM_TABLE)
)

user_dim = spark.table(DIM_TABLE)
print(f"✅ {DIM_TABLE}: {user_dim.count():,} users")
user_dim.orderBy("volume_rank").show(15, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Broadcast Join → Platform Metrics
# MAGIC
# MAGIC Force a broadcast join so Spark copies the small dimension to every executor
# MAGIC instead of shuffling the large fact table. Look for `BroadcastHashJoin` in the plan.

# COMMAND ----------

def build_platform_metrics(fact_df, dim_df):
    """Join fact to broadcast dimension and compute platform-level concentration metrics."""
    # Disable auto-broadcast so the explicit hint is visible in the physical plan
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    enriched = (
        fact_df.alias("f")
        .join(
            broadcast(dim_df.alias("d")),
            on="username",
            how="left",
        )
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
            F.col("f.is_retweet"),
            F.col("f.is_reply"),
            F.col("f.is_quote_status"),
            F.col("f.is_original"),
        )
    )

    print("📋 Physical plan — expect BroadcastHashJoin (no Exchange on fact side):")
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
    )

    metrics = totals.select(
        "total_tweets",
        "total_users",
        F.round(100.0 * F.col("tweets_from_top_10") / F.col("total_tweets"), 2).alias("pct_top_10_tweeters"),
        F.round(100.0 * F.col("tweets_from_top_100") / F.col("total_tweets"), 2).alias("pct_top_100_tweeters"),
        F.round(100.0 * F.col("tweets_from_big_accounts") / F.col("total_tweets"), 2).alias("pct_big_accounts_100k_plus"),
        F.round(F.col("total_retweets") / F.col("total_tweets"), 2).alias("avg_retweets_per_tweet"),
        F.round(F.col("total_favorites") / F.col("total_tweets"), 2).alias("avg_favorites_per_tweet"),
        F.round(F.col("avg_followers_per_tweet"), 0).alias("avg_followers_per_tweet"),
        F.round(F.col("avg_followers_top_10"), 0).alias("avg_followers_top_10_tweeters"),
        F.round(F.col("avg_followers_not_top_10"), 0).alias("avg_followers_non_top_10"),
        F.current_timestamp().alias("computed_at"),
    )

    return enriched, metrics

enriched_tweets, platform_metrics = build_platform_metrics(tweets_fact, user_dim)

(
    platform_metrics
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(METRICS_TABLE)
)

print(f"✅ {METRICS_TABLE} written")
spark.table(METRICS_TABLE).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Additional Breakdowns

# COMMAND ----------

def show_volume_tier_breakdown(enriched_df):
    """Tweet share and engagement by user volume tier."""
    (
        enriched_df
        .groupBy("volume_tier")
        .agg(
            F.count("*").alias("tweet_count"),
            F.countDistinct("username").alias("user_count"),
            F.round(F.avg("followers_count"), 0).alias("avg_followers"),
            F.round(F.avg("retweet_count"), 2).alias("avg_retweets"),
            F.round(F.avg("favorite_count"), 2).alias("avg_favorites"),
        )
        .withColumn(
            "pct_of_all_tweets",
            F.round(100.0 * F.col("tweet_count") / enriched_df.count(), 2),
        )
        .orderBy(F.desc("tweet_count"))
        .show(truncate=False)
    )

def show_monthly_concentration(enriched_df):
    """How concentrated tweet volume is month-over-month."""
    monthly = (
        enriched_df
        .groupBy("tweet_year_month")
        .agg(
            F.count("*").alias("total_tweets"),
            F.sum(F.when(F.col("is_top_10_tweeter"), 1).otherwise(0)).alias("top_10_tweets"),
            F.sum(F.when(F.col("is_big_account"), 1).otherwise(0)).alias("big_account_tweets"),
        )
        .withColumn("pct_top_10", F.round(100.0 * F.col("top_10_tweets") / F.col("total_tweets"), 2))
        .withColumn("pct_big_accounts", F.round(100.0 * F.col("big_account_tweets") / F.col("total_tweets"), 2))
        .orderBy("tweet_year_month")
    )
    monthly.show(20, truncate=False)
    return monthly

def show_top_tweeters(dim_df, n=10):
    """Leaderboard of highest-volume accounts."""
    (
        dim_df
        .orderBy("volume_rank")
        .limit(n)
        .select(
            "volume_rank",
            "username",
            "tweet_count",
            "followers_count",
            "is_big_account",
            F.round(F.col("total_favorites") / F.col("tweet_count"), 1).alias("avg_favorites_per_tweet"),
        )
        .show(truncate=False)
    )

show_top_tweeters(user_dim)
show_volume_tier_breakdown(enriched_tweets)
monthly_metrics = show_monthly_concentration(enriched_tweets)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Gini-Style Concentration 
# MAGIC
# MAGIC Measures how unevenly tweets are distributed across users (0 = perfectly even, 1 = one user posts everything).

# COMMAND ----------

def tweet_concentration_index(fact_df):
    """Simple HHI-style concentration: sum of squared user tweet shares."""
    user_shares = (
        fact_df
        .groupBy("username")
        .agg(F.count("*").alias("tweet_count"))
        .withColumn("share", F.col("tweet_count") / fact_df.count())
        .agg(F.sum(F.pow("share", 2)).alias("concentration_index"))
    )
    user_shares.show(truncate=False)

tweet_concentration_index(tweets_fact)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Content Analytics — Language, Hashtags & Engagement
# MAGIC
# MAGIC Uses extra CSV columns: `language`, `hashtags`, `is_retweet`, `is_quote_status`, reply fields.

# COMMAND ----------

def build_language_stats(fact_df):
    """Tweet volume by language vs rough global population share (amplification index)."""
    total = fact_df.count()
    pop_map = F.create_map(*[x for kv in POPULATION_SHARE.items() for x in (F.lit(kv[0]), F.lit(kv[1]))])

    return (
        fact_df
        .withColumn("language", F.coalesce(F.lower(F.trim("language")), F.lit("unknown")))
        .groupBy("language")
        .agg(
            F.count("*").alias("tweet_count"),
            F.countDistinct("username").alias("distinct_users"),
            F.round(F.avg("retweet_count"), 2).alias("avg_retweets"),
            F.round(F.avg("followers_count"), 0).alias("avg_followers"),
            F.sum(F.when(F.col("is_retweet"), 1).otherwise(0)).alias("retweet_posts"),
            F.sum(F.when(F.col("is_reply"), 1).otherwise(0)).alias("reply_posts"),
        )
        .withColumn("pct_of_tweets", F.round(100.0 * F.col("tweet_count") / total, 2))
        .withColumn("tweets_per_user", F.round(F.col("tweet_count") / F.col("distinct_users"), 2))
        .withColumn("population_share_pct", F.coalesce(pop_map[F.col("language")], F.lit(0.1)))
        .withColumn(
            "amplification_index",
            F.round(F.col("pct_of_tweets") / F.col("population_share_pct"), 2),
        )
        .withColumn("computed_at", F.current_timestamp())
        .orderBy(F.desc("tweet_count"))
    )


def build_hashtag_stats(fact_df, top_n=50):
    """Explode hashtag array strings and rank by tweet volume."""
    total = fact_df.count()
    hashtag_rows = (
        fact_df
        .filter(F.col("hashtags").isNotNull())
        .select(
            "tweet_id",
            "username",
            "retweet_count",
            F.explode(
                F.regexp_extract_all(
                    F.lower("hashtags"),
                    F.lit(r"'text': '([^']+)'"),
                    F.lit(1),
                )
            ).alias("hashtag"),
        )
        .filter(F.col("hashtag").isNotNull() & (F.length("hashtag") > 0))
    )

    return (
        hashtag_rows
        .groupBy("hashtag")
        .agg(
            F.count("*").alias("tweet_count"),
            F.countDistinct("username").alias("distinct_users"),
            F.round(F.avg("retweet_count"), 2).alias("avg_retweets"),
        )
        .withColumn("pct_of_all_tweets", F.round(100.0 * F.col("tweet_count") / total, 4))
        .withColumn("computed_at", F.current_timestamp())
        .orderBy(F.desc("tweet_count"))
        .limit(top_n)
    )


def build_engagement_stats(fact_df):
    """Share of originals, retweets, replies, and quote tweets."""
    total = fact_df.count()
    counts = fact_df.agg(
        F.sum(F.when(F.col("is_original"), 1).otherwise(0)).alias("original_tweets"),
        F.sum(F.when(F.col("is_retweet"), 1).otherwise(0)).alias("retweet_tweets"),
        F.sum(F.when(F.col("is_reply"), 1).otherwise(0)).alias("reply_tweets"),
        F.sum(F.when(F.col("is_quote_status"), 1).otherwise(0)).alias("quote_tweets"),
        F.sum(F.when(F.col("language").isNull(), 1).otherwise(0)).alias("unknown_language_tweets"),
    )

    return counts.select(
        F.lit(total).alias("total_tweets"),
        "original_tweets",
        "retweet_tweets",
        "reply_tweets",
        "quote_tweets",
        "unknown_language_tweets",
        F.round(100.0 * F.col("original_tweets") / total, 2).alias("pct_original"),
        F.round(100.0 * F.col("retweet_tweets") / total, 2).alias("pct_retweet"),
        F.round(100.0 * F.col("reply_tweets") / total, 2).alias("pct_reply"),
        F.round(100.0 * F.col("quote_tweets") / total, 2).alias("pct_quote"),
        F.current_timestamp().alias("computed_at"),
    )


language_stats = build_language_stats(tweets_fact)
hashtag_stats = build_hashtag_stats(tweets_fact)
engagement_stats = build_engagement_stats(tweets_fact)

for df, table in [
    (language_stats, LANGUAGE_TABLE),
    (hashtag_stats, HASHTAG_TABLE),
    (engagement_stats, ENGAGEMENT_TABLE),
]:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    print(f"✅ {table}")
    spark.table(table).show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Interpretation cheat sheet
# MAGIC
# MAGIC | Metric | Meaning |
# MAGIC |--------|---------|
# MAGIC | `pct_of_tweets` | Language/hashtag share of all tweets |
# MAGIC | `tweets_per_user` | Avg tweets per distinct user in that language |
# MAGIC | `amplification_index` | Tweet share ÷ population share (>1 = over-represented on Twitter vs world population) |
# MAGIC | `pct_retweet` / `pct_reply` | How conversational vs broadcast the dataset is |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC | Table | Purpose |
# MAGIC |-------|---------|
# MAGIC | `bootcamp_students.alperendavran.tweets_fact` | Partitioned fact table (`tweet_date`) |
# MAGIC | `bootcamp_students.alperendavran.tweets_user_dim` | User dimension with `volume_rank` |
# MAGIC | `bootcamp_students.alperendavran.tweets_platform_metrics` | Single-row platform KPIs from broadcast join |
# MAGIC | `bootcamp_students.alperendavran.tweets_language_stats` | Language volume vs population amplification |
# MAGIC | `bootcamp_students.alperendavran.tweets_hashtag_stats` | Top hashtags by tweet count |
# MAGIC | `bootcamp_students.alperendavran.tweets_engagement_stats` | Original / retweet / reply / quote shares |
# MAGIC
# MAGIC **Key metrics in `tweets_platform_metrics`:**
# MAGIC - `pct_top_10_tweeters` — share of all tweets from the 10 most prolific users
# MAGIC - `pct_big_accounts_100k_plus` — share from accounts with ≥ 100k followers
# MAGIC - `pct_top_100_tweeters`, engagement averages, follower comparisons
# MAGIC
# MAGIC **Content analytics:**
# MAGIC - `amplification_index` — e.g. Ukrainian (`uk`) often >> 1 in this war dataset
# MAGIC - `tweets_per_user` — which languages have power users vs casual posters
# MAGIC - Top hashtags — `#Ukraine`, `#Russia`, etc.

# COMMAND ----------
