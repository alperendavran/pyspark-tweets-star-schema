# Spark UI screenshots

Drop your Databricks Spark UI captures here so they render in the README.

| File | What to capture |
|------|-----------------|
| `broadcast-hash-join.png` | SQL / DataFrame **Physical Plan** showing `BroadcastHashJoin` |
| `fact-table-partitions.png` | Delta table details — partition columns (`tweet_date`) |
| `job-stages.png` | Spark UI **Stages** tab for the platform-metrics job |
| `cluster-metrics.gif` | Optional: short screen recording of job progress |

**How to capture (Databricks):**
1. Run section 4 (`build_platform_metrics`) in the notebook.
2. Open the cell output **Physical Plan** or the job link in the cluster Spark UI.
3. Screenshot the `BroadcastHashJoin LeftOuter BuildRight` node.
4. Save as `broadcast-hash-join.png` in this folder.
