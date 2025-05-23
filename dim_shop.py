from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# Create SparkSession
spark = (
    SparkSession.builder.appName("YourAppName")
    .config("spark.master", "local[*]")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)

# Set log level (optional - reduces verbose output)
spark.sparkContext.setLogLevel("WARN")


def main():

    df = spark.read.option("header", "true").csv("/Users/sanjay/Downloads/sample.csv")

    df = df.withColumnsRenamed(
        {"SHOP_ID": "shop_id", "DATE": "curr_trans_date", "N_TRANS": "n_trans"}
    )

    df = df.withColumn("shop_id", F.col("shop_id").cast("int"))

    windowSpec = Window.partitionBy("shop_id").orderBy("curr_trans_date")
    df_prev_trans_date = df.withColumn(
        "prev_trans_date", F.lag(F.col("curr_trans_date")).over(windowSpec)
    )
    df_days_since_prev_trans = df_prev_trans_date.withColumn(
        "days_since_last_trans",
        (F.datediff(F.col("curr_trans_date"), F.col("prev_trans_date"))),
    ).withColumn("status", F.lit("open"))

    df_days_since_prev_trans = df_days_since_prev_trans.drop("n_trans")

    df_closed_dates = df_days_since_prev_trans.filter(
        "days_since_last_trans >= 30"
    ).select(
        F.col("shop_id"),
        (F.date_add(F.col("prev_trans_date"), 1)).alias(
            "curr_trans_date"
        ),  # date the closed period began
        (F.date_sub(F.col("curr_trans_date"), 1)).alias(
            "prev_trans_date"
        ),  # date the closed period ended
        F.lit("closed").alias("status"),
        F.lit(0).alias("days_since_last_trans"),
    )

    df_open_or_closed = df_closed_dates.unionByName(df_days_since_prev_trans)

    df_status_change = df_open_or_closed.withColumn(
        "prev_status", F.lag(F.col("status")).over(windowSpec)
    )

    df_status_change_indicated = df_status_change.withColumn(
        "status_change_streak",
        F.sum(F.when((F.col("status") != F.col("prev_status")), 1).otherwise(0)).over(
            windowSpec
        ),
    )

    streakWindow = Window.partitionBy("shop_id")
    df_max_status_change_indicated = df_status_change_indicated.withColumn(
        "max_status_change_streak",
        F.max(F.col("status_change_streak")).over(streakWindow),
    )

    df_lead_trans_date = df_max_status_change_indicated.withColumn(
        "next_trans_date", F.lead("curr_trans_date").over(windowSpec)
    )

    df_valid_from = df_lead_trans_date.groupBy(
        "shop_id", "status", "status_change_streak", "max_status_change_streak"
    ).agg(
        F.min(F.col("curr_trans_date")).alias("valid_from"),
        F.date_sub(F.max(F.col("next_trans_date")), 1).alias("valid_to_temp"),
    )

    df_dim_shop = df_valid_from.withColumn(
        "valid_to",
        F.when(
            (F.col("status_change_streak") == F.col("max_status_change_streak")),
            F.to_date(F.lit("9999-12-31")),
        ).otherwise((F.col("valid_to_temp"))),
    ).drop("valid_to_temp", "status_change_streak", "max_status_change_streak")

    ## At this stage, dim_shop can be written to a table and is a complete SCD 2 table
    df_dim_shop.orderBy("shop_id", "valid_from").show(200)

    ## Answer to a)

    df_dim_shop.filter("status = 'closed'").select(
        F.count_distinct("shop_id").alias("shops_that_had_closed_periods")
    ).show()

    ## Answer to b)

    quarters_data = [
        ("Q1 2021", "2021-01-01"),
        ("Q2 2021", "2021-04-01"),
        ("Q3 2021", "2021-07-01"),
        ("Q4 2021", "2021-10-01"),
        ("Q1 2022", "2022-01-01"),
        ("Q2 2022", "2022-04-01"),
        ("Q3 2022", "2022-07-01"),
        ("Q4 2022", "2022-10-01"),
    ]

    quarters_df = spark.createDataFrame(quarters_data, ["quarter", "q_start"])

    quarters_df = quarters_df.withColumn(
        "q_start", F.to_date(quarters_df.q_start, "yyyy-MM-dd")
    )

    s = df_dim_shop.alias("s")
    q = quarters_df.alias("q")

    df_status_at_quarter_start = s.join(
        q, (q.q_start >= s.valid_from) & (q.q_start <= s.valid_to), how="inner"
    )

    df_stores_open_at_quarter_start = df_status_at_quarter_start.filter(
        "status = 'open'"
    )

    df_stores_open_at_quarter_start.groupBy("q_start").agg(
        F.count("*").alias("num_shops_open")
    ).show()


if __name__ == "__main__":

    main()
