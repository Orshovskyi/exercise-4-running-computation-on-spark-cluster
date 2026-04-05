"""
Обробка даних Divvy Trips (Q4 2019) за допомогою PySpark.
Результати зберігаються у CSV у підкаталогах out/.
"""
from __future__ import annotations

import os
import shutil

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    avg,
    col,
    count,
    date_format,
    date_sub,
    dense_rank,
    desc,
    lit,
    max as spark_max,
    regexp_replace,
    row_number,
    to_date,
)
from pyspark.sql.types import DateType, IntegerType, LongType, StringType, StructField, StructType


def _out_base() -> str:
    docker_out = "/opt/bitnami/spark/out"
    if os.path.isdir(docker_out):
        return docker_out
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "out"))


def _write_csv(df: DataFrame, subdir: str) -> None:
    base = _out_base()
    path = os.path.join(base, subdir)
    if os.path.isdir(path):
        shutil.rmtree(path)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(path)


def load_trips(spark: SparkSession, csv_path: str) -> DataFrame:
    raw = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .csv(csv_path)
    )
    duration = regexp_replace(col("tripduration"), ",", "").cast("double")
    return (
        raw.withColumn("duration_sec", duration)
        .withColumn("trip_date", to_date(col("start_time"), "yyyy-MM-dd HH:mm:ss"))
        .filter(col("trip_date").isNotNull() & col("duration_sec").isNotNull())
    )


def avg_duration_per_day(df: DataFrame) -> DataFrame:
    """a. Середня тривалість поїздки (сек) на день."""
    return (
        df.groupBy("trip_date")
        .agg(avg("duration_sec").alias("avg_duration_sec"))
        .orderBy("trip_date")
    )


def trips_count_per_day(df: DataFrame) -> DataFrame:
    """b. Кількість поїздок за кожен день."""
    return (
        df.groupBy("trip_date")
        .agg(count("*").alias("trips_count"))
        .orderBy("trip_date")
    )


def popular_start_station_per_month(df: DataFrame) -> DataFrame:
    """c. Найпопулярніша початкова станція за кожен місяць."""
    with_period = df.withColumn(
        "year_month",
        date_format(col("trip_date"), "yyyy-MM"),
    )
    counts = with_period.groupBy("year_month", "from_station_name").agg(
        count("*").alias("trip_starts")
    )
    w = Window.partitionBy("year_month").orderBy(desc("trip_starts"))
    ranked = counts.withColumn("rank", dense_rank().over(w))
    return (
        ranked.filter(col("rank") == 1)
        .select("year_month", "from_station_name", "trip_starts")
        .orderBy("year_month")
    )


def top3_start_stations_last_two_weeks(df: DataFrame) -> DataFrame:
    """
    d. Трійка лідерів початкових станцій за кількістю поїздок для кожного дня
    за останні 14 календарних днів у наборі даних (включно з останнім днём).
    """
    max_row = df.select(spark_max("trip_date").alias("max_d")).first()
    if max_row is None or max_row["max_d"] is None:
        empty_schema = StructType(
            [
                StructField("trip_date", DateType(), True),
                StructField("rank", IntegerType(), True),
                StructField("from_station_name", StringType(), True),
                StructField("trips_count", LongType(), True),
            ]
        )
        return df.sparkSession.createDataFrame([], empty_schema)
    max_d = max_row["max_d"]
    start_d = date_sub(lit(max_d), 13)
    recent = df.filter(col("trip_date") >= start_d)
    daily = recent.groupBy("trip_date", "from_station_name").agg(
        count("*").alias("trips_count")
    )
    w = Window.partitionBy("trip_date").orderBy(desc("trips_count"), col("from_station_name"))
    return (
        daily.withColumn("rank", row_number().over(w))
        .filter(col("rank") <= 3)
        .select("trip_date", "rank", "from_station_name", "trips_count")
        .orderBy("trip_date", "rank")
    )


def gender_avg_duration(df: DataFrame) -> DataFrame:
    """e. Середня тривалість по статі (лише Male / Female)."""
    g = df.filter(col("gender").isin("Male", "Female"))
    return (
        g.groupBy("gender")
        .agg(avg("duration_sec").alias("avg_duration_sec"))
        .orderBy("gender")
    )


def main() -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_csv = os.path.join(script_dir, "Divvy_Trips_2019_Q4.csv")
    csv_path = os.environ.get("DIVVY_CSV", default_csv)

    spark = SparkSession.builder.appName("DivvyTripsLab4").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = load_trips(spark, csv_path)

    _write_csv(avg_duration_per_day(df), "avg_duration_per_day")
    _write_csv(trips_count_per_day(df), "trips_per_day")
    _write_csv(popular_start_station_per_month(df), "popular_start_station_per_month")
    _write_csv(top3_start_stations_last_two_weeks(df), "top3_stations_last_two_weeks")
    _write_csv(gender_avg_duration(df), "gender_avg_duration")

    spark.stop()


if __name__ == "__main__":
    main()
