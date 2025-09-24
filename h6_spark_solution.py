"""
PySpark solution for H6 - Spark Structured APIs Music Listener Behaviour Analysis

This script reads `listening_logs.csv` and `songs_metadata.csv` and produces outputs:
- user_favorite_genres
- avg_listen_time_per_song
- genre_loyalty_scores (and those above 0.8)
- night_owl_users

Usage:
    python h6_spark_solution.py --listening inputs/listening_logs.csv --songs inputs/songs_metadata.csv --out output/

"""
import argparse
from pyspark.sql import SparkSession, functions as F, Window

def main(listening_path, songs_path, out_dir):
    spark = SparkSession.builder.appName("H6_Music_Analysis").getOrCreate()
    ll = spark.read.option("header", True).option("inferSchema", True).csv(listening_path)
    sm = spark.read.option("header", True).option("inferSchema", True).csv(songs_path)

    # join
    merged = ll.join(sm, on="song_id", how="left")

    # 1) favorite genre per user
    user_genre_counts = merged.groupBy("user_id", "genre").count()
    window_user = Window.partitionBy("user_id").orderBy(F.desc("count"), F.asc("genre"))
    fav = user_genre_counts.withColumn("rn", F.row_number().over(window_user)).filter(F.col("rn")==1).select("user_id","genre","count")
    fav.coalesce(1).write.mode("overwrite").option("header",True).csv(f"{out_dir}/user_favorite_genres")

    # 2) avg listen time per song
    avg_listen = ll.groupBy("song_id").agg(F.avg("duration_sec").alias("avg_duration_sec"))
    avg_with_meta = avg_listen.join(sm.select("song_id","title","artist","genre"), on="song_id", how="left")
    avg_with_meta.coalesce(1).write.mode("overwrite").option("header",True).csv(f"{out_dir}/avg_listen_time_per_song")

    # 3) genre loyalty score
    total_plays = merged.groupBy("user_id").count().withColumnRenamed("count","total_plays")
    fav = fav.withColumnRenamed("genre","favorite_genre").withColumnRenamed("count","fav_genre_plays")
    loyalty = total_plays.join(fav, on="user_id", how="left").withColumn("genre_loyalty_score", F.col("fav_genre_plays") / F.col("total_plays"))
    loyalty.coalesce(1).write.mode("overwrite").option("header",True).csv(f"{out_dir}/genre_loyalty_scores")
    loyalty.filter(F.col("genre_loyalty_score") > 0.8).coalesce(1).write.mode("overwrite").option("header",True).csv(f"{out_dir}/genre_loyalty_scores_above_0.8")

    # 4) night owl users (00:00-05:00)
    merged = merged.withColumn("hour", F.hour(F.col("timestamp").cast("timestamp")))
    night = merged.filter((F.col("hour") >= 0) & (F.col("hour") <= 5))
    night_counts = night.groupBy("user_id").count().withColumnRenamed("count","night_plays")
    night_with_total = night_counts.join(total_plays, on="user_id", how="left").withColumn("night_proportion", F.col("night_plays") / F.col("total_plays"))
    # filter definition: night_plays >=5 or night_proportion >= 0.2
    night_with_total.filter((F.col("night_plays") >= 5) | (F.col("night_proportion") >= 0.2)).coalesce(1).write.mode("overwrite").option("header",True).csv(f"{out_dir}/night_owl_users")

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--listening", required=True)
    parser.add_argument("--songs", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    main(args.listening, args.songs, args.out)
