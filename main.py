#!/usr/bin/env python3
"""
main.py - Spark Structured API solution for the assignment.

Writes outputs/ folder with subfolders:
- user_favorite_genres
- avg_listen_time_per_song
- genre_loyalty_scores
- night_owl_users

Usage:
    spark-submit main.py --listening inputs/listening_logs.csv --songs inputs/songs_metadata.csv --out outputs/
"""
import argparse
from pyspark.sql import SparkSession, functions as F, Window
def run(listening, songs, out):
    spark = SparkSession.builder.appName("H6_Music_Analysis").getOrCreate()
    ll = spark.read.option("header",True).option("inferSchema",True).csv(listening)
    sm = spark.read.option("header",True).option("inferSchema",True).csv(songs)
    merged = ll.join(sm, on="song_id", how="left")

    # 1. favorite genre per user
    ug = merged.groupBy("user_id","genre").count()
    w = Window.partitionBy("user_id").orderBy(F.desc("count"), F.asc("genre"))
    fav = ug.withColumn("rn", F.row_number().over(w)).filter(F.col("rn")==1).select("user_id","genre","count")
    fav.coalesce(1).write.mode("overwrite").option("header",True).csv(f"{out}/user_favorite_genres")

    # 2. avg listen time per song
    avg = ll.groupBy("song_id").agg(F.avg("duration_sec").alias("avg_duration_sec"))
    avg.join(sm.select("song_id","title","artist","genre"), on="song_id", how="left")\
       .coalesce(1).write.mode("overwrite").option("header",True).csv(f"{out}/avg_listen_time_per_song")

    # 3. genre loyalty
    total = merged.groupBy("user_id").count().withColumnRenamed("count","total_plays")
    fav2 = fav.withColumnRenamed("genre","favorite_genre").withColumnRenamed("count","fav_genre_plays")
    loyalty = total.join(fav2, on="user_id", how="left").withColumn("genre_loyalty_score", F.col("fav_genre_plays")/F.col("total_plays"))
    loyalty.coalesce(1).write.mode("overwrite").option("header",True).csv(f"{out}/genre_loyalty_scores")
    loyalty.filter(F.col("genre_loyalty_score")>0.8).coalesce(1).write.mode("overwrite").option("header",True).csv(f"{out}/genre_loyalty_scores_above_0.8")

    # 4. night owls
    merged2 = merged.withColumn("hour", F.hour(F.col("timestamp").cast("timestamp")))
    night = merged2.filter((F.col("hour")>=0)&(F.col("hour")<=5))
    night_counts = night.groupBy("user_id").count().withColumnRenamed("count","night_plays")
    night_with_tot = night_counts.join(total, on="user_id", how="left").withColumn("night_proportion", F.col("night_plays")/F.col("total_plays"))
    night_with_tot.filter((F.col("night_plays")>=5)|(F.col("night_proportion")>=0.2))\
                  .coalesce(1).write.mode("overwrite").option("header",True).csv(f"{out}/night_owl_users")

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--listening", required=True)
    parser.add_argument("--songs", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    run(args.listening, args.songs, args.out)
