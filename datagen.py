#!/usr/bin/env python3
"""
datagen.py - generate synthetic listening_logs.csv and songs_metadata.csv
Usage:
    python3 datagen.py --out inputs/
"""
import csv, random, argparse
from datetime import datetime, timedelta

def generate(out_dir="inputs", num_songs=80, num_users=120, num_listens=600):
    random.seed(42)
    genres = ["Pop","Rock","Jazz","Hip-Hop","Electronic","Classical","Country","R&B"]
    moods = ["Happy","Sad","Energetic","Chill","Romantic","Angry"]
    songs = []
    for i in range(1, num_songs+1):
        songs.append({
            "song_id": f"S{i:04d}",
            "title": f"Song {i}",
            "artist": f"Artist {random.randint(1,30)}",
            "genre": random.choice(genres),
            "mood": random.choice(moods)
        })
    os.makedirs(out_dir, exist_ok=True)
    with open(out_dir+"/songs_metadata.csv","w",newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["song_id","title","artist","genre","mood"])
        writer.writeheader()
        writer.writerows(songs)

    start = datetime(2025,1,1)
    end = datetime(2025,9,1,23,59,59)
    total_seconds = int((end-start).total_seconds())
    users = [f"U{u:04d}" for u in range(1,num_users+1)]
    listens = []
    for _ in range(num_listens):
        user = random.choice(users)
        song = random.choice(songs)["song_id"]
        ts = start + timedelta(seconds=random.randint(0,total_seconds))
        duration = max(10, min(400, int(random.expovariate(1/120))))
        listens.append({"user_id":user,"song_id":song,"timestamp":ts.strftime("%Y-%m-%d %H:%M:%S"),"duration_sec":duration})
    with open(out_dir+"/listening_logs.csv","w",newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["user_id","song_id","timestamp","duration_sec"])
        writer.writeheader()
        writer.writerows(listens)

if __name__ == "__main__":
    import os, argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="inputs")
    args = parser.parse_args()
    generate(out_dir=args.out)
