# Music Streaming Analysis Using Spark Structured APIs

## Overview
This project analyzes user listening behavior and music trends on a fictional music streaming platform using **Spark Structured APIs**. The analysis provides insights into:
- User favorite genres
- Average listen time per song
- Genre loyalty scores
- Night owl users (active between 12 AM and 5 AM)

---

## Dataset Description
The project uses two input datasets generated via `datagen.py`:

1. **listening_logs.csv**
   - `user_id` – Unique ID of the user  
   - `song_id` – Unique ID of the song  
   - `timestamp` – Date and time of play (e.g., 2025-03-23 14:05:00)  
   - `duration_sec` – Duration (in seconds) the song was played  

2. **songs_metadata.csv**
   - `song_id` – Unique ID of the song  
   - `title` – Title of the song  
   - `artist` – Artist of the song  
   - `genre` – Genre (Pop, Rock, Jazz, etc.)  
   - `mood` – Mood (Happy, Sad, Energetic, Chill, etc.)  

---

## Repository Structure
```
hands_on_6/
│── datagen.py              # Script to generate synthetic input CSVs
│── main.py                 # Spark Structured API solution
│── inputs/                 # Input datasets (CSV)
│   ├── listening_logs.csv
│   └── songs_metadata.csv
│── outputs/                # Output results (CSV folders)
│── README.md               # Project documentation
│── summary.json            # Metadata about dataset generation
```

---

## Output Directory Structure
The Spark jobs generate results into `outputs/` with the following subfolders:
```
outputs/
│── user_favorite_genres/
│── avg_listen_time_per_song/
│── genre_loyalty_scores/
│── night_owl_users/
```

---

## Tasks and Outputs

### 1. Find each user’s favorite genre  
**Output folder**: `outputs/user_favorite_genres/`  

Preview:
| user_id   | genre      |   plays |
|:----------|:-----------|--------:|
| U0001     | R&B        |       2 |
| U0002     | Electronic |       1 |
| U0003     | Classical  |       2 |
| U0004     | R&B        |       3 |
| U0005     | Classical  |       2 |

---

### 2. Calculate the average listen time per song  
**Output folder**: `outputs/avg_listen_time_per_song/`  

Preview:
| song_id   |   avg_duration_sec | title   | artist    | genre      |
|:----------|-------------------:|:--------|:----------|:-----------|
| S0001     |            125.167 | Song 1  | Artist 21 | Rock       |
| S0002     |            147.75  | Song 2  | Artist 24 | Electronic |
| S0003     |            257.667 | Song 3  | Artist 8  | Jazz       |
| S0004     |            165.875 | Song 4  | Artist 4  | Rock       |
| S0005     |            135.333 | Song 5  | Artist 14 | Pop        |

---

### 3. Compute genre loyalty score for each user  
**Output folder**: `outputs/genre_loyalty_scores/`  

Preview:
| user_id   |   total_plays | favorite_genre   |   fav_genre_plays |   genre_loyalty_score |
|:----------|--------------:|:-----------------|------------------:|----------------------:|
| U0001     |             4 | R&B              |                 2 |              0.5      |
| U0002     |             4 | Electronic       |                 1 |              0.25     |
| U0003     |             5 | Classical        |                 2 |              0.4      |
| U0004     |             6 | R&B              |                 3 |              0.5      |
| U0005     |             9 | Classical        |                 2 |              0.222222 |

---

### 4. Identify night owl users (12 AM – 5 AM)  
**Output folder**: `outputs/night_owl_users/`  

Preview:
| user_id   |   night_plays |   total_plays |   night_proportion |
|:----------|--------------:|--------------:|-------------------:|
| U0005     |             4 |             9 |           0.444444 |
| U0058     |             4 |             9 |           0.444444 |
| U0029     |             4 |            10 |           0.4      |
| U0040     |             3 |             5 |           0.6      |
| U0052     |             3 |             5 |           0.6      |

---

## Execution Instructions

### Prerequisites
Before starting, ensure you have the following installed:

- **Python 3.x**
  ```bash
  python3 --version
  ```
- **PySpark**
  ```bash
  pip install pyspark
  ```
- **Apache Spark**  
  [Download here](https://spark.apache.org/downloads.html)  
  Verify installation:
  ```bash
  spark-submit --version
  ```

---

### 1. Running Locally

#### Generate the Input
```bash
python3 datagen.py --out inputs/
```

#### Execute All Tasks
```bash
spark-submit main.py --listening inputs/listening_logs.csv --songs inputs/songs_metadata.csv --out outputs/
```

#### Verify the Outputs
```bash
ls outputs/
```

---

## Errors and Resolutions
- **PySpark not found** → Run `pip install pyspark`  
- **Permission denied** → Ensure you have write permissions to `outputs/` directory  
- **Java errors** → Spark requires Java; ensure Java 8 or later is installed and configured  

---
