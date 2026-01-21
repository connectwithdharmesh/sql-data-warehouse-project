import os
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import (
    col, lit, trim, when, split, log10, avg, count, desc, explode, row_number, array, concat_ws
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType
from pyspark.sql.window import Window
import shutil  # For safe directory deletion in write_to_csv - why: Handles Windows file locks during overwrite.
import requests  # For optional download (not used in this script, but kept for potential future extensions) - why: Allows easy addition of data downloading if needed without major changes.


# Argparse setup: Parse command-line arguments to control pipeline behavior, such as which layer to run or if incremental mode is enabled - why: Enables flexible execution, e.g., running only gold layer or processing specific dates for efficiency.
parser = argparse.ArgumentParser(description="IMDB Data Pipeline V2 - Starting from Silver")
parser.add_argument("--layer", choices=["bronze", "silver", "gold", "all"], default="all", help="Layer to process")
parser.add_argument("--incremental", action="store_true", help="Run in incremental mode (append with dedup)")
parser.add_argument("--date", default=None, help="Process for specific date (YYYY-MM-DD)")
parser.add_argument("--no-date-filter", action="store_true", help="Load all data from Bronze, ignoring date filter")
args = parser.parse_args()

# Parse date: Convert provided date string to date object or default to current date for partitioning and filtering - why: Ensures data is timestamped correctly for incremental loads and historical tracking.
process_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else datetime.now().date()

# Initialize Spark: Create Spark session with configurations for parallelism, memory, and timeouts to optimize performance and prevent failures - why: Custom settings improve resource utilization and handle large datasets without timeouts or OOM errors.
spark = SparkSession.builder \
    .appName("IMDB Pipeline V2") \
    .config("spark.default.parallelism", "16") \
    .config("spark.sql.shuffle.partitions", "16") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.memory", "6g") \
    .config("spark.executor.heartbeatInterval", "30s") \
    .config("spark.network.timeout", "60s") \
    .getOrCreate()

# Paths: Define base directories for raw data and each layer (bronze, silver, gold) to organize data storage - why: Structured paths prevent clutter and make data management scalable across layers.
raw_path = "data/raw"
bronze_base = "data/bronze"
silver_base = "data/silver"
gold_base = "data/gold"
os.makedirs(raw_path, exist_ok=True)  # Create raw directory if it doesn't exist - why: Avoids errors if directories are missing during first run.
os.makedirs(bronze_base, exist_ok=True)  # Create bronze directory if it doesn't exist - why: Ensures bronze output path is ready.
os.makedirs(silver_base, exist_ok=True)  # Create silver directory if it doesn't exist - why: Ensures silver output path is ready.
os.makedirs(gold_base, exist_ok=True)  # Create gold directory if it doesn't exist - why: Ensures gold output path is ready.

# Schemas: Define StructTypes for each dataset to enforce data types during reading, ensuring consistency even if bronze is skipped - why: Prevents type inference issues and maintains data integrity from raw ingestion.
title_basics_schema = StructType([StructField(name, StringType(), True) for name in [
    "tconst", "titleType", "primaryTitle", "originalTitle", "isAdult", "startYear", "endYear", "runtimeMinutes", "genres"
]])
ratings_schema = StructType([StructField(name, StringType(), True) for name in ["tconst", "averageRating", "numVotes"]])
principals_schema = StructType([StructField(name, StringType(), True) for name in [
    "tconst", "ordering", "nconst", "category", "job", "characters"
]])
names_schema = StructType([StructField(name, StringType(), True) for name in [
    "nconst", "primaryName", "birthYear", "deathYear", "primaryProfession", "knownForTitles"
]])

# Helper: Read TSV.gz with progress: Load gzipped TSV file into DataFrame using schema, print progress, and optionally sample for testing - why: Provides visibility into loading process and allows quick testing with samples to speed up development.
def read_tsv_gz(path, schema):
    print(f"Progress: Reading file {path}...")
    df = spark.read.option("compression", "gzip").csv(path, sep="\t", header=True, schema=schema)
    # For testing: Uncomment to sample 5% data for faster runs
    # df = df.sample(0.05)
    row_count = df.count()  # Count rows to track data volume - why: Helps monitor data size for debugging and performance tuning.
    print(f"Progress: Read complete - {row_count} rows from {path}")
    return df

# Helper: Write to Parquet with progress: Write DataFrame to Parquet, partitioned by ingestion_date, in specified mode (append for incremental), with progress tracking - why: Parquet format is efficient for big data; partitioning aids querying by date.
def write_to_parquet(df, path, mode="overwrite"):
    if args.incremental:
        mode = "append"  # Switch to append mode if incremental flag is set - why: Allows adding new data without overwriting historical partitions.
    row_count = df.count()  # Count rows before writing - why: Verifies data presence before I/O operation.
    print(f"Progress: Writing {row_count} rows to {path} in mode {mode}...")
    if row_count > 0:
        df.write.mode(mode).partitionBy("ingestion_date").parquet(path)  # Write partitioned Parquet if data exists - why: Partitioning improves read performance for date-based queries.
    else:
        print("Warning: Skipping write - 0 rows")  # Skip if no data to avoid empty writes - why: Prevents creating unnecessary empty files.
    print(f"Progress: Write complete for {path}")

# Helper: Write to CSV with progress: Write DataFrame to CSV for easy viewing, in overwrite mode (since CSV doesn't support partitioning easily), with progress tracking - why: CSV is human-readable for quick inspection outside Spark.
def write_to_csv(df, path):
    row_count = df.count()  # Count rows before writing - why: Verifies data presence before I/O operation.
    print(f"Progress: Writing {row_count} rows to {path} as CSV...")
    # Manually delete existing directory to handle Windows locks - why: Spark's overwrite can fail on locked files; this ensures clean slate.
    if os.path.exists(path):
        shutil.rmtree(path, ignore_errors=True)  # Ignore errors if locked; retry if needed in production.
    if row_count > 0 or row_count == 0:  # Always write, even if empty - why: Ensures file creation for debugging.
        df.coalesce(1).write.mode("overwrite").csv(path, header=True, emptyValue='')  # Use emptyValue for nulls.
    else:
        print("Warning: Skipping CSV write - 0 rows")  # But still create empty if needed.
    print(f"Progress: CSV write complete for {path}")

# Bronze Layer: Ingestion: Load raw TSV.gz files, add ingestion_date, and write to Parquet (uncomment if rerunning bronze) - why: Bronze stores raw data as-is for auditing and reprocessing if needed.
def process_bronze():
    print("Progress: Starting Bronze layer (raw ingestion)...")
    datasets = {
        "title_basics": (f"{raw_path}/title.basics.tsv.gz", title_basics_schema),  # Title basics dataset path and schema - why: Maps dataset names to files for organized loading.
        "ratings": (f"{raw_path}/title.ratings.tsv.gz", ratings_schema),  # Ratings dataset path and schema.
        "principals": (f"{raw_path}/title.principals.tsv.gz", principals_schema),  # Principals dataset path and schema.
        "names": (f"{raw_path}/name.basics.tsv.gz", names_schema)  # Names dataset path and schema.
    }
    for name, (raw_file, schema) in datasets.items():  # Loop through each dataset - why: Automates processing for multiple files.
        df = read_tsv_gz(raw_file, schema)  # Read raw file.
        df = df.withColumn("ingestion_date", lit(process_date))  # Add ingestion date column for partitioning - why: Enables time-based partitioning and incremental loads.
        path = f"{bronze_base}/{name}.parquet"  # Define output path.
        write_to_parquet(df, path)  # Write to Parquet.
    print("Progress: Bronze layer complete")

# Silver Layer: Data Processing/Cleaning: Load from bronze, clean data (nulls, trims, casts, filters, dedup), and write to silver - why: Silver refines data for analysis, removing noise and enforcing types.
def process_silver():
    print("Progress: Starting Silver layer (preprocessing)...")
    def preprocess_df(df, key_col, null_variants=["\\N", "", " ", "NULL"], int_cols=[], float_cols=[]):
        print(f"Progress: Preprocessing DataFrame (null handling, trimming, casting)...")
        for c in df.columns:  # Loop through columns - why: Applies transformations uniformly.
            if c != "ingestion_date":  # Skip ingestion_date - why: Date is already properly set.
                df = df.withColumn(c, when(col(c).isin(null_variants), None).otherwise(col(c)))  # Replace null variants with actual nulls - why: Standardizes missing values for consistent handling.
        string_cols = [c for c, t in df.dtypes if t == "string" and c != "ingestion_date"]  # Identify string columns - why: Targets only strings for trimming.
        for c in string_cols:  # Loop through string columns.
            df = df.withColumn(c, trim(col(c)))  # Trim whitespace - why: Removes leading/trailing spaces that could cause matching issues.
        for c in int_cols:  # Loop through integer columns.
            df = df.withColumn(c, col(c).cast(IntegerType()))  # Cast to integer - why: Ensures numeric columns are integers for math operations and filtering.
        for c in float_cols:  # Loop through float columns.
            df = df.withColumn(c, col(c).cast(FloatType()))  # Cast to float - why: Ensures proper decimal handling for ratings or scores.
        for c in int_cols + float_cols:  # Loop through numeric columns.
            df = df.filter(col(c).isNull() | (col(c) >= 0))  # Filter non-negative values or nulls - why: Removes invalid negative values common in raw data errors.
        
        # New: Calculate completeness score (count of non-null columns, excluding key_col and ingestion_date) - why: Quantifies data quality to prioritize fuller rows during dedup.
        from pyspark.sql.functions import expr
        from functools import reduce
        from operator import add
        non_key_cols = [c for c in df.columns if c not in [key_col, "ingestion_date"]]  # List columns to evaluate for completeness - why: Focuses on content columns.
        if non_key_cols:
            completeness_expr = reduce(add, [when(col(c).isNotNull(), lit(1)).otherwise(lit(0)) for c in non_key_cols])  # Reduce/add non-null indicators per row - why: Sums horizontally across columns for per-row score.
        else:
            completeness_expr = lit(0)  # Handle case with no columns to score - why: Avoids errors on empty column lists.
        df = df.withColumn("completeness_score", completeness_expr)  # Add temporary score column.
        
        # New: Deduplicate by keeping the row with the highest completeness score per key and date - why: Retains the most informative duplicate instead of arbitrary first.
        from pyspark.sql.window import Window
        from pyspark.sql.functions import desc, row_number
        window_spec = Window.partitionBy(key_col, "ingestion_date").orderBy(desc("completeness_score"))  # Rank by score descending - why: Highest score first for quality.
        df = df.withColumn("rank", row_number().over(window_spec))  # Add rank - why: Enables filtering top row.
        df = df.filter(col("rank") == 1).drop("rank", "completeness_score")  # Keep only top rank, drop temp columns - why: Cleans up DF after dedup.
        
        print(f"Progress: Preprocessing complete - {df.count()} rows remaining")
        return df

    def load_bronze(name, filter_date=True):
        path = f"{bronze_base}/{name}.parquet"  # Define bronze path.
        print(f"Progress: Loading Bronze data from {path}...")
        df = spark.read.parquet(path)  # Read Parquet - why: Efficient columnar format for big data.
        if (args.incremental or filter_date) and not args.no_date_filter:  # Apply date filter if not disabled - why: Processes only relevant data for efficiency.
            df = df.filter(col("ingestion_date") == lit(process_date))  # Filter by process date.
        row_count = df.count()  # Count rows - why: Checks for data presence.
        if row_count == 0:
            print(f"Warning: Loaded 0 rows from {path} - check Bronze files or use --no-date-filter")  # Warn if empty - why: Alerts to potential issues.
        print(f"Progress: Load complete - {row_count} rows")
        return df

    tb = load_bronze("title_basics")  # Load title basics.
    tb_silver = preprocess_df(tb, "tconst", int_cols=["startYear", "runtimeMinutes"])  # Preprocess title basics.
    print("Progress: Handling genres array in title_basics...")
    tb_silver = tb_silver.withColumn("genres", when(col("genres").isNull(), array(lit("Unknown"))).otherwise(split(col("genres"), ",")))  # Convert genres to array, handle nulls - why: Enables exploding for genre-based analysis.
    tb_silver = tb_silver.filter((col("titleType") == "movie") & (col("startYear") >= 2000))  # Filter for movies since 2000 - why: Focuses on recent movies as per business scope.
    write_to_parquet(tb_silver, f"{silver_base}/movies.parquet")  # Write movies to silver.

    rat = load_bronze("ratings")  # Load ratings.
    rat_silver = preprocess_df(rat, "tconst", int_cols=["numVotes"], float_cols=["averageRating"])  # Preprocess ratings.
    write_to_parquet(rat_silver, f"{silver_base}/ratings.parquet")  # Write ratings to silver.

    prin = load_bronze("principals")  # Load principals.
    prin_silver = preprocess_df(prin, "tconst", int_cols=["ordering"])  # Preprocess principals.
    write_to_parquet(prin_silver, f"{silver_base}/principals.parquet")  # Write principals to silver.

    nam = load_bronze("names")  # Load names.
    nam_silver = preprocess_df(nam, "nconst")  # Preprocess names.
    write_to_parquet(nam_silver, f"{silver_base}/names.parquet")  # Write names to silver.

    print("Progress: Building cast_crew in Silver...")
    cast_crew = prin_silver.join(nam_silver, prin_silver["nconst"] == nam_silver["nconst"], "inner") \
        .select(
            prin_silver["tconst"].alias("movie_id"),  # Select and alias columns for cast_crew - why: Creates a denormalized view for easier querying.
            prin_silver["nconst"].alias("person_id"),
            nam_silver["primaryName"].alias("person_name"),
            prin_silver["category"].alias("role"),
            prin_silver["ingestion_date"]  # Include ingestion_date.
        ).dropDuplicates(["movie_id", "person_id"])  # Deduplicate by movie and person - why: Ensures unique person-movie pairs.
    write_to_parquet(cast_crew, f"{silver_base}/cast_crew.parquet")  # Write cast_crew to silver.
    print("Progress: Silver layer complete")

# Gold Layer: Transformations/Output: Load from silver, compute analytics (popularity, top by genre, directors), write to Parquet and CSV - why: Gold provides business-ready insights like rankings.
def process_gold():
    print("Progress: Starting Gold layer (transformations and output)...")
    def load_silver(name):
        path = f"{silver_base}/{name}.parquet"  # Define silver path.
        print(f"Progress: Loading Silver data from {path}...")
        try:
            df = spark.read.parquet(path)  # Read Parquet.
            if args.incremental:  # Filter by date if incremental - why: Processes only new data.
                df = df.filter(col("ingestion_date") == lit(process_date))
            row_count = df.count()  # Count rows.
            if row_count == 0:
                print(f"Warning: Loaded 0 rows from {path} - no data to process in Gold")  # Warn if empty.
            print(f"Progress: Load complete - {row_count} rows")
            return df.cache()  # Cache for performance in multiple uses - why: Speeds up repeated accesses in joins/aggs.
        except Exception as e:  # Handle load errors - why: Prevents pipeline crash on missing files.
            print(f"Warning: Failed to load {path} - {str(e)}. Skipping.")
            return spark.createDataFrame([], StructType([]))  # Return empty DF to prevent crashes - why: Allows graceful continuation.

    movies = load_silver("movies")  # Load movies.
    ratings = load_silver("ratings")  # Load ratings.
    cast_crew = load_silver("cast_crew")  # Load cast_crew.

    if movies.count() == 0 or ratings.count() == 0:  # Check for sufficient data - why: Skips computations if dependencies missing.
        print("Warning: Skipping Gold computations - insufficient data from Silver")
        return

    print("Progress: Computing popularity score...")
    popularity = movies.join(ratings, movies["tconst"] == ratings["tconst"], "inner") \
        .select(
            movies["tconst"].alias("movie_id"),  
            movies["primaryTitle"].alias("title"),
            movies["startYear"].alias("year"),
            (col("averageRating") * log10(col("numVotes"))).alias("popularity_score"),  
            col("numVotes"),
            movies["genres"],  
            movies["ingestion_date"]  
        )  # No numVotes filter here - why: Matches SQL's unfiltered movie_popularity for broader use in directors.
    print("Popularity rows (Task 1):", popularity.count())  # Debug: Check if data loss here.
    popularity_csv = popularity.withColumn("genres", concat_ws(",", col("genres")))  # Flatten genres array to string for CSV - why: CSV doesn't support arrays.
    write_to_parquet(popularity, f"{gold_base}/popularity.parquet", mode="append")  
    write_to_csv(popularity_csv, f"{gold_base}/popularity.csv")  

    print("Progress: Computing top movies by genre...")
    genre_exploded = popularity.withColumn("genre", explode(col("genres"))) \
        .filter(col("numVotes") >= 5000)  
    print("Genre exploded rows (pre-rank):", genre_exploded.count())  # Debug: Check filter impact.
    window_spec = Window.partitionBy("genre").orderBy(desc("popularity_score"))  
    top_by_genre = genre_exploded.withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") <= 10).drop("rank")  
    print("Top by genre rows (Task 2):", top_by_genre.count())  # Debug: Should be ~ #genres * 10.
    top_by_genre_csv = top_by_genre.withColumn("genres", concat_ws(",", col("genres"))) if "genres" in top_by_genre.columns else top_by_genre  
    write_to_parquet(top_by_genre, f"{gold_base}/top_by_genre.parquet", mode="append")  
    write_to_csv(top_by_genre_csv, f"{gold_base}/top_by_genre.csv")  

    print("Progress: Computing director analytics...")
    directors = cast_crew.filter(col("role") == "director") \
        .join(popularity, "movie_id", "inner") \
        .groupBy("person_id", "person_name") \
        .agg(avg("popularity_score").alias("avg_popularity"), 
    count("movie_id").alias("movie_count")) \
        .filter(col("movie_count") >= 3) \
        .orderBy(desc("avg_popularity")).limit(5)  # Filter directors, join with popularity, aggregate averages and counts, filter >=3 movies, top 5 by avg score - why: Focuses on prolific directors with high average quality.
    directors_pre_filter = cast_crew.filter(col("role") == "director") \
        .join(popularity, "movie_id", "inner") \
        .groupBy("person_id", "person_name") \
        .agg(avg("popularity_score").alias("avg_popularity"), 
    count("movie_id").alias("movie_count"))
    print("Directors before >=3 filter:", directors_pre_filter.count())  # Debug: Check if any directors at all post-join.
    print("Final directors rows:", directors.count())  # Debug: Check after filter and limit.
    directors = directors.withColumn("ingestion_date", lit(process_date))  # Add ingestion_date to directors - why: Consistent partitioning across gold tables.
    write_to_parquet(directors, f"{gold_base}/top_directors.parquet", mode="append")  # Write to Parquet (append mode).
    write_to_csv(directors, f"{gold_base}/top_directors.csv")  # Write to CSV for viewing - why: Human-readable export for Task 3.

    print("Progress: Showing sample outputs for verification...")
    popularity.show(5)  
    top_by_genre.show(5)  
    directors.show()  
    print("Progress: Gold layer complete")

# Run selected layers: Execute bronze, silver, gold based on args.layer, with error handling and final Spark stop - why: Modular execution with try-finally for resource cleanup.
try:
    if args.layer in ["bronze", "all"]:
        process_bronze()  # Run bronze if selected.
    if args.layer in ["silver", "all"]:
        process_silver()  # Run silver if selected.
    if args.layer in ["gold", "all"]:
        process_gold()  # Run gold if selected.

    # Debugging snippet: Load cast_crew, count and show sample directors for verification - why: Extra check to ensure data quality post-silver (moved here to run before spark.stop()).
    if args.layer in ["silver", "all", "gold"]:  # Only run if silver or gold was processed (gold depends on silver).
        cast_crew = spark.read.parquet("data/silver/cast_crew.parquet")  # Load cast_crew for check.
        directors_count = cast_crew.filter(col("role") == "director").count()  # Count directors.
        print("Number of directors in cast_crew:", directors_count)  # Print count.
        if directors_count > 0:
            cast_crew.filter(col("role") == "director").show(5)  # Show sample if exists.
except Exception as e:
    print(f"Error during execution: {str(e)}")  # Catch and print errors - why: Logs issues without crashing.
finally:
    spark.stop()  # Always stop Spark session - why: Releases resources.
    print("Progress: Spark session stopped - pipeline complete or terminated")
