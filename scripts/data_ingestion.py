import requests
import gzip
import shutil
import pandas as pd
import pyodbc
import os
from datetime import datetime

# Download URLs
urls = {
    'title.basics': 'https://datasets.imdbws.com/title.basics.tsv.gz',
    'title.ratings': 'https://datasets.imdbws.com/title.ratings.tsv.gz',
    'title.principals': 'https://datasets.imdbws.com/title.principals.tsv.gz',
    'name.basics': 'https://datasets.imdbws.com/name.basics.tsv.gz'
}

# Connection to SQL Server (use your working conn_str)
conn_str = r'DRIVER={ODBC Driver 17 for SQL Server};SERVER=laptop-jfg7a0bl\SQLEXPRESS;DATABASE=DataWarehouse;Trusted_Connection=yes'
conn = pyodbc.connect(conn_str)
conn.autocommit = False  # Enable for bulk ops
cursor = conn.cursor()
cursor.fast_executemany = True  # Faster bulk inserts

# Create Bronze schema if not exists
cursor.execute("""
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');
""")

# Create tables with proper SQL Server conditional syntax
cursor.execute("""
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'bronze.title_basics') AND type IN (N'U'))
BEGIN
    CREATE TABLE bronze.title_basics (
        tconst VARCHAR(50),
        titleType VARCHAR(50),
        primaryTitle VARCHAR(MAX),
        originalTitle VARCHAR(MAX),
        isAdult VARCHAR(10),
        startYear VARCHAR(10),
        endYear VARCHAR(10),
        runtimeMinutes VARCHAR(10),
        genres VARCHAR(MAX),
        DW_LoadDate DATETIME DEFAULT GETDATE()
    );
END
""")

cursor.execute("""
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'bronze.title_ratings') AND type IN (N'U'))
BEGIN
    CREATE TABLE bronze.title_ratings (
        tconst VARCHAR(50),
        averageRating VARCHAR(10),
        numVotes VARCHAR(10),
        DW_LoadDate DATETIME DEFAULT GETDATE()
    );
END
""")

cursor.execute("""
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'bronze.title_principals') AND type IN (N'U'))
BEGIN
    CREATE TABLE bronze.title_principals (
        tconst VARCHAR(50),
        ordering VARCHAR(10),
        nconst VARCHAR(50),
        category VARCHAR(50),
        job VARCHAR(MAX),
        characters VARCHAR(MAX),
        DW_LoadDate DATETIME DEFAULT GETDATE()
    );
END
""")

cursor.execute("""
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'bronze.name_basics') AND type IN (N'U'))
BEGIN
    CREATE TABLE bronze.name_basics (
        nconst VARCHAR(50),
        primaryName VARCHAR(MAX),
        birthYear VARCHAR(10),
        deathYear VARCHAR(10),
        primaryProfession VARCHAR(MAX),
        knownForTitles VARCHAR(MAX),
        DW_LoadDate DATETIME DEFAULT GETDATE()
    );
END
""")

conn.commit()

# Function to download, unzip, and load each file in chunks
def ingest_file(file_key, url):
    gz_file = f'{file_key}.tsv.gz'
    
    # Download if not exists
    if not os.path.exists(gz_file):
        print(f"Downloading {file_key} from {url}...")
        response = requests.get(url, stream=True)
        with open(gz_file, 'wb') as f:
            shutil.copyfileobj(response.raw, f)
        print(f"Download complete: {gz_file}")
    
    # Load directly from gz (no unzip needed)
    table_name = f'bronze.{file_key.replace(".", "_")}'
    print(f"Truncating and loading {table_name} from {gz_file}...")
    cursor.execute(f"TRUNCATE TABLE {table_name}")
    for chunk_num, chunk in enumerate(pd.read_csv(gz_file, sep='\t', chunksize=100000, low_memory=False, quoting=3, dtype=str, compression='gzip')):
        chunk = chunk.fillna('\\N').astype(str)  # Ensure no NaNs and all strings
        print(f"Processing chunk {chunk_num + 1}...")
        values = [tuple(row) for _, row in chunk.iterrows()]
        columns = ','.join(chunk.columns)
        placeholders = ','.join(['?'] * len(chunk.columns))
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cursor.executemany(sql, values)
        conn.commit()
    print(f"Loaded {file_key} into {table_name} at {datetime.now()}")

# Run ingestion for all files
for key, url in urls.items():
    ingest_file(key, url)

# Close connection
conn.close()
print("Ingestion complete!")
