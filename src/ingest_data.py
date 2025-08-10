import os
import pandas as pd
from sqlalchemy import create_engine

# Read environment variables
csv_file = os.getenv('CSV_FILE_PATH', '/src/data/val_round_details_sample.csv')
db_user = os.getenv('POSTGRES_USER', 'admin')
db_password = os.getenv('POSTGRES_PASSWORD', 'admin123')
db_host = os.getenv('POSTGRES_HOST', 'db')  # 'db' is the Docker service name
db_port = os.getenv('POSTGRES_PORT', '5432')
db_name = os.getenv('POSTGRES_DB', 'mydb')

# PostgreSQL connection URL
db_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

# Create SQLAlchemy engine
engine = create_engine(db_url)

# Read CSV and ingest into PostgreSQL
try:
    df = pd.read_csv(csv_file)
    df.to_sql('round_data', engine, if_exists='replace', index=False)
    print("✅ Data ingested successfully into 'round_data' table.")
except Exception as e:
    print(f"❌ Error ingesting data: {e}")
