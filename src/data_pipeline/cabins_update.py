import os
import logging
from glob import glob
from dotenv import load_dotenv

import pandas as pd
from sqlalchemy import create_engine, text

# Load environment variables from the .env file (if present)
load_dotenv()

# Access environment variables 
POSTGRES_USER = os.getenv('PostgreSQL_USERNAME')
POSTGRES_PSW = os.getenv('PostgreSQL_PSW')
POSTGRES_SERVER = os.getenv('PostgreSQL_SERVER')
POSTGRES_PORT = os.getenv('PostgreSQL_PORT')
POSTGRES_DATABASE = os.getenv('PostgreSQL_DATABASE')

# Define the folder path
FOLDER_PATH = r'data\cabins'

def define_new_file():
    # Get the list of CSV files sorted by the date in the filename
    csv_files = sorted(glob(os.path.join(FOLDER_PATH, 'etuovi_data_*.csv')), key=lambda x: os.path.basename(x).split('_')[-1].split('.')[0], reverse=True)
    return csv_files[0]

def update_data():
    new_file = define_new_file()
    new_data = pd.read_csv(new_file)

    new_data['winterized'] = new_data['winterized'].apply(lambda x: True if x == 'YES' else False)

    # Convert date columns from text to datetime
    new_data['first_posting_date'] = pd.to_datetime(new_data['first_posting_date']).dt.date
    new_data['last_posting_date'] = pd.to_datetime(new_data['last_posting_date']).dt.date

    # Database connection
    engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PSW}@{POSTGRES_SERVER}:{POSTGRES_PORT}/{POSTGRES_DATABASE}')
    
    # Insert new data into temporary table
    new_data.to_sql('cabins_temp', engine, if_exists='replace', index=False)
    
    # Upsert data into main table and get counts
    with engine.connect() as conn:
        try:
            result = conn.execute(text("""
                WITH upserted AS (
                    INSERT INTO cabins_main (
                        address, url, description, rooms, winterized, price, surface, year, original_price,
                        latitude, longitude, distance, duration, first_posting_date, last_posting_date
                    )
                    SELECT 
                        address, url, description, rooms, winterized, price, surface, year, original_price,
                        latitude, longitude, distance, duration, first_posting_date, last_posting_date
                    FROM cabins_temp
                    ON CONFLICT (url) DO UPDATE SET
                        address = EXCLUDED.address,
                        description = EXCLUDED.description,
                        rooms = EXCLUDED.rooms,
                        winterized = EXCLUDED.winterized,
                        price = EXCLUDED.price,
                        surface = EXCLUDED.surface,
                        year = EXCLUDED.year,
                        original_price = EXCLUDED.original_price,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        distance = EXCLUDED.distance,
                        duration = EXCLUDED.duration,
                        first_posting_date = EXCLUDED.first_posting_date,
                        last_posting_date = EXCLUDED.last_posting_date
                    RETURNING 
                        (xmax = 0) AS inserted -- true if row was inserted, false if updated
                )
                SELECT 
                    COUNT(*) FILTER (WHERE inserted) AS new_rows,
                    COUNT(*) FILTER (WHERE NOT inserted) AS updated_rows
                FROM upserted;
            """))

            counts = result.fetchone()
            new_rows = counts['new_rows']
            updated_rows = counts['updated_rows']

            logging.info(f"Successfully uploaded data: {new_rows} new rows, {updated_rows} updated rows.")
        
        except Exception as e:
            logging.error(f"Error during execution: {e}")

# If running this file directly
if __name__ == "__main__":
    update_data()
