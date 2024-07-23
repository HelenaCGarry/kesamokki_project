import os
import re
from datetime import datetime
from glob import glob

import pandas as pd
import requests
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from dotenv import load_dotenv

# Load environment variables from the .env file (if present)
load_dotenv()

# Access environment variables 
OPENROUTESERVICE_KEY = os.getenv('OPENROUTESERVICE_API_KEY')
GOOGLE_KEY = os.getenv('GOOGLE_API_KEY')
ORIGIN = 'place_id:ChIJsaJij2X4jUYRlrMoLAHZ8Ps' # Helsinki Airport place_id

# Define the folder path
FOLDER_PATH = 'data/cabins'

# Helper functions to find price, surface, and year
def find_price(metrics):
    price = next((i.replace('€', '').replace('\xa0', '').replace(',', '.').strip() for i in metrics if '€' in i), pd.NA)
    return float(price) if price != pd.NA else price

def find_surface(metrics):
    surface = next((i.split(' ')[0].replace('\xa0', '').replace(',', '.').strip() for i in metrics if 'm²' in i), pd.NA)
    return float(surface) if surface != pd.NA else surface

def find_year(metrics):
    years = [int(i) for i in metrics if re.search(r'\d{4}', i)]
    return years[0] if len(years) == 1 else pd.NA

# Helper functions to find the geographical coordinates of an address location

def get_coordinates_nominatim(address):
    try:
        location = geocode(address)
        return (location.latitude, location.longitude) if location else (None, None)
    except Exception as e:
        print(f"Error geocoding address with Nominatim {address}: {e}")
        return (None, None)

def get_coordinates_google(address, api_key):
    try:
        url = f'https://maps.googleapis.com/maps/api/geocode/json?address={address}&key={api_key}'
        response = requests.get(url).json()
        if response['status'] == 'OK':
            location = response['results'][0]['geometry']['location']
            return location['lat'], location['lng']
        return (None, None)
    except Exception as e:
        print(f"Error geocoding address with Google {address}: {e}")
        return (None, None)

def get_coordinates_openrouteservice(address, api_key):
    try:
        url = f'https://api.openrouteservice.org/geocode/search?api_key={api_key}&text={address}'
        response = requests.get(url).json()
        if response.get('features'):
            location = response['features'][0]['geometry']['coordinates']
            return location[1], location[0]
        return (None, None)
    except Exception as e:
        print(f"Error geocoding address with OpenRouteService {address}: {e}")
        return (None, None)

def get_coordinates(row):
    if pd.notna(row.get('latitude')) and pd.notna(row.get('longitude')):
        return pd.Series((row['latitude'], row['longitude']))
    coords = get_coordinates_nominatim(row['address'])
    if coords == (None, None):
        coords = get_coordinates_google(row['address'], GOOGLE_KEY)
    if coords == (None, None):
        coords = get_coordinates_openrouteservice(row['address'], OPENROUTESERVICE_KEY)
    return pd.Series(coords)

# Function to obtain driving distance from HEL

def get_distance_and_time(row, api_key, origin):
    if pd.isna(row['latitude']) or pd.isna(row['longitude']):
        return pd.Series([None, None])
    if pd.notna(row.get('distance')) and pd.notna(row.get('duration')):
        return pd.Series([row['distance'], row['duration']])
    
    try:
        destination = f"{row['latitude']},{row['longitude']}"
        url = f"https://maps.googleapis.com/maps/api/distancematrix/json?units=metric&origins={origin}&destinations={destination}&key={api_key}"
        response = requests.get(url).json()
        if response['status'] == 'OK':
            element = response['rows'][0]['elements'][0]
            if element['status'] == 'OK':
                distance = element['distance']['text']
                duration = element['duration']['text']
                return pd.Series([distance, duration])
        return pd.Series([None, None])
    except Exception as e:
        print(f"Error fetching distance and time for {row['latitude']}, {row['longitude']}: {e}")
        return pd.Series([None, None])


# Get a list of all CSV and JSON files in the folder
csv_files = sorted(glob(os.path.join(FOLDER_PATH, 'etuovi_data_*.csv')), key=lambda x: os.path.basename(x).split('_')[-1].split('.')[0], reverse=True)
json_files = sorted(glob(os.path.join(FOLDER_PATH, 'etuovi_data_*.json')), key=lambda x: os.path.basename(x).split('_')[-1].split('.')[0], reverse=True)

# Extract the timestamp from the most recent file's filename
most_recent_date = datetime.strptime(os.path.basename(json_files[0]).split('_')[-1].split('.')[0], '%Y%m%d-%H%M%S').date()

# Load data from the previous week (already in csv format)
old_df = pd.read_csv(csv_files[0])

# Load data from latest scraping (still in json format)
with open(json_files[0], 'r') as data_raw:
    data = data_raw.read()

jsons = data.split('[\n')
etuovi_listings = eval(''.join(jsons[1][:-1].split("\n")))
listing_details = eval(''.join(jsons[2][:-2].split("\n")))

df_listings = pd.DataFrame(etuovi_listings)
df_details = pd.DataFrame(listing_details)

new_df = df_listings.merge(df_details, on='url', how='left')


# Extract price, surface, year data from listing metrics
new_df["price"] = new_df["metrics"].apply(find_price)
new_df["surface"] = new_df["metrics"].apply(find_surface)
new_df["year"] = new_df["metrics"].apply(find_year)
new_df = new_df.drop(['metrics'], axis=1)

# Clean 'description' column
new_df['description'] = new_df['description'].str.replace('Mökki tai huvila | ', '', regex=False).str.replace('Mökki tai huvila', '', regex=False)
new_df['description'] = new_df['description'].str.replace('Mökki tai huvila', '', regex=False).str.replace('Mökki tai huvila', '', regex=False)


# Convert 'rooms' values to integer
room_values = {'Ei tiedossa': pd.NA, 'Yksiö': 1, 'Kaksio': 2, '3 huonetta': 3, '4 huonetta': 4, '5 huonetta': 5, 'Yli 5 huonetta': 6}
new_df['rooms'] = new_df['rooms'].map(room_values)

# Merge the dataframes on "url"
merged_df = new_df.merge(old_df, on='url', how='left', suffixes=('', '_old'))

# Update the columns
merged_df['first_posting_date'] = merged_df.apply(
    lambda row: row['first_posting_date'] if pd.notna(row['first_posting_date']) else most_recent_date,
    axis=1
)

merged_df['last_posting_date'] = most_recent_date

# Obtain geographical data from new listings

# Initialize Nominatim API
geolocator = Nominatim(user_agent="kesa_mokki_project")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

merged_df[['latitude', 'longitude']] = merged_df.apply(get_coordinates, axis=1)

merged_df[['distance', 'duration']] = merged_df.apply(
    lambda row: get_distance_and_time(row, GOOGLE_KEY, ORIGIN), axis=1
)

# Create original_price data for new listings and update for past listings

merged_df['original_price'] = merged_df.apply(
    lambda row: row['original_price'] if pd.notna(row['original_price']) else row['price'],
    axis=1
)

# Drop the old columns that were added during the merge
final_df = merged_df.drop(columns=[col for col in merged_df.columns if col.endswith('_old')])

final_path = json_files[0].replace('.json', '.csv')
final_df.to_csv(final_path, index=False)
