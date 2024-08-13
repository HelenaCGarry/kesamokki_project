import os
import re
from datetime import datetime
from glob import glob

import pandas as pd
import requests
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from dotenv import load_dotenv

def load_environment_variables():
    """Load environment variables from the .env file."""
    load_dotenv()
    return {
        "openrouteservice_key": os.getenv('OPENROUTESERVICE_API_KEY'),
        "google_key": os.getenv('GOOGLE_API_KEY'),
        "origin": 'place_id:ChIJsaJij2X4jUYRlrMoLAHZ8Ps'  # Helsinki Airport place_id
    }

def find_price(metrics: list) -> float:
    """Extract price from metrics."""
    price = next((i.replace('€', '').replace('\xa0', '').replace(',', '.').strip() for i in metrics if '€' in i), pd.NA)
    return float(price) if not pd.isna(price) else price

def find_surface(metrics: list) -> float:
    """Extract surface area from metrics."""
    surface = next((i.split(' ')[0].replace('\xa0', '').replace(',', '.').strip() for i in metrics if 'm²' in i), pd.NA)
    return float(surface) if not pd.isna(surface) else surface

def find_year(metrics: list) -> int:
    """Extract construction year from metrics."""
    years = [int(i) for i in metrics if re.search(r'\d{4}', i)]
    return years[0] if len(years) == 1 else pd.NA

def get_coordinates_nominatim(address: str, geolocator: Nominatim) -> tuple:
    """Get geographical coordinates using Nominatim."""
    try:
        location = geolocator.geocode(address)
        return (location.latitude, location.longitude) if location else (None, None)
    except Exception as e:
        print(f"Error geocoding address with Nominatim {address}: {e}")
        return (None, None)

def get_coordinates_google(address: str, api_key: str) -> tuple:
    """Get geographical coordinates using Google Maps API."""
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

def get_coordinates_openrouteservice(address: str, api_key: str) -> tuple:
    """Get geographical coordinates using OpenRouteService API."""
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

def get_coordinates(row: pd.Series, geolocator: Nominatim, google_key: str, openrouteservice_key: str) -> pd.Series:
    """Determine the coordinates of a listing."""
    if pd.notna(row.get('latitude')) and pd.notna(row.get('longitude')):
        return pd.Series((row['latitude'], row['longitude']))
    coords = get_coordinates_nominatim(row['address'], geolocator)
    if coords == (None, None):
        coords = get_coordinates_google(row['address'], google_key)
    if coords == (None, None):
        coords = get_coordinates_openrouteservice(row['address'], openrouteservice_key)
    return pd.Series(coords)

def get_distance_and_time(row: pd.Series, api_key: str, origin: str) -> pd.Series:
    """Calculate the driving distance and time from HEL to the listing."""
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

def process_listings(data: str, most_recent_date: datetime, google_key: str, openrouteservice_key: str) -> pd.DataFrame:
    """Process listings to extract relevant information and calculate metrics."""
    jsons = data.split('[\n')
    etuovi_listings = eval(''.join(jsons[1][:-1].split("\n")))
    listing_details = eval(''.join(jsons[2][:-2].split("\n")))

    df_listings = pd.DataFrame(etuovi_listings)
    df_details = pd.DataFrame(listing_details)

    new_df = df_listings.merge(df_details, on='url', how='left')

    new_df["price"] = new_df["metrics"].apply(find_price)
    new_df["surface"] = new_df["metrics"].apply(find_surface)
    new_df["year"] = new_df["metrics"].apply(find_year)
    new_df = new_df.drop(['metrics'], axis=1)

    new_df['description'] = new_df['description'].str.replace('Mökki tai huvila | ', '', regex=False)
    new_df['rooms'] = new_df['rooms'].map({
        'Ei tiedossa': pd.NA, 'Yksiö': 1, 'Kaksio': 2, '3 huonetta': 3, '4 huonetta': 4, '5 huonetta': 5, 'Yli 5 huonetta': 6
    })

    return new_df

def merge_and_update_data(old_df: pd.DataFrame, new_df: pd.DataFrame, most_recent_date: datetime) -> pd.DataFrame:
    """Merge old and new data and update columns."""
    merged_df = new_df.merge(old_df, on='url', how='left', suffixes=('', '_old'))

    merged_df['first_posting_date'] = merged_df.apply(
        lambda row: row['first_posting_date'] if pd.notna(row['first_posting_date']) else most_recent_date,
        axis=1
    )

    merged_df['last_posting_date'] = most_recent_date
    merged_df['original_price'] = merged_df.apply(
        lambda row: row['original_price'] if pd.notna(row['original_price']) else row['price'],
        axis=1
    )

    return merged_df.drop(columns=[col for col in merged_df.columns if col.endswith('_old')])

def save_to_csv(final_df: pd.DataFrame, final_path: str):
    """Save the final DataFrame to a CSV file."""
    final_df.to_csv(final_path, index=False)

def transform_data():
    env_vars = load_environment_variables()
    geolocator = Nominatim(user_agent="kesa_mokki_project")
    final_df = None

    # Get the most recent data files
    csv_files = sorted(glob(os.path.join('data/cabins', 'etuovi_data_*.csv')), key=lambda x: os.path.basename(x).split('_')[-1].split('.')[0], reverse=True)
    json_files = sorted(glob(os.path.join('data/cabins', 'etuovi_data_*.json')), key=lambda x: os.path.basename(x).split('_')[-1].split('.')[0], reverse=True)

    # Extract the timestamp from the most recent file's filename
    most_recent_date = datetime.strptime(os.path.basename(json_files[0]).split('_')[-1].split('.')[0], '%Y%m%d-%H%M%S').date()

    # Load previous week data
    old_df = pd.read_csv(csv_files[0])

    # Load and process new data
    with open(json_files[0], 'r') as data_raw:
        data = data_raw.read()

    new_df = process_listings(data, most_recent_date, env_vars["google_key"], env_vars["openrouteservice_key"])

    # Merge with old data and update
    final_df = merge_and_update_data(old_df, new_df, most_recent_date)

    # Obtain geographical data from new listings
    final_df[['latitude', 'longitude']] = final_df.apply(
        lambda row: get_coordinates(row, geolocator, env_vars["google_key"], env_vars["openrouteservice_key"]), axis=1
    )

    # Obtain driving distance and time
    final_df[['distance', 'duration']] = final_df.apply(
        lambda row: get_distance_and_time(row, env_vars["google_key"], env_vars["origin"]), axis=1
    )

    # Save final data to CSV
    final_path = json_files[0].replace('.json', '.csv')
    save_to_csv(final_df, final_path)

if __name__ == "__main__":
    transform_data()
