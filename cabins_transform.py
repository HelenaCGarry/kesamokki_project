import pandas as pd # type: ignore
import requests # type: ignore
import re
import os
from glob import glob
from datetime import datetime

from geopy.geocoders import Nominatim # type: ignore
from geopy.extra.rate_limiter import RateLimiter # type: ignore
from dotenv import load_dotenv
import os

# Load environment variables from the .env file (if present)
load_dotenv()

# Access environment variables as if they came from the actual environment
OPENROUTESERVICE_KEY = os.getenv('OPENROUTESERVICE_API_KEY')
GOOGLE_KEY = os.getenv('GOOGLE_API_KEY')


# Define the folder path
folder_path = 'data/cabins'

# Get a list of all CSV files in the folder
csv_files = glob(os.path.join(folder_path, 'etuovi_data_*.csv'))

# Get a list of all JSON files in the folder
json_files = glob(os.path.join(folder_path, 'etuovi_data_*.json'))

# Extract the timestamp from the filename and sort by it
csv_files.sort(key=lambda x: os.path.basename(x).split('_')[-1].split('.')[0], reverse=True)

# Extract the timestamp from the filename and sort by it
json_files.sort(key=lambda x: os.path.basename(x).split('_')[-1].split('.')[0], reverse=True)

# Extract the timestamp from the most recent file's filename
most_recent_date = datetime.strptime(os.path.basename(json_files[0]).split('_')[-1].split('.')[0], '%Y%m%d-%H%M%S').date()

# Open the second most recent file and name it df_minus_one
df_minus_one = pd.read_csv(csv_files[0])


# Open the most recent json file

data_raw = open(json_files[0],"r") #sustitute this for openening the most recent file in data
# then write a line that opens the most recent file in S3
data = data_raw.read()
jsons = data.split('[\n')

etuovi_listings = jsons[1][:-1].split("\n")
etuovi_listings = ''.join(etuovi_listings)
etuovi_listings = eval(etuovi_listings)
df_listings =pd.DataFrame(etuovi_listings) 

listing_details = jsons[2][:-2].split("\n")
listing_details = ''.join(listing_details)
listing_details = eval(listing_details)
df_details =pd.DataFrame(listing_details) 

df = df_listings.merge(df_details, on = 'url', how = 'left')


# Find the price of the property 
def find_price(x):
    # first instance of "€" in the list of strings "metrics"
    euro_symbol = '€'
    euro_hits = [i for i in x if euro_symbol in i]
    if len(euro_hits) > 0:
        price = euro_hits[0]
        # then remove the symbol and spaces
        price = price.replace('€', '')
        price = price.replace('\xa0', '')
        price = price.replace(',', '.')
        #convert to float
        return float(price)

    else:
        return pd.NA

# Find the surface of the property

def find_surface(x):
    # first instance of "m²" in the list of strings "metrics"
    sqmt_symbol = 'm²'
    surface_hits = [i for i in x if sqmt_symbol in i]
    if len(surface_hits) > 0:
        surface = surface_hits[0]
        # split by spaces and get the first value
        surface = surface.split(' ')[0]
        surface = surface.replace('\xa0', '')
        # convert to float
        surface = surface.replace(',', '.')
        return float(surface)

    else:
        return pd.NA

    
# Find the year of built

def find_year(x):
    year_hits = []
    for i in x:
        if re.search('\d{4}', i):
            year_hits.append(i)
    if len(year_hits)==1:
        return int(year_hits[0])
    else:
        return pd.NA

df["price"] = df["metrics"].apply(find_price)
df["surface"] = df["metrics"].apply(find_surface)
df["year"] = df["metrics"].apply(find_year)
df=df.drop(['metrics'], axis=1)

# remove 'Mökki tai huvila | ' from 'description'

df['description'] = df['description'].str.replace('Mökki tai huvila | ', '', regex = False)
df['description'] = df['description'].str.replace('Mökki tai huvila', '', regex = False)

# convert 'rooms' values to integer
room_values = {'Ei tiedossa': pd.NA, 'Yksiö': 1, 'Kaksio': 2, '3 huonetta': 3, '4 huonetta': 4, '5 huonetta' : 5, 'Yli 5 huonetta' : 6}

df['rooms'] = df['rooms'].map(room_values)

# Initialize Nominatim API
geolocator = Nominatim(user_agent="kesa_mokki_project")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)


def get_coordinates_nominatim(address):
    try:
        location = geocode(address)
        if location:
            return location.latitude, location.longitude
        else:
            return None, None
    except Exception as e:
        print(f"Error geocoding address with Nominatim {address}: {e}")
        return None, None

def get_coordinates_google(address, api_key):
    try:
        url = f'https://maps.googleapis.com/maps/api/geocode/json?address={address}&key={api_key}'
        response = requests.get(url)
        response_json = response.json()

        if response_json['status'] == 'OK':
            location = response_json['results'][0]['geometry']['location']
            return location['lat'], location['lng']
        else:
            return None, None
    except Exception as e:
        print(f"Error geocoding address with Google {address}: {e}")
        return None, None

def get_coordinates_openrouteservice(address, api_key):
    try:
        url = f'https://api.openrouteservice.org/geocode/search?api_key={api_key}&text={address}'
        response = requests.get(url)
        response_json = response.json()

        if response.status_code == 200 and 'features' in response_json and len(response_json['features']) > 0:
            location = response_json['features'][0]['geometry']['coordinates']
            return location[1], location[0]
        else:
            return None, None
    except Exception as e:
        print(f"Error geocoding address with OpenRouteService {address}: {e}")
        return None, None

def get_coordinates(row):
    try:
        if pd.notna(row.get('latitude')) and pd.notna(row.get('longitude')):
            coords = (row['latitude'], row['longitude'])
        else:
            coords = get_coordinates_nominatim(row['address'])
            if coords == (None, None):
                coords = get_coordinates_google(row['address'], GOOGLE_KEY)
            if coords == (None, None):
                coords = get_coordinates_openrouteservice(row['address'], OPENROUTESERVICE_KEY)
        return pd.Series(coords)
    except Exception as e:
        print(f"Error geocoding row {row}: {e}")
        return pd.Series((None, None))

# Obtain driving distance from HEL
ORIGIN = 'place_id:ChIJsaJij2X4jUYRlrMoLAHZ8Ps' #Helsinki Airport place_id

def get_distance_and_time(row, api_key, origin):
    if pd.isna(row['latitude']) or pd.isna(row['longitude']):
        return pd.Series([None, None])
    
    if pd.notna(row.get('distance')) and pd.notna(row.get('duration')):
        return pd.Series([row['distance'], row['duration']])
    
    try:
        destination = f"{row['latitude']},{row['longitude']}"
        url = f"https://maps.googleapis.com/maps/api/distancematrix/json?units=metric&origins={origin}&destinations={destination}&key={api_key}"
        response = requests.get(url)
        response_json = response.json()

        if response_json['status'] == 'OK':
            element = response_json['rows'][0]['elements'][0]
            if element['status'] == 'OK':
                distance = element['distance']['text']
                duration = element['duration']['text']
                return pd.Series([distance, duration])
            else:
                return pd.Series([None, None])
        else:
            return pd.Series([None, None])
    except Exception as e:
        print(f"Error fetching distance and time for {row['latitude']}, {row['longitude']}: {e}")
        return pd.Series([None, None])
    
    # Merge the dataframes on "url"
merged_df = pd.merge(df, df_minus_one, on='url', how='left', suffixes=('', '_old'))

# Update the columns
merged_df['first_posting_date'] = merged_df.apply(
    lambda row: row['first_posting_date'] if pd.notna(row['first_posting_date']) else most_recent_date,
    axis=1
)

merged_df['last_posting_date'] = most_recent_date

merged_df[['latitude', 'longitude']] = merged_df.apply(
    lambda row: get_coordinates(row), axis=1
)

merged_df[['distance', 'duration']] = merged_df.apply(
    lambda row: pd.Series(get_distance_and_time(row, GOOGLE_KEY, ORIGIN)), axis=1
)

merged_df['original_price'] = merged_df.apply(
    lambda row: row['original_price'] if pd.notna(row['original_price']) else row['price'],
    axis=1
)

# Drop the old columns that were added during the merge
final_df = merged_df.drop(columns=[col for col in merged_df.columns if col.endswith('_old')])

final_path = json_files[0].split('.')[0]+".csv"
final_df.to_csv(final_path, index= False)