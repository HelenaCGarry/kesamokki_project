# Import libraries
import pandas as pd # type: ignore
import requests # type: ignore
from bs4 import BeautifulSoup # type: ignore

from dotenv import load_dotenv
import os

# API Key
# Load environment variables from the .env file (if present)
load_dotenv()

# Access environment variables as if they came from the actual environment
OPENROUTESERVICE_KEY = os.getenv('OPENROUTESERVICE_API_KEY')

# Define URL and retrieve the page content
url_hospitals = "https://fi.wikipedia.org/wiki/Luettelo_Suomen_sairaaloista"
page_hospitals = requests.get(url_hospitals)
soup_hospitals = BeautifulSoup(page_hospitals.text, 'html.parser')

# Define helper functions
def has_numbers(text):
    return any(char.isdigit() for char in text)

def extract_hospitals(soup):
    result = []
    for table in soup.find_all('ul'):
        for element in table.find_all('li'):
            if has_numbers(element.text):
                continue
            if "Pohjola Sairaala" in element.text:
                result.extend(["Pihlajalinna, Helsinki", 
                               "Pihlajalinna, Tampere", 
                               "Pihlajalinna, Oulu", 
                               "Pihlajalinna, Kuopio", 
                               "Pihlajalinna, Turku"])
            elif element.text not in ["Helsinki", "Oulu", "Tampere", "Kuopio", "Turku"]:
                result.append(element.text)
    
    stop_index = result.index("Suomen julkisen terveydenhuollon tehohoito on keskitetty suurimpiin sairaaloihin")
    return result[:stop_index]

# Extract hospital data
hospitals = extract_hospitals(soup_hospitals)

# Create hospitals dataframe
pd.set_option('display.max_colwidth', None)
df_hospitals = pd.DataFrame(hospitals, columns=['name'])

def create_city_column(x):
    chunks = x.split(',')
    return chunks[-1] if len(chunks) > 1 else None

df_hospitals["location"] = df_hospitals["name"].apply(create_city_column)
df_hospitals = df_hospitals[df_hospitals.name != "Pohjola Sairaala\nHelsinki\nTampere\nOulu\nKuopio\nTurku"]

def add_hospital_network(x):
    if "KYS" in x:
        return "Kuopion yliopistollinen sairaala"
    elif "HYKS" in x:
        return "Helsingin seudun yliopistollinen keskussairaala"
    elif "KHKS" in x:
        return "Kanta-Hämeen keskussairaala"
    elif "TAYS" in x:
        return "Tampereen yliopistollinen sairaala"
    elif "OYS" in x:
        return "Oulun yliopistollinen sairaala"
    elif "TYKS" in x:
        return "Turun yliopistollinen keskussairaala"
    elif "KSKS" in x:
        return "Keski-Suomen keskussairaala"

def clean_location_name(x):
    if isinstance(x, str):
        if "(" in x:
            return x.split("(")[0].strip()
        return x.strip()
    return "Helsinki"

def clean_hospital_name(x):
    x = x.split(",")[0].strip()
    if "(" in x:
        x = x.split("(")[0].strip()
    return x.strip()

df_hospitals["network"] = df_hospitals["name"].apply(add_hospital_network)
df_hospitals["location"] = df_hospitals["location"].apply(clean_location_name)
df_hospitals["name"] = df_hospitals["name"].apply(clean_hospital_name)
df_hospitals["type"] = "Hospital"


# Define URL and retrieve the page content for healthcare centers
url_health_centers = "https://fi.wikipedia.org/wiki/Luettelo_Suomen_terveysasemista_ja_terveyskeskusp%C3%A4ivystyksist%C3%A4"
page_health_centers = requests.get(url_health_centers)
soup_health_centers = BeautifulSoup(page_health_centers.text, 'html.parser')

# Extract health center data
table = soup_health_centers.find("table")

rows = []
data_rows = table.find_all('tr')

for row in data_rows:
    value = row.find_all('td')
    beautified_value = [ele.text.strip() for ele in value]
    if beautified_value:
        rows.append(beautified_value)

# Convert the extracted data directly to a dataframe
headers = ["name", "location", "address", "website"]
df_health_centers = pd.DataFrame(rows, columns=headers)
df_health_centers["type"] = "Health Center"

# Clean health center data
df_health_centers = df_health_centers.replace('\[(.*?)\]', '', regex=True)
df_health_centers = df_health_centers.drop([1, 4]).reset_index(drop=True)

# Combine the hospitals and health centers dataframes
df_combined = pd.concat([df_hospitals, df_health_centers], ignore_index=True)

# List of Hospitals with no address
list_of_names = df_combined['name'][df_combined['address'].isna()].tolist()

# List of corresponding addresses obtained by manual scraping

list_of_addresses = ['Tenholantie 10, 00280 Helsinki, Finland',
'Kiinamyllynkatu 4-8, 20520 Turku, Finland',
'Kajaanintie 50, 90220 Oulu, Finland',
'Elämänaukio 2, 33520 Tampere, Finland',
'Puijonlaaksontie 2, 70200 Kuopio, Finland',
'Kotkantie 41, 48210 Kotka, Finland',
'Valto Käkelän katu 1, 53130 Lappeenranta, Finland',
'Keskussairaalankatu 7, 15850 Lahti, Finland',
'Parantolankatu 6, 13530 Hämeenlinna, Finland',
'Sairaalantie 3, 28500 Pori, Finland',
'Ounasrinteentie 22, 96400 Rovaniemi, Finland',
'Hietalahdenkatu 2 4, 65130 Vaasa, Finland',
'Hanneksenrinne 7, 60220 Seinäjoki, Finland',
'Mariankatu, 67200 Kokkola, Finland',
'Hoitajantie 3, 40620 Jyväskylä, Finland',
'Porrassalmenkatu 35 37, 50100 Mikkeli, Finland',
'Keskussairaalantie 6, 57120 Savonlinna, Finland',
'Tikkamäentie 16, 80210 Joensuu, Finland',
'Sotkamontie 13, 87300 Kajaani, Finland',
'1 Doktorsvägen, Mariehamn 22100, Åland Islands',
'Kauppakatu 25, 94100 Kemi, Finland',
'Stornäsvägen 40, 22410 Godby, Finland',
'Hanneksenrinne 7, 60220 Seinäjoki, Finland',
'Sairaalantie 14, 76100 Pieksämäki, Finland',
'Nordenskiöldinkatu 20, 00250 Helsinki, Finland',
'Haartmaninkatu 4 Rakennus 12, 00290 Helsinki, Finland',
'Välskärinkatu 12, 00260 Helsinki, Finland',
'Meilahdentie 2, 00250 Helsinki, Finland',
'Kasarmikatu 11-13, 00130 Helsinki, Finland',
'Talvelantie 6, 00700 Helsinki, Finland',
'Haartmaninkatu 2, 00290 Helsinki, Finland',
'Tenholantie 10, 00280 Helsinki, Finland',
'Stenbäckinkatu 11, 00290 Helsinki, Finland',
'Haartmaninkatu 4 C ja E, 00290 Helsinki, Finland',
'Haartmaninkatu 4, 00290 Helsinki, Finland',
'Suursuonlaita 3 B, 00630 Helsinki, Finland',
'Haartmaninkatu 4 Rakennus 2, 00290 Helsinki, Finland',
'Stenbäckinkatu 9 Rakennus 6, 00290 Helsinki, Finland',
'Sairaalantie 1, 06150 Porvoo, Finland',
'Sairaalankatu 1, 05850 Hyvinkää, Finland',
'Kiljavantie 539 A, 05250 Kiljava, Finland',
'Ohkolantie 10, 04500 Mäntsälä, Finland',
'Karvasmäentie 6, 02740 Espoo, Finland',
'Karvasmäentie 8, 02740 Espoo, Finland',
'Sairaalatie 8, 08200 Lohja, Finland',
'Östra Strandgatan 9, 10600 Ekenäs, Finland',
'Katriinankuja 4, 01760 Vantaa, Finland',
'Sairaalakatu 1, 01400 Vantaa, Finland',
'Urheilukentänkatu 9, 30100 Forssa, Finland',
'Kontiontie 77, 11120 Riihimäki, Finland',
'Viipurintie 1-3, 13200 Hämeenlinna, Finland',
'Sairaalantie 11, 42100 Jämsä, Finland',
'Sairaalankuja 3d, 45750 Kouvola, Finland',
'Ruskeasuonkatu 3, 45100 Kouvola, Finland',
'Toivolantie 1, 95410 Tornio, Finland',
'Hatanpäänkatu 24, 33900 Tampere, Finland',
'Itsenäisyydentie 2, 38200 Sastamala, Finland',
'Elämänaukio 1, 33520 Tampere, Finland',
'',
'Niveltie 4, 33520 Tampere, Finland',
'Salonkatu 24, 37600 Valkeakoski, Finland',
'',
'Kajaanintie 48, 90220 Oulu, Finland',
'Stenbäckinkatu 11, 00290 Helsinki, Finland',
'Välskärinkatu 12, 00260 Helsinki, Finland',
'Rantakatu 4, Rantakatu 4, 92100 Raahe, Finland',
'Talvelantie 6, 00700 Helsinki, Finland',
'Bottenviksvägen 1, 68600 Jakobstad, Finland',
'Lapväärtintie 10, 64100 Kristiinankaupunki, Finland',
'',
 'Kaartokatu 9, 70620 Kuopio, Finland',
 'Riistakatu 23, 74120 Iisalmi, Finland',
 'Viestikatu 1-3, 70600 Kuopio, Finland',
 'Savontie 55, 78300 Varkaus, Finland',
 'Harjukatu 48, 15100 Lahti, Finland',
 'Sairaalantie 14, Harjavalta 29200, Finland',
 'Maantiekatu 31, 28120 Pori, Finland',
 'Steniuksenkatu 2, Rauma, Western Finland 26100', 
 'Märyntie 1, 25250 Märynummi, Finland',
 'Kasarmikatu 11-13, 00130 Helsinki, Finland',
 'Seppälänkatu 15-17, 32200 Loimaa, Finland',
 'Sairaalakatu 5, 21200 Raisio, Finland',
 'Sairaalantie 9, 24130 Salo, Finland',
 'Kunnallissairaalantie 20, 20700 Turku, Finland',
 'Kaskenkatu 13, 20700 Turku, Finland',
 'Terveystie 2, 23500 Uusikaupunki, Finland',
 'Niuvankuja 65, 70240 Kuopio, Finland',
 '',
 'Vierinkiventie 1, 65380 Vaasa, Finland',
 '',
 'Laivurinkatu 29, 00150 Helsinki, Finland',
 'Saukonpaadenranta 2, 00180 Helsinki, Finland',
 'Sairaalantie 11, 42100 Jämsä, Finland',
 'Kylpyläntie 19, 02700 Kauniainen, Finland',
 'Raumantie 1 a, 00350 Helsinki, Finland',
 'Hatanpään valtatie 1, 33100 Tampere, Finland',
 'Kiilakivenkuja 1, 90250 Oulu, Finland',
 'Leväsentie 1, 70700 Kuopio, Finland',
 'Yliopistonkatu 29 B, 20100 Turku, Finland',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
 '',
'',
'']

# Create dictionary to map missing addresses
dict_map = {}
for key in list_of_names:
    for value in list_of_addresses:
        dict_map[key] = value
        list_of_addresses.remove(value)
        break
               
# Function to fill missing addresses
def fill_address(row, address_dict):
    if pd.isna(row['address']):
        return address_dict.get(row['name'], row['address'])
    return row['address']

# Apply the function to the DataFrame
df_combined['address'] = df_combined.apply(fill_address, address_dict=dict_map, axis=1)

df_combined = df_combined.dropna(subset=['address'])
df_combined = df_combined[df_combined['address'] != '']
df_combined = df_combined.reset_index(drop=True)

def get_coordinates_openrouteservice(address, api_key):
    try:
        url = f'https://api.openrouteservice.org/geocode/search?api_key={api_key}&text={address}'
        response = requests.get(url)
        response_json = response.json()

        if response.status_code == 200 and 'features' in response_json and len(response_json['features']) > 0:
            location = response_json['features'][0]['geometry']['coordinates']
            address = response_json['features'][0]['geometry']
            return location[1], location[0]
        else:
            return None, None
    except Exception as e:
        print(f"Error geocoding address with OpenRouteService {address}: {e}")
        return None, None


# Apply the get_coordinates function to the DataFrame
df_combined[['latitude', 'longitude']] = df_combined['address'].apply(lambda addr: pd.Series(get_coordinates_openrouteservice(addr, OPENROUTESERVICE_KEY)))

# Manually correct final coordinates

lat_lon_dict = {"Ison Omenan terveysasema" :[60.16033039151025, 24.73804015108906],
"Kontinkankaan terveysasema" : [65.0100829354884, 25.51410301573667],
"Keltakankaan terveysasema" : [60.74663545007424, 26.831775579080073],
"Pohjan terveysasema" : [60.097872892359696, 23.523324241036757]}

# Function to fill latitude and longitude
def fill_lat_lon(row, lat_lon_dict):
    if row['name'] in lat_lon_dict:
        row['latitude'], row['longitude'] = lat_lon_dict[row['name']]
    return row

# Apply the function to the DataFrame
df_combined = df_combined.apply(fill_lat_lon, lat_lon_dict=lat_lon_dict, axis=1)


# Save the combined dataframe to a CSV file
df_combined.to_csv("data/healthcare_locations.csv", index=False)
