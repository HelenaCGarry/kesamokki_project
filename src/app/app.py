import streamlit as st
import streamlit.components.v1 as components

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
import re


#graph libraries

import plotly.express as px 
import plotly.io as pio
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# import paramenters

# Load environment variables from the .env file (if present)
load_dotenv()

# Access environment variables 
POSTGRES_USER = os.getenv('PostgreSQL_USERNAME')
POSTGRES_PSW = os.getenv('PostgreSQL_PSW')
POSTGRES_SERVER = os.getenv('PostgreSQL_SERVER')
POSTGRES_PORT = os.getenv('PostgreSQL_PORT')
POSTGRES_DATABASE = os.getenv('PostgreSQL_DATABASE')

#PALETTE = os.getenv('Streamlit_Palette')

# Setting personalised graphs palette
palette = ['#004d39', '#2a9d90', '#6abea6', '#b0c2a8', '#eee3dd']
pio.templates["palette"] = go.layout.Template(
    layout = {
        'title':
            {'font': {'color': '#000000'}
            },
        'font': {'color': '#000000', 'family': 'Roboto'},
        'colorway': palette,
    }
)
pio.templates.default = "palette"

@st.cache_data
def load_data():
    # Import data from table
    db_url = f"postgresql://{POSTGRES_USER}:{POSTGRES_PSW}@{POSTGRES_SERVER}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"

    # Create a SQLAlchemy engine
    engine = create_engine(db_url)

    # Define the table name
    table_name = "cabins_main"

    # Fetch the data into a pandas DataFrame
    df = pd.read_sql_table(table_name, engine)
    return df

@st.cache_data 
def clean_data(df):
    # Remove rows where distance is NaN (they most likely are on an island)
    df.dropna(subset=['distance'], inplace=True)

    # Remove rows where price is NaN 
    df.dropna(subset=['original_price'], inplace=True)

    # Remove rows where price is NaN 
    df.dropna(subset=['original_price'], inplace=True)

    # Remove rows where orginal_price or price is 0
    no_price = ((df['original_price'] < 1000) | (df['price'] < 1000))
    df = df[~no_price]

    # Remove rows missing the surface and number of rooms
    no_rooms_and_surface = ((df['rooms'].isna()) & (df['surface'].isna()))
    df = df[~no_rooms_and_surface]

    # Remove rows where the surface is over 250 m2
    big_surface = ((df['surface'] >= 250))
    df = df[~big_surface]

    def impute_rooms(row):
        if pd.isna(row['rooms']):
            # Find the number of rooms with the closest average surface to the row's surface
            closest_rooms = (average_surface_by_rooms - row['surface']).abs().idxmin()
            return closest_rooms
        return row['rooms']
    
    average_surface_by_rooms = df.groupby('rooms')['surface'].mean()
    df['rooms'] = df.apply(impute_rooms, axis=1)

    # Remove outliers
    lower_bound = df['price'].quantile(0.05)
    upper_bound = df['price'].quantile(0.95)

    filtered_df = df[(df['price'] >= lower_bound) & (df['price'] <= upper_bound)]
    return filtered_df

df = load_data()
filtered_df = clean_data(df)


#intro
st.markdown('<h1 style="text-align: center;"> The kesämökki project </h1>', unsafe_allow_html=True)
st.markdown('<h4 style="text-align: center;"> Finding the right summer cabin is hard, but not impossible </h4>', unsafe_allow_html=True)

st.markdown('This project offers you an analysis of the current Finnish real estate market for summer cabins. The data is updated weekly.')

col1, col2, col3 = st.columns(3)
col1.metric("Median Price", '{:,.2f} €'.format(filtered_df.price.median()), "0 €")
col2.metric("Median Surface", f"{filtered_df.surface.median()} m²")
col3.metric("Median Year of Built", f"{int(filtered_df.year.median())}")



# Plot 1: Distribution of Price
fig1 = px.histogram(filtered_df, x='price', nbins=60, 
                    title="Distribution of Price")

st.plotly_chart(fig1, use_container_width=True)

# Plot 2: Distribution of Surface
fig2 = px.histogram(filtered_df, x='surface', nbins=30, 
                    title="Distribution of Surface")
st.plotly_chart(fig2, use_container_width=True)

# Plot 3: Distribution of Number of Rooms
fig3 = px.histogram(filtered_df, x='rooms', nbins=20, 
                    title="Distribution of Number of Rooms")
st.plotly_chart(fig3, use_container_width=True)


# Plot 4: Proportion of Winterized Properties
winterized_counts = filtered_df['winterized'].value_counts(normalize=True).reset_index()
winterized_counts.columns = ['winterized', 'proportion']

fig4 = px.pie(winterized_counts, names='winterized', values='proportion', 
              title="Proportion of Winterized Properties", 
              labels={'winterized': 'Property Status', 'proportion': 'Proportion'})
st.plotly_chart(fig4, use_container_width=True)

# Plot 5: Distance from HEL

# Function to convert duration to minutes
def duration_to_minutes(duration):
    hours = 0
    minutes = 0
    
    # Extract hours and minutes using regular expressions
    hour_match = re.search(r'(\d+)\s*hour', duration)
    min_match = re.search(r'(\d+)\s*min', duration)
    
    if hour_match:
        hours = int(hour_match.group(1))
    if min_match:
        minutes = int(min_match.group(1))
    
    # Convert to total minutes
    return hours * 60 + minutes

# Apply the conversion
filtered_df['duration_minutes'] = filtered_df['duration'].apply(duration_to_minutes)

# Create a scatter map plot
fig5 = px.scatter_mapbox(filtered_df, lat='latitude', lon='longitude', 
                        color='duration_minutes', 
                        color_continuous_scale='Darkmint',  # You can choose other color scales as well
                        size_max=8, 
                        zoom=4,  # Adjust the zoom level as needed
                        width=800, height=800,
                        center= {'lat':65.5, 'lon':27},
                        mapbox_style="carto-positron",  # You can choose other map styles
                        title="Properties with Driving Duration to HEL Airport")

# Update layout for better visualization
fig5.update_layout(coloraxis_colorbar=dict(title="Duration (mins)"))
st.plotly_chart(fig5, use_container_width=True)



