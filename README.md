# The kesämökki project 🏡 :evergreen_tree:
## *Finding the perfect Finnish summer cabin is hard, but not impossible.* ##
Finding the perfect Finnish summer cabin is hard, but not impossible.
This project scrapes real estate data from a major Finnish real estate aggregator weekly and maintains and updates a database of past and present summer cabins, their price, location, and distance from HEL airport. The data is used to analyse the market and feed a price prediction model. 
The data analysis and machine learning model will be published soon.

### Part 1 - Data Scraping and Transformation
  - Generate a dataset of Finnish healthcare locations from these sources (this part does not need scheduled scraping):
    - https://fi.wikipedia.org/wiki/Luettelo_Suomen_sairaaloista
    - https://fi.wikipedia.org/wiki/Luettelo_Suomen_terveysasemista_ja_terveyskeskusp%C3%A4ivystyksist%C3%A4
  - Generate and automate a script to scrape, transform and upload kesämökki (summer cabin) data from etuovi.com. The script extract general infos about the properties' price and location as well as their surface, number of rooms, and year of construction. Three different APIs (OpenStreetMaps, Google, and OpenRouteService) are used to obtain each properties latitude and longitude as well as their driving distance from Helsinki Vantaa Airport. The data is then saves as a csv as well as uploaded into an SQL database.

### Part 2 - Data Analysis
 - Analyse data
 - Develop Stremlit Dashboard
 
 ### Phase 3 - Price prediction
 - Test and train model (linear regression?)
 - Create MLFlow Server
 - Track and automate training on MlFlow

### Phase 4 - Cloud deployment
