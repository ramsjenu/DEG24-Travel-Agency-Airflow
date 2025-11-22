# RETRIEVING THE DATA FROM THE API
# Import packages and libraries
import requests
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def get_data():
    """
    A function that gets the data from the API
    and then turns the extracted data into a pandas dataframe
    """
    url = "https://restcountries.com/v3.1/all"

    response = requests.get(url)
    logging.info("Fetching data from the API...")

    if response.status_code == 200:
        #Parse JSON response
        data = response.json()

        # Convert JSON data to Pandas DataFrame
        profiles_data = pd.DataFrame(data)
        logging.info(f"Data successsfuly turned into a pandas Dataframe with\
                      {profiles_data.shape[0]}records and {profiles_data.shape[1]} columns ")
        
        return profiles_data
    
print(get_data())
# print(get_data().shape)
# print(get_data().columns)
