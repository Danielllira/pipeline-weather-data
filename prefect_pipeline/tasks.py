from prefect import task
from datetime import datetime
import requests
import os
from dotenv import load_dotenv
from google.cloud import storage
import json

# Load environment variables from .env file
load_dotenv()


@task
def fetch_weather_data() -> dict:
    """'
    Fetch weather data from OpenWeather API
    Args:
        None
    Returns:
        dict
    """
    BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
    API_KEY = os.getenv("API_KEY")  
    CITY = "Rio de Janeiro"
    
    if not API_KEY:
        raise ValueError("API_KEY não encontrada. Verifique seu arquivo .env.")

    params = {
        "q": CITY,
        "appid": API_KEY,
        "units": "metric"
    }

    response = requests.get(BASE_URL, params=params)
    
    if response.status_code != 200:
        raise Exception(f"Erro na API: {response.status_code}, {response.text}")
    
    return response.json()


@task
def load_weather_data(weather_data: dict) -> None:
    """
    Load the data into a landing zone (GCS)
    Args:
        weather_data: dict
    Returns:
        None
    """
    try:
        # Initialize a GCS client
        client = storage.Client()

        # Define the bucket name and the destination blob name
        bucket_name = "prefect-weather-data-bucket"
        destination_blob_name = (
            f"weather_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
        )

        # Get the bucket
        bucket = client.bucket(bucket_name)

        # Create a new blob and upload the JSON data
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(
            json.dumps(weather_data), content_type="application/json"
        )

        print(f"Weather data loaded to GCS at {datetime.now()} successfully")
    except Exception as e:
        print(f"Error when trying to load weather data to GCS: {e}")
