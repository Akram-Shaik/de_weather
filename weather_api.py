import requests
import logging
import pandas as pd

def get_weather_data(lat, lon):
    """
    Purpose: Fetches live temperature from the Open-Meteo API.
    Term: API Endpoint Consumption.
    """
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true"
    
    try:
        response = requests.get(url)
        # Check if the "Handshake" with the server was successful (HTTP 200)
        response.raise_for_status() 
        
        # Convert the raw string from the server into a Python Dictionary
        data = response.json() 
        
        # Accessing the "Nested" data (JSON Parsing)
        temp = data['current_weather']['temperature']
        return temp
        
    except Exception as e:
        logging.error(f"☁️ Weather API Error: {e}")
        return None
    
if __name__ == "__main__":
        print(f"The temperature in London is: {get_weather_data(51.5, -0.12)}°C")