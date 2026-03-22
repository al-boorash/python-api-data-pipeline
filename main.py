import requests
import pandas as pd
from config import API_KEY

def fetch_weather(city):
    # This is the "API Integration" part of the job description
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        # "Data Transformation": Extracting only what we need
        weather_data = {
            "City": data["name"],
            "Temperature": data["main"]["temp"],
            "Description": data["weather"][0]["description"],
            "Humidity": data["main"]["humidity"]
        }
        return weather_data
    else:
        print(f"Error: {response.status_code}")
        return None

# List of cities for our "Scalable Pipeline"
cities = ["Nairobi", "London", "New York"]
results = []

for city in cities:
    data = fetch_weather(city)
    if data:
        results.append(data)

# Save to CSV - The "Deliverable"
df = pd.DataFrame(results)
df.to_csv("weather_report.csv", index=False)
print("Pipeline complete: weather_report.csv created!")
