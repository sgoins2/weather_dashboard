import os
import json
import boto3
import requests
from datetime import datetime
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# Load environment variables
load_dotenv()

class WeatherDashboard:
    def __init__(self):
        self.api_key = os.getenv('OPENWEATHER_API_KEY')
        self.bucket_name = os.getenv('AWS_BUCKET_NAME')
        
        if not self.api_key:
            raise ValueError("Environment variable 'OPENWEATHER_API_KEY' is not set.")
        if not self.bucket_name:
            raise ValueError("Environment variable 'AWS_BUCKET_NAME' is not set.")
        
        self.s3_client = boto3.client('s3')

    def create_bucket_if_not_exists(self):
        """Create S3 bucket if it doesn't exist"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            print(f"Bucket '{self.bucket_name}' exists.")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                print(f"Bucket '{self.bucket_name}' does not exist. Creating it now...")
                try:
                    region = boto3.session.Session().region_name or 'us-east-1'
                    if region == 'us-east-1':
                        self.s3_client.create_bucket(Bucket=self.bucket_name)
                    else:
                        self.s3_client.create_bucket(
                            Bucket=self.bucket_name,
                            CreateBucketConfiguration={'LocationConstraint': region}
                        )
                    print(f"Bucket '{self.bucket_name}' created successfully.")
                except ClientError as create_error:
                    print(f"Error creating bucket: {create_error}")
            else:
                print(f"Error accessing bucket: {e}")

    def fetch_weather(self, city):
        """Fetch weather data from OpenWeather API"""
        base_url = "http://api.openweathermap.org/data/2.5/weather"
        params = {
            "q": city,
            "appid": self.api_key,
            "units": "imperial"
        }
        
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching weather data for {city}: {e}")
            return None

    def save_to_s3(self, weather_data, city):
        """Save weather data to S3 bucket"""
        if not weather_data:
            print(f"No weather data to save for {city}.")
            return False
            
        timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        file_name = f"weather-data/{city}-{timestamp}.json"
        
        try:
            weather_data['timestamp'] = timestamp
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=file_name,
                Body=json.dumps(weather_data),
                ContentType='application/json'
            )
            print(f"Weather data for {city} saved to S3 as '{file_name}'.")
            return True
        except ClientError as e:
            print(f"Error saving weather data to S3: {e}")
            return False

def main():
    dashboard = WeatherDashboard()
    
    # Create bucket if needed
    dashboard.create_bucket_if_not_exists()
    
    cities = ["Atlanta", "San Diego", "Bahia"]
    
    for city in cities:
        print(f"\nFetching weather data for {city}...")
        weather_data = dashboard.fetch_weather(city)
        
        if weather_data:
            try:
                main_info = weather_data['main']
                weather_description = weather_data['weather'][0]['description']
                
                print(f"Temperature: {main_info['temp']}°F")
                print(f"Feels like: {main_info['feels_like']}°F")
                print(f"Humidity: {main_info['humidity']}%")
                print(f"Conditions: {weather_description}")
            except (KeyError, IndexError):
                print(f"Unexpected response structure for {city}: {weather_data}")
                continue
            
            # Save data to S3
            dashboard.save_to_s3(weather_data, city)
        else:
            print(f"Failed to fetch weather data for {city}.")

if __name__ == "__main__":
    main()
