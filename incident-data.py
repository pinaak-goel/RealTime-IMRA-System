import time
import random
from datetime import datetime

# Actual latitude and longitude values for major cities in New York state
city_coordinates = {
    'New York City': (40.7128, -74.0060),
    'Buffalo': (42.8864, -78.8784),
    'Rochester': (43.1566, -77.6088),
    'Syracuse': (43.0481, -76.1474),
    'Albany': (42.6526, -73.7562),
    'Yonkers': (40.9312, -73.8988),
    'White Plains': (41.0339, -73.7629),
    'Ithaca': (42.4430, -76.5019),
    'Binghamton': (42.0987, -75.9179),
    'Utica': (43.1009, -75.2327)
}

# Function to generate real-time data for incidents
def generate_realtime_incidents():
    while True:
        # Generate timestamp
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        
        # Generate random incident data
        city = random.choice(list(city_coordinates.keys()))
        incident_type = random.choice(['Fire', 'Rescue', 'Accident', 'Medical Emergency'])
        latitude, longitude = city_coordinates[city]
        incident_data = {
            'Timestamp': timestamp,
            'City': city,
            'IncidentType': incident_type,
            'Latitude': latitude,
            'Longitude': longitude
        }
        
        yield incident_data
        time.sleep(random.uniform(0.5, 2.0))

# Test generator function
if __name__ == '__main__':
    data_generator = generate_realtime_incidents()
    for _ in range(10):  # Generate 10 data points for demonstration
        incident_data = next(data_generator)
        print("Incident Data:", incident_data)
        print("-" * 50)
