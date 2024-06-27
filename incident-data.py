import json
import random
import asyncio
from datetime import datetime
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient

EVENT_HUB_CONNECTION_STR = ""
EVENT_HUB_NAME = ""

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

async def run(incident_data):
    # Create a producer client to send messages to the event hub.
    # Use a global producer client for efficiency.
    global producer
    if not producer:
        producer = EventHubProducerClient.from_connection_string(
            conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
        )
    
    async with producer:
        # Create a batch.
        event_data_batch = await producer.create_batch()

        # Serialize incident_data to JSON and add to the batch.
        event_data_batch.add(EventData(json.dumps(incident_data)))

        # Send the batch of events to the event hub.
        await producer.send_batch(event_data_batch)

# Function to generate real-time data for incidents
async def generate_realtime_incidents():
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
        
        # Send the incident data asynchronously
        await run(incident_data)
        
        # Delay before generating the next incident
        await asyncio.sleep(2 * 60)

# Initialize the global producer client
producer = None

# Test asyncio event loop
if __name__ == '__main__':
    asyncio.run(generate_realtime_incidents())