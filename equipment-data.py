import json
import time
import random
import asyncio
from datetime import datetime
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient

EVENT_HUB_CONNECTION_STR = ""
EVENT_HUB_NAME = ""

# Define vehicles with pre-assigned IDs and types
equipment_list = [
    {"id": f"FT-{i:03d}", "type": "Fire Truck"} for i in range(1, 21)
] + [
    {"id": f"AM-{i:03d}", "type": "Ambulance"} for i in range(1, 21)
] + [
    {"id": f"LR-{i:03d}", "type": "Ladder Truck"} for i in range(1, 11)
] + [
    {"id": f"RE-{i:03d}", "type": "Rescue Engine"} for i in range(1, 11)
]

# List of statuses
statuses = ["stationed", "moving", "deployed"]

# Stationed locations (fixed coordinates for simplicity)
deployed_locations = [
    {"latitude": 40.7128, "longitude": -74.0060},  # Example: New York City
    {"latitude": 42.6526, "longitude": -73.7562},  # Example: Albany
    {"latitude": 43.0481, "longitude": -76.1474},  # Example: Syracuse
    {"latitude": 40.7306, "longitude": -73.9352},  # Example: Brooklyn
    {"latitude": 40.7891, "longitude": -73.1350}   # Example: Islip
]

# Coordinates for random generation in New York state (approximate ranges)
lat_range_ny = (40.4774, 45.0159)
lon_range_ny = (-79.7624, -71.7517)

async def run(equipment_data):
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
        for data in equipment_data:
            event_data_batch.add(EventData(json.dumps(data)))

        # Send the batch of events to the event hub.
        await producer.send_batch(event_data_batch)

# Generate fixed stationed locations for each vehicle
for equipment in equipment_list:
    equipment["stationed_latitude"] = round(random.uniform(lat_range_ny[0], lat_range_ny[1]), 4)
    equipment["stationed_longitude"] = round(random.uniform(lon_range_ny[0], lon_range_ny[1]), 4)

# Function to generate real-time data for vehicle status and location
async def generate_equipment_status():
    equipment_data = []
    for equipment in equipment_list:
        status = random.choice(statuses)
        if status == "stationed":
            lat = equipment["stationed_latitude"]
            lon = equipment["stationed_longitude"]
        elif status == "moving":
            # Generate multiple entries for moving vehicles over a period of time
            num_entries = random.randint(3, 10)  # Example: Generate 3 to 10 entries for movement
            for _ in range(num_entries):
                lat = round(random.uniform(lat_range_ny[0], lat_range_ny[1]), 4)
                lon = round(random.uniform(lon_range_ny[0], lon_range_ny[1]), 4)
                equipment_data.append({
                    "id": equipment["id"],
                    "type": equipment["type"],
                    "status": status,
                    "latitude": lat,
                    "longitude": lon,
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                })
            continue  # Skip adding the single entry for the end of movement
        
        elif status == "deployed":
            location = random.choice(deployed_locations)
            lat = location["latitude"]
            lon = location["longitude"]
        
        equipment_data.append({
            "id": equipment["id"],
            "type": equipment["type"],
            "status": status,
            "latitude": lat,
            "longitude": lon,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        })
    
    await run(equipment_data)
    await asyncio.sleep(30)

# Initialize the global producer client
producer = None

# Test asyncio event loop
if __name__ == '__main__':
    asyncio.run(generate_equipment_status())