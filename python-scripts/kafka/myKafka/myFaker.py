import random
import json
from faker import Faker
from datetime import datetime, timedelta

# Initialize Faker
fake = Faker()

# Generate random data
def generate_random_data(num_records = 1):
    data = []
    property_types = ['Detached', 'Semi-Detached', 'Terraced', 'Flat/Maisonette', 'Other']
    old_new = ['Y', 'N']
    duration_types = ['Freehold', 'Leasehold']
    ppd_category_types = ['A', 'B']
    
    for _ in range(num_records):
        record = {
            "Transaction_unique_identifier": fake.uuid4(),
            "price": random.randint(50000, 2000000),
            "Date_of_Transfer": fake.date_between(start_date="-10y", end_date="today").strftime("%Y-%m-%d"),
            "postcode": fake.postcode(),
            "Property_Type": random.choice(property_types),
            "Old/New": random.choice(old_new),
            "Duration": random.choice(duration_types),
            "PAON": fake.building_number(),
            "SAON": fake.secondary_address() if random.random() > 0.7 else "",
            "Street": fake.street_name(),
            "Locality": fake.city_suffix(),
            "Town/City": fake.city(),
            "District": fake.city(),
            "County": fake.state(),
            "PPDCategory_Type": random.choice(ppd_category_types),
            "Record_Status - monthly_file_only": random.choice(["A", "C"])
        }
        data.append(record)
    return data

# Number of records to generate
num_records = 1  # Adjust as needed

data = generate_random_data(num_records)

print(data)

