import os
import random
import duckdb
import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import pytz

# Set seeds for reproducibility
Faker.seed(0)
np.random.seed(0)
random.seed(43)

# Initialize Faker
fake = Faker()

# Define constants
NUM_ROWS = 100000  # Number of main data rows to generate
DB_PATH = './db/sales.duckdb'  # Path to the DuckDB database

# Time constants
MONTHS_AGO_30 = datetime.now(pytz.utc) - timedelta(days=30 * 30)  
MONTHS_AGO_40 = datetime.now(pytz.utc) - timedelta(days=40 * 30) 
YEARS_IN_FUTURE_10 = datetime.now(pytz.utc) + timedelta(days=10 * 365)  

# Country codes and corresponding time zones
COUNTRY_TIMEZONES = {
    'US': 'America/New_York',
    'GB': 'Europe/London',
    'DE': 'Europe/Berlin',
    'FR': 'Europe/Paris',
    'AU': 'Australia/Sydney',
    'JP': 'Asia/Tokyo',
    'IN': 'Asia/Kolkata',
    'BR': 'America/Sao_Paulo',
    'CA': 'America/Toronto',
    'CN': 'Asia/Shanghai',
}
COUNTRY_CODES = list(COUNTRY_TIMEZONES.keys())

# Ensure the database directory exists
os.makedirs('./db', exist_ok=True)

def generate_random_record(activity_date_utc, activity_country_code=None):
    """
    Generates a single data record using random data
    """
    tenant = random.choice(['acme_industries', 'fake_inc'])

    uuid_value = fake.uuid4()

    # Remove timezone info to make datetime timezone-naive (pandas/duckdb compatibility)
    activity_date_utc_naive = activity_date_utc.replace(tzinfo=None)

    # Determine local time and remove timezone info
    if activity_country_code:
        timezone_str = COUNTRY_TIMEZONES[activity_country_code]
        timezone = pytz.timezone(timezone_str)
        activity_date_local = activity_date_utc.astimezone(timezone)
        activity_date_local_naive = activity_date_local.replace(tzinfo=None)
        activity_location = fake.city()
        activity_location_id = fake.postcode()
        sale_location = activity_location
        sale_store_id = fake.bothify(text='STORE-#####')
    else:
        activity_date_local_naive = activity_date_utc_naive
        activity_location = None
        activity_location_id = None
        sale_location = None
        sale_store_id = None

    # 
    sale_date_local_naive = activity_date_local_naive

    # Generate other sale-related data
    sale_assigned_associate = fake.name()
    sale_product_name = f"{fake.word().capitalize()} {fake.word().capitalize()}"
    sale_price_net = round(random.uniform(10.0, 1000.0), 2)

    # Assemble the record
    record = {
        'exec_time': datetime.utcnow(),  # Timezone-naive UTC datetime
        'tenant': tenant,
        'uuid': uuid_value,
        'activity__date': activity_date_utc_naive,
        'activity__date_local': activity_date_local_naive,
        'activity__country_code': activity_country_code,
        'activity__location': activity_location,
        'activity__location_id': activity_location_id,
        'activity__associate_id': fake.uuid4(),
        'sale__external_id': fake.bothify(text='??#######'),
        'sale__assigned_associate': sale_assigned_associate,
        'sale__assigned_associate_id': fake.uuid4(),
        'sale__channel_type': random.choice(['store', 'online', 'mobile']),
        'sale__item_type': random.choice(['product', 'service', 'subscription', 'shipping']),
        'sale__location': sale_location,
        'sale__store_id': sale_store_id,
        'sale__date_local': sale_date_local_naive,
        'sale__price_net': sale_price_net,
        'sale__price_tax': round(random.uniform(1.0, 200.0), 2),
        'sale__tax_method': random.choice(['vat_excluded', 'vat_included', 'gst']),
        'sale__product_name': sale_product_name,
        'sale__product_sku': fake.bothify(text='SKU-########'),
        'sale__currency_consumer': random.choice(['USD', 'EUR', 'GBP', 'AUD', 'JPY']),
        'sale__order_status': random.choice(['open', 'closed', 'pending', 'cancelled']),
        'sale__item_status': random.choice(['in_stock', 'out_of_stock', 'preorder']),
        'sale__is_historical': random.choice([True, False]),
    }

    return record

def generate_main_data(num_rows):
    """
    Generates the main dataset of random records.
    """
    data = []
    for _ in range(num_rows):
        # Generate a random UTC datetime within the past 30 months
        activity_date_utc = fake.date_time_between(start_date=MONTHS_AGO_30, end_date=datetime.now(pytz.utc), tzinfo=pytz.utc)
        # Randomly select a country code (including None for no country)
        activity_country_code = random.choice(COUNTRY_CODES + [None])
        # Generate the record
        record = generate_random_record(activity_date_utc, activity_country_code)
        data.append(record)
    return data

def generate_additional_records():
    """
    Generates additional records with specific dates:
    - 3 records with a date 40 months in the past
    - 2 records with a date 10 years in the future
    """
    data = []

    # 3 records 40 months in the past
    for _ in range(3):
        activity_date_utc = MONTHS_AGO_40
        activity_country_code = random.choice(COUNTRY_CODES + [None])
        record = generate_random_record(activity_date_utc, activity_country_code)
        data.append(record)

    # 2 records 10 years in the future
    for _ in range(2):
        activity_date_utc = YEARS_IN_FUTURE_10
        activity_country_code = random.choice(COUNTRY_CODES + [None])
        record = generate_random_record(activity_date_utc, activity_country_code)
        data.append(record)

    return data

def main():
    data = generate_main_data(NUM_ROWS)
    additional_data = generate_additional_records()
    data.extend(additional_data)
    # Mix-up data
    random.shuffle(data)
    # Create a DataFrame
    df = pd.DataFrame(data)
    # Convert date fields to appropriate types
    date_columns = ['exec_time', 'activity__date', 'activity__date_local', 'sale__date_local']
    for col in date_columns:
        df[col] = pd.to_datetime(df[col])

    # Create DuckDB connection and insert data
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE TABLE IF NOT EXISTS raw_sales AS SELECT * FROM df")
    con.close()

if __name__ == "__main__":
    main()
