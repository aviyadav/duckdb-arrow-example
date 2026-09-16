import duckdb
from faker import Faker

DB_FILE = "demo.duckdb"
TABLE_NAME = "customers"
NUM_RECORDS = 1000

def main():
    fake = Faker()
    con = duckdb.connect(DB_FILE)
    
    # Create table
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER,
            first_name VARCHAR,
            last_name VARCHAR,
            email VARCHAR,
            phone VARCHAR,
            credit_card VARCHAR,
            ssn VARCHAR,
            created_at TIMESTAMP
        )
    """)
    
    # Clear existing data
    con.execute(f"DELETE FROM {TABLE_NAME}")
    print(f"Generating {NUM_RECORDS} fake customer records...")
    
    # Generate and insert data
    data = [
        (
            i, fake.first_name(), fake.last_name(), fake.email(),
            fake.phone_number(), fake.credit_card_number(),
            fake.ssn(), fake.date_time_this_decade()
        )
        for i in range(1, NUM_RECORDS + 1)
    ]
        
    con.executemany(f"INSERT INTO {TABLE_NAME} VALUES (?, ?, ?, ?, ?, ?, ?, ?)", data)
    print(f"Successfully inserted {NUM_RECORDS} records into {TABLE_NAME}.")
    con.close()

if __name__ == "__main__":
    main()