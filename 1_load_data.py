# 1_load_data.py
import pandas as pd
from sqlalchemy import create_engine

# --- Database Connection ---
db_user = 'root'
db_password = 'rahul' # IMPORTANT: Update with your password
db_host = 'localhost'
db_name = 'marketing_db'

# Create a connection engine
try:
    engine = create_engine(f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}")
    print("Successfully connected to the MySQL database.")
except Exception as e:
    print(f"Error connecting to database: {e}")
    exit()

# --- Load and Clean Data ---
print("Loading CSV file... (This may take a few minutes)")
# Use pd.read_csv for .csv files
df = pd.read_csv('online_retail_II.csv', encoding='latin1')

print("Cleaning data...")
# Remove rows without a Customer ID
df.dropna(subset=['Customer ID'], inplace=True)
# Remove returns (invoices starting with 'C')
df = df[~df['Invoice'].astype(str).str.startswith('C')]
# Ensure Quantity and Price are positive
df = df[(df['Quantity'] > 0) & (df['Price'] > 0)]

# Convert Customer ID to integer and InvoiceDate to datetime
df['Customer ID'] = df['Customer ID'].astype(int)
df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])


# --- Upload to MySQL ---
print(f"Uploading {len(df)} rows to MySQL...")
try:
    # Specify data types for SQL to handle dates correctly
    from sqlalchemy.types import Integer, DateTime, String, Float
    
    df.to_sql(name='transactions', 
              con=engine, 
              if_exists='replace', 
              index=False, 
              chunksize=1000,
              dtype={
                  'Invoice': String(20),
                  'StockCode': String(20),
                  'Description': String(255),
                  'Quantity': Integer,
                  'InvoiceDate': DateTime,
                  'Price': Float,
                  'Customer ID': Integer,
                  'Country': String(100)
              })
    print("Data loaded successfully into the 'transactions' table.")
except Exception as e:
    print(f"Error uploading data to MySQL: {e}")