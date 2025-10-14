#File to load data to mongo db
import pandas as pd
from pymongo import MongoClient
import mongomock # For local testing without a real DB
import os 
from dotenv import load_dotenv
load_dotenv()


# --- Configuration ---
# Getting MongoDB credentials from environment variables
MONGO_USER =os.getenv("MONGO_USER")

MONGO_PASS = os.getenv("MONGO_PASS")

MONGO_CLUSTER_URL = os.getenv("MONGO_CLUSTER_URL")

CLEANED_CSV_PATH = os.getenv("CLEANED_CSV_PATH")

# MongoDB connection URI
uri = f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}@{MONGO_CLUSTER_URL}/?retryWrites=true&w=majority&appName=CIS3590"




# --- Database Connection -
client = MongoClient(uri) #




#Main program code

if __name__ == "__main__":

    db = client['water_quality_data'] #Database name 
    collection = client['water_quality_data']['asv_1'] # Collection name

    # 2. Load the cleaned CSV file into a pandas DataFrame

    print(f"Reading cleaned data from {CLEANED_CSV_PATH}...") # Print the path being read
    try:
        df = pd.read_csv(CLEANED_CSV_PATH) # Read the CSV file
        
    except FileNotFoundError: # Handle file not found error
        print(f"Error: The file {CLEANED_CSV_PATH} was not found.")
        exit()

    # 3. Prepare data for insertion (convert DataFrame to a list of dictionaries)
    records = df.to_dict('records') # Each row becomes a dictionary
    print(f"Loaded {len(records)} records from the CSV.") # Print number of records loaded

    # 4. Clear existing data and insert the new records
    print(f"Deleting old data from collection '{collection}'...") # Print the collection being cleared
    collection.delete_many({}) # Clears the collection to avoid duplicates
    
    print("Inserting new data...")
    collection.insert_many(records) # Insert new data
    print("Data insertion complete.") # Confirm completion

    # 5. Create an index for performance
    
    print("Creating index on 'timestamp' field...")
    collection.create_index([("timestamp", 1)]) # 1 for ascending order
    collection.create_index([("temperature", 1)])
    print("Indexes created successfully.")

    # 6. Verification
    print("\n--- Verification ---")
    record_count = collection.count_documents({}) # Count documents in the collection
    print(f"Total documents in collection '{collection}': {record_count}") # Print the count
    
    print("Fetching one sample document from the database:") 
    sample_doc = collection.find_one() # Fetch one document
    print(sample_doc) # Print the sample document

    client.close() # Close the connection
    print("\nProcess finished successfully!")