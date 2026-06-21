from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

# Connect to the MongoDB client
client = MongoClient("mongodb://localhost:27017/")

try:
    # 2. Force a connection test by pinging the admin database
    client.admin.command('ping')
    print("MongoDB connection successful!")
    
except ServerSelectionTimeoutError as e:
    print(f"Could not connect to MongoDB: {e}")