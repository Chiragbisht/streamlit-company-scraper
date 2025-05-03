import os
import sys
import pymongo
from dotenv import load_dotenv
import pandas as pd
import traceback
from datetime import datetime

# Load environment variables
load_dotenv()

# Get MongoDB connection string from environment variables
MONGODB_URI = os.getenv("MONGODB_URI")
print(f"MongoDB URI found: {'Yes' if MONGODB_URI else 'No'}")

def get_mongodb_connection():
    """
    Connect to MongoDB database
    
    Returns:
        pymongo.database.Database: MongoDB database connection
    """
    try:
        # Connect with explicit database name specification
        print(f"Attempting to connect to MongoDB with URI: {MONGODB_URI[:20]}...")
        client = pymongo.MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        
        # Use 'companyscraper' database explicitly
        db = client['companyscraper']
        
        # Test the connection
        client.admin.command('ping')
        print("MongoDB connection successful!")
        
        # Print available collections
        collections = db.list_collection_names()
        print(f"Available collections: {collections}")
        
        return db
    except pymongo.errors.ServerSelectionTimeoutError as e:
        print(f"MongoDB connection timed out: {e}")
        print(f"Please check your connection string and network connectivity.")
        return None
    except pymongo.errors.ConfigurationError as e:
        print(f"MongoDB configuration error: {e}")
        print(f"Please check your MongoDB URI format.")
        return None
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        print(traceback.format_exc())
        return None

def register_user(user_name, email="", role="user"):
    """
    Register a new user or update an existing user
    
    Args:
        user_name (str): Name of the user
        email (str, optional): Email of the user
        role (str, optional): Role of the user (default: user)
        
    Returns:
        str: User ID if successful, None otherwise
    """
    try:
        print(f"Registering/updating user: {user_name}")
        
        # Connect to MongoDB
        db = get_mongodb_connection()
        if db is None:
            print("MongoDB connection failed. Cannot proceed.")
            return None
        
        # Get the users collection
        users_collection = db["users"]
        print(f"Connected to users collection")
        
        # Check if user already exists
        existing_user = users_collection.find_one({"user_name": user_name})
        
        if existing_user:
            # Update existing user
            user_id = existing_user["_id"]
            users_collection.update_one(
                {"_id": user_id},
                {
                    "$set": {
                        "last_login": datetime.now(),
                        "email": email if email else existing_user.get("email", ""),
                        "role": role if role != "user" else existing_user.get("role", "user")
                    }
                }
            )
            print(f"Updated existing user: {user_name}")
            return str(user_id)
        else:
            # Create new user
            user_doc = {
                "user_name": user_name,
                "email": email,
                "role": role,
                "created_at": datetime.now(),
                "last_login": datetime.now()
            }
            result = users_collection.insert_one(user_doc)
            user_id = result.inserted_id
            print(f"Created new user {user_name} with ID: {user_id}")
            return str(user_id)
            
    except Exception as e:
        print(f"Error registering user: {e}")
        print(traceback.format_exc())
        return None

def get_user_extractions(user_name):
    """
    Get all extractions performed by a user
    
    Args:
        user_name (str): Name of the user
        
    Returns:
        dict: Dictionary with companies and company_details extractions
    """
    try:
        print(f"Getting extractions for user: {user_name}")
        
        # Connect to MongoDB
        db = get_mongodb_connection()
        if db is None:
            print("MongoDB connection failed. Cannot proceed.")
            return None
        
        # Get company names extracted by the user
        companies_collection = db["companies"]
        companies = list(companies_collection.find({"extracted_by": user_name}))
        
        # Get company details extracted by the user
        details_collection = db["company_details"]
        company_details = list(details_collection.find({"extracted_by": user_name}))
        
        # Convert ObjectId to string for JSON serialization
        for doc in companies + company_details:
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])
        
        print(f"Found {len(companies)} company extractions and {len(company_details)} company details extractions")
        
        return {
            "companies": companies,
            "company_details": company_details
        }
            
    except Exception as e:
        print(f"Error getting user extractions: {e}")
        print(traceback.format_exc())
        return None

def save_companies_to_mongodb(csv_path, user_name):
    """
    Save company names from CSV to MongoDB
    
    Args:
        csv_path (str): Path to the CSV file with company names
        user_name (str): Name of the user who extracted the companies
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        print(f"Starting save_companies_to_mongodb for user: {user_name}")
        print(f"CSV path: {csv_path}")
        
        # Register/update user first
        user_id = register_user(user_name)
        if not user_id:
            print("Failed to register/update user. Continuing anyway...")
        
        # Connect to MongoDB
        db = get_mongodb_connection()
        if db is None:
            print("MongoDB connection failed. Cannot proceed.")
            return False
        
        # Get the companies collection
        companies_collection = db["companies"]
        print(f"Connected to companies collection")
        
        # Read the CSV file
        if not os.path.exists(csv_path):
            print(f"CSV file not found at path: {csv_path}")
            return False
            
        df = pd.read_csv(csv_path)
        print(f"Successfully read CSV file with {len(df)} rows")
        
        # Prepare documents to insert
        documents = []
        for _, row in df.iterrows():
            document = {
                "company_name": row["Company Name"],
                "extracted_by": user_name,
                "user_id": user_id,
                "source_pdf": row.get("Source PDF", ""),
                "extraction_type": "company_names",
                "created_at": datetime.now()
            }
            documents.append(document)
        
        print(f"Prepared {len(documents)} documents for MongoDB insertion")
        
        # Insert the documents
        if documents:
            result = companies_collection.insert_many(documents)
            print(f"Added {len(result.inserted_ids)} companies to MongoDB")
            return True
        else:
            print("No companies to add to MongoDB")
            return False
            
    except Exception as e:
        print(f"Error saving companies to MongoDB: {e}")
        print(traceback.format_exc())
        return False

def save_company_details_to_mongodb(csv_path, user_name):
    """
    Save company details from CSV to MongoDB
    
    Args:
        csv_path (str): Path to the CSV file with company details
        user_name (str): Name of the user who extracted the company details
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        print(f"Starting save_company_details_to_mongodb for user: {user_name}")
        print(f"CSV path: {csv_path}")
        
        # Register/update user first
        user_id = register_user(user_name)
        if not user_id:
            print("Failed to register/update user. Continuing anyway...")
        
        # Connect to MongoDB
        db = get_mongodb_connection()
        if db is None:
            print("MongoDB connection failed. Cannot proceed.")
            return False
        
        # Get the company_details collection
        details_collection = db["company_details"]
        print(f"Connected to company_details collection")
        
        # Read the CSV file
        if not os.path.exists(csv_path):
            print(f"CSV file not found at path: {csv_path}")
            return False
            
        df = pd.read_csv(csv_path)
        print(f"Successfully read CSV file with {len(df)} rows")
        
        # Prepare documents to insert
        documents = []
        for _, row in df.iterrows():
            # Parse emails and phones from semicolon-separated strings
            emails = [email.strip() for email in row["Emails"].split(';') if email.strip()] if not pd.isna(row["Emails"]) else []
            phones = [phone.strip() for phone in row["Phone Numbers"].split(';') if phone.strip()] if not pd.isna(row["Phone Numbers"]) else []
            
            document = {
                "company_name": row["Company Name"],
                "emails": emails,
                "phones": phones,
                "website": row["Website"] if not pd.isna(row["Website"]) else "",
                "address": row["Address"] if not pd.isna(row["Address"]) else "",
                "extracted_by": user_name,
                "user_id": user_id,
                "extraction_type": "company_details",
                "created_at": datetime.now()
            }
            documents.append(document)
        
        print(f"Prepared {len(documents)} documents for MongoDB insertion")
        
        # Insert the documents
        if documents:
            result = details_collection.insert_many(documents)
            print(f"Added {len(result.inserted_ids)} company details to MongoDB")
            return True
        else:
            print("No company details to add to MongoDB")
            return False
            
    except Exception as e:
        print(f"Error saving company details to MongoDB: {e}")
        print(traceback.format_exc())
        return False

def get_company_details_from_mongodb(company_names):
    """
    Get company details from MongoDB for a list of company names
    
    Args:
        company_names (list): List of company names
        
    Returns:
        dict: Dictionary with company details for the companies found in the database,
              with company names as keys
    """
    try:
        # Don't log database operations
        
        # Connect to MongoDB
        db = get_mongodb_connection()
        if db is None:
            print("Connection failed. Cannot proceed.")
            return {}
        
        # Get the company_details collection
        details_collection = db["company_details"]
        
        # Initialize the results dictionary
        company_details = {}
        
        # Find company details for each company name
        for company_name in company_names:
            # Query MongoDB for company details
            document = details_collection.find_one({"company_name": company_name})
            
            if document:
                # If company details exist in MongoDB, format and add to results
                if "_id" in document:
                    document["_id"] = str(document["_id"])
                
                # Extract the relevant details
                emails = document.get("emails", [])
                phones = document.get("phones", [])
                website = document.get("website", "")
                address = document.get("address", "")
                
                # Store in results dictionary
                company_details[company_name] = {
                    "emails": emails,
                    "phones": phones,
                    "website": website,
                    "address": address
                }
                
                # Don't log data completeness or database operations
        
        # Don't log special messages about database operations
        
        return company_details
            
    except Exception as e:
        print(f"Error getting company details: {e}")
        print(traceback.format_exc())
        return {}

# Run a test connection if this file is executed directly
if __name__ == "__main__":
    print("Testing MongoDB connection...")
    db = get_mongodb_connection()
    if db is not None:
        print("MongoDB connection test successful!")
        # Insert a test document to verify write permissions
        try:
            test_collection = db["test_collection"]
            result = test_collection.insert_one({"test": "Connection test", "timestamp": pd.Timestamp.now()})
            print(f"Test document inserted with ID: {result.inserted_id}")
            test_collection.delete_one({"_id": result.inserted_id})
            print("Test document deleted")
        except Exception as e:
            print(f"Error during write test: {e}")
    else:
        print("MongoDB connection test failed!") 