import os
import csv
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
from bs4 import BeautifulSoup

# Load environment variables from .env file
load_dotenv()

# Define Google API Key from environment variable
GOOGLE_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

# Create a session with retry logic
def create_retry_session(retries=3, backoff_factor=0.3):
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def verify_phone_number(phone):
    """
    Verify if a phone number has the minimum expected format
    
    Args:
        phone (str): Phone number to verify
        
    Returns:
        bool: True if valid, False otherwise
    """
    # Basic validation: Check if phone has at least 7 digits
    digits = ''.join(c for c in phone if c.isdigit())
    return len(digits) >= 7

def get_google_maps_info(company_name, city="", country=""):
    """
    Get company information from Google Maps API
    
    Args:
        company_name (str): Name of the company
        city (str): City where the company is located (optional)
        country (str): Country where the company is located (optional)
        
    Returns:
        tuple: (phone, website, status_message)
    """
    # First try searching in India
    phone, website, status = search_in_location(company_name, "", "India")
    
    # If not found in India, try with original location or global search
    if "not found" in status or not phone and not website:
        if country and country.lower() != "india":
            # Try with the provided country
            phone, website, status = search_in_location(company_name, city, country)
        else:
            # Try global search (no location bias)
            phone, website, status = search_in_location(company_name, "", "")
    
    return phone, website, status

def search_in_location(company_name, city="", country=""):
    """
    Search for company in a specific location
    
    Args:
        company_name (str): Name of the company
        city (str): City where the company is located (optional)
        country (str): Country where the company is located (optional)
        
    Returns:
        tuple: (phone, website, status_message)
    """
    # Retry mechanism for SSL issues
    max_retries = 3
    for attempt in range(max_retries):
        try:
            full_query = f"{company_name}"
            if city:
                full_query += f", {city}"
            if country:
                full_query += f", {country}"
                
            # Use a session with retry logic
            session = create_retry_session()
                
            url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
            params = {
                "input": full_query,
                "inputtype": "textquery",
                "fields": "place_id",
                "key": GOOGLE_API_KEY
            }
            
            # Add locationbias for India if specified
            if country and country.lower() == "india":
                # Rough center of India with a radius covering most of the country
                params["locationbias"] = "circle:1000000@20.5937,78.9629"
            
            response = session.get(url, params=params, timeout=10)
            data = response.json()

            if response.status_code != 200 or "error_message" in data:
                return "", "", f"Google Maps error: {data.get('error_message', response.text)}"

            candidates = data.get("candidates", [])
            if not candidates:
                return "", "", f"Place not found in {country if country else 'global search'}"

            place_id = candidates[0]["place_id"]

            details_url = "https://maps.googleapis.com/maps/api/place/details/json"
            details_params = {
                "place_id": place_id,
                "fields": "formatted_phone_number,website,formatted_address",
                "key": GOOGLE_API_KEY
            }
            details_resp = session.get(details_url, params=details_params, timeout=10)
            details_data = details_resp.json()

            if details_resp.status_code != 200 or "error_message" in details_data:
                return "", "", f"Details error: {details_data.get('error_message', details_resp.text)}"

            result = details_data.get("result", {})
            phone = result.get("formatted_phone_number", "")
            if phone and not verify_phone_number(phone):
                phone = ""
            website = result.get("website", "")
            address = result.get("formatted_address", "")

            location_text = ""
            if country:
                location_text = f" in {country}"
            if address:
                location_text = f" ({address})"

            return phone, website, f"Google Maps Success{location_text}"
        
        except requests.exceptions.SSLError as ssl_error:
            if attempt < max_retries - 1:
                # Wait before retrying (exponential backoff)
                wait_time = 2 ** attempt
                time.sleep(wait_time)
                continue
            return "", "", f"SSL Error after {max_retries} attempts: {ssl_error}"
            
        except requests.exceptions.ConnectionError as conn_error:
            if attempt < max_retries - 1:
                # Wait before retrying (exponential backoff)
                wait_time = 2 ** attempt
                time.sleep(wait_time)
                continue
            return "", "", f"Connection Error after {max_retries} attempts: {conn_error}"
            
        except requests.exceptions.Timeout as timeout_error:
            if attempt < max_retries - 1:
                # Wait before retrying (exponential backoff)
                wait_time = 2 ** attempt
                time.sleep(wait_time)
                continue
            return "", "", f"Timeout Error after {max_retries} attempts: {timeout_error}"
            
        except Exception as e:
            return "", "", f"Google Maps exception: {e}"

def get_company_details(company_names, status_callback=None):
    """
    Get company details using Google Maps API
    
    Args:
        company_names (list): List of company names
        status_callback (function): Callback function to report status updates
        
    Returns:
        tuple: (company_details_dict, db_companies_list)
            - company_details_dict: Dictionary with company details
            - db_companies_list: List of company names that were found in the database
    """
    # Import MongoDB utils (importing here to avoid circular import)
    from mongodb_utils import get_company_details_from_mongodb
    
    # First, check if company details already exist in MongoDB
    # Don't show database references in status messages
    
    # Get existing company details from MongoDB
    existing_company_details = get_company_details_from_mongodb(company_names)
    db_companies_count = len(existing_company_details)
    
    # List of company names that were found in the database
    db_companies_list = list(existing_company_details.keys())
    
    # Create empty dictionary to store company details
    company_details = existing_company_details.copy()
    
    # Get the list of companies that don't have details in MongoDB
    companies_to_scrape = [name for name in company_names if name not in existing_company_details]
    
    if status_callback:
        if existing_company_details:
            # Don't show database references in status messages
            pass
    
    # If all companies were found in MongoDB, return early
    if not companies_to_scrape:
        return company_details, db_companies_list
    
    # Loop through each company name that needs to be scraped
    for i, company_name in enumerate(companies_to_scrape):
        # Report progress
        if status_callback:
            status_callback(f"Searching for details for: {company_name} ({i+1}/{len(companies_to_scrape)})")
        
        try:
            # Get company information from Google Maps
            phone, website, status = get_google_maps_info(company_name)
            
            # Store emails as empty list for now - this API only provides phone and website
            emails = []
            phones = [phone] if phone else []
            
            # Extract address from status if available
            address = ""
            if "(" in status and ")" in status:
                address_part = status[status.find("(")+1:status.find(")")]
                if address_part:
                    address = address_part
            
            # Log the result
            if status_callback:
                if "Success" in status:
                    status_callback(f"Found details for {company_name}: {phone} / {website}")
                else:
                    status_callback(f"Issue with {company_name}: {status}")
            
            # Store the results
            company_details[company_name] = {
                "emails": emails,
                "phones": phones,
                "website": website,
                "address": address
            }
            
            # Add a delay to avoid hitting rate limits
            time.sleep(1.5)
            
        except Exception as e:
            if status_callback:
                status_callback(f"Error getting details for {company_name}: {str(e)}")
            company_details[company_name] = {
                "emails": [],
                "phones": [],
                "website": "",
                "address": ""
            }
    
    return company_details, db_companies_list

def save_company_details_to_csv(company_details, csv_path):
    """
    Save company details to CSV
    
    Args:
        company_details (dict): Dictionary with company details
        csv_path (str): Path to save the CSV file
    """
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Company Name', 'Emails', 'Phone Numbers', 'Website', 'Address'])
        
        for company_name, details in company_details.items():
            writer.writerow([
                company_name, 
                '; '.join(details.get('emails', [])), 
                '; '.join(details.get('phones', [])),
                details.get('website', ''),
                details.get('address', '')
            ])
    
    return csv_path 