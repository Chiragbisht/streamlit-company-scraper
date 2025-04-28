import os
import csv
import argparse
import sys
import subprocess
import importlib.util
import json
import hashlib
import re

# Function to check and install required packages
def install_required_packages():
    required_packages = {
        "google-generativeai": "google.generativeai",
        "pypdf": "pypdf",
        "pillow": "PIL"
    }
    
    for package, import_name in required_packages.items():
        try:
            importlib.import_module(import_name)
            print(f"{package} is already installed.")
        except ImportError:
            print(f"Installing {package}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            print(f"{package} has been installed.")

# Install required packages
install_required_packages()

# Now import the packages
import google.generativeai as genai
from pypdf import PdfReader
from PIL import Image

# Set up the API key
API_KEY = "AIzaSyBqBw2a7_rRzrw6V8f-abFsxULTREqCmN4"
genai.configure(api_key=API_KEY)

# Cache file to store extracted company names to ensure consistency
CACHE_FILE = "company_cache.json"
# File to store the raw text from PDFs
TEXT_CACHE_FILE = "text_cache.json"
# File to store the companies extracted from each PDF
COMPANY_MAPPING_FILE = "company_mapping.json"

def load_json_file(filename):
    """Load data from a JSON file."""
    if os.path.exists(filename):
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading {filename}: {e}")
    return {}

def save_json_file(data, filename):
    """Save data to a JSON file."""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"Error saving to {filename}: {e}")

def load_cache():
    """Load the cache of already processed PDFs and their company names."""
    return load_json_file(CACHE_FILE)

def save_cache(cache):
    """Save the cache of processed PDFs and their company names."""
    save_json_file(cache, CACHE_FILE)

def load_text_cache():
    """Load the cache of extracted text from PDFs."""
    return load_json_file(TEXT_CACHE_FILE)

def save_text_cache(cache):
    """Save the cache of extracted text from PDFs."""
    save_json_file(cache, TEXT_CACHE_FILE)

def load_company_mapping():
    """Load the mapping of PDFs to company names."""
    return load_json_file(COMPANY_MAPPING_FILE)

def save_company_mapping(mapping):
    """Save the mapping of PDFs to company names."""
    save_json_file(mapping, COMPANY_MAPPING_FILE)

def create_pdf_folder():
    """Create the pdf folder if it doesn't exist."""
    if not os.path.exists("pdf"):
        os.makedirs("pdf")
        print("Created 'pdf' directory. Please place your PDF files in this folder.")
        return False
    return True

def normalize_text(text):
    """Clean and normalize text to ensure consistent extraction."""
    # Replace multiple whitespace characters with a single space
    text = re.sub(r'\s+', ' ', text)
    # Convert to lowercase
    text = text.lower()
    # Replace tabs and newlines with spaces
    text = text.replace('\t', ' ').replace('\n', ' ')
    # Remove special characters
    text = re.sub(r'[^\w\s]', ' ', text)
    # Normalize whitespace again
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def compute_text_hash(text):
    """Compute a hash of the text for caching purposes."""
    return hashlib.md5(text.encode('utf-8')).hexdigest()

def extract_text_from_pdf(pdf_path, text_cache=None):
    """
    Extract text from a PDF file.
    
    Args:
        pdf_path (str): Path to the PDF file
        text_cache (dict): Cache of already extracted text
        
    Returns:
        str: Extracted text from the PDF
    """
    # Generate a unique key for this PDF
    file_stats = os.stat(pdf_path)
    file_size = file_stats.st_size
    file_mtime = file_stats.st_mtime
    pdf_filename = os.path.basename(pdf_path)
    cache_key = f"{pdf_filename}_{file_size}_{file_mtime}"
    
    # Check if we have cached text for this PDF
    if text_cache and cache_key in text_cache:
        print(f"Using cached text for {pdf_filename}")
        return text_cache[cache_key]
    
    try:
        reader = PdfReader(pdf_path)
        text = ""
        for page in reader.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
        
        # Normalize the text
        normalized_text = normalize_text(text)
        
        # Cache the extracted text
        if text_cache is not None:
            text_cache[cache_key] = normalized_text
        
        return normalized_text
    except Exception as e:
        print(f"Error processing PDF {pdf_path}: {e}")
        return ""

def extract_company_names(text, force_api_call=False):
    """
    Extract company names from the text.
    
    Args:
        text (str): Text extracted from the PDF
        force_api_call (bool): Whether to force a new API call even if cached results exist
        
    Returns:
        list: List of potential company names
    """
    # If text is empty, return empty list
    if not text:
        return []
    
    # Compute a hash of the text for caching
    text_hash = compute_text_hash(text)
    
    # Check if we have cached results for this text
    cache = load_cache()
    if text_hash in cache and not force_api_call:
        print(f"Using cached company names for text hash {text_hash[:8]}...")
        return cache[text_hash]
    
    try:
        # Generate content using Gemini model
        model = genai.GenerativeModel('gemini-2.0-flash')
        
        prompt = f"""
        You are a precise information extractor. From the following text, extract ONLY company names.
        
        Guidelines:
        1. Return company names as a comma-separated list
        2. Include only formal business entities
        3. Do not include abbreviations unless they are officially part of the company name
        4. Do not extract product names, unless they are also the company name
        5. Be consistent and deterministic in your extraction
        6. If no company names are found, return only the text 'No company names found'
        
        Text: {text}
        
        Company names (comma-separated):
        """
        
        # Make a deterministic call with temperature=0
        generation_config = {"temperature": 0, "top_p": 1, "top_k": 1}
        response = model.generate_content(prompt, generation_config=generation_config)
        
        result = response.text.strip()
        if result == "No company names found":
            return []
        
        # Clean the results for consistency
        raw_names = [name.strip() for name in result.split(',')]
        
        # Normalize and clean company names
        company_names = []
        for name in raw_names:
            # Skip empty names
            if not name:
                continue
                
            # Remove common legal suffixes for consistency
            cleaned_name = re.sub(r'\s+(Inc\.?|LLC|Ltd\.?|Corp\.?|Corporation|Limited|Company)$', '', name, flags=re.IGNORECASE)
            # Remove quotes if they wrap the entire name
            cleaned_name = re.sub(r'^["\'](.*)["\']$', r'\1', cleaned_name)
            # Trim whitespace
            cleaned_name = cleaned_name.strip()
            
            if cleaned_name and len(cleaned_name) > 1:  # Require at least 2 characters
                company_names.append(cleaned_name)
        
        # Sort alphabetically for consistency
        company_names = sorted(set(company_names))
        
        # Cache the results
        cache[text_hash] = company_names
        save_cache(cache)
        
        return company_names
    except Exception as e:
        print(f"Error extracting company names: {e}")
        return []

def save_to_csv(company_names, csv_file="companylist.csv", pdf_filename=None, is_new_file=False):
    """
    Save company names to a CSV file. Appends to existing file if it exists.
    
    Args:
        company_names (list): List of company names
        csv_file (str): Path to the CSV file
        pdf_filename (str, optional): Name of the PDF file the companies were extracted from
        is_new_file (bool): If True, create a new file instead of appending
    """
    try:
        # Get existing companies to avoid duplicates if appending
        existing_companies = set()
        if os.path.isfile(csv_file) and not is_new_file:
            try:
                with open(csv_file, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    next(reader)  # Skip header
                    for row in reader:
                        if row:  # Ensure row is not empty
                            existing_companies.add(row[0])
            except Exception as e:
                print(f"Warning: Could not read existing CSV: {e}")
        
        # Filter out duplicates
        unique_companies = [c for c in company_names if c not in existing_companies]
        
        # Decide whether to create new file or append
        mode = 'w' if is_new_file else 'a'
        file_exists = os.path.isfile(csv_file) and not is_new_file
        
        with open(csv_file, mode, newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Write header if new file
            if not file_exists or is_new_file:
                if pdf_filename:
                    writer.writerow(['Company Name', 'Source PDF'])
                else:
                    writer.writerow(['Company Name'])
            
            # Write company names, appending to existing entries
            for company in unique_companies:
                if pdf_filename:
                    writer.writerow([company, pdf_filename])
                else:
                    writer.writerow([company])
        
        if unique_companies:
            print(f"{len(unique_companies)} new company names {'saved to' if is_new_file else 'appended to'} {csv_file}")
        else:
            print(f"No new companies found to add to {csv_file}")
            
    except Exception as e:
        print(f"Error saving to CSV: {e}")

def process_pdf(pdf_path, csv_file="companylist.csv", is_first_pdf=False):
    """
    Process a single PDF and extract company names to CSV.
    
    Args:
        pdf_path (str): Path to the PDF file
        csv_file (str): Path to the CSV file
        is_first_pdf (bool): Whether this is the first PDF being processed
    
    Returns:
        list: List of company names extracted
    """
    pdf_filename = os.path.basename(pdf_path)
    print(f"Processing PDF: {pdf_filename}")
    
    # Load the cached extracted text
    text_cache = load_text_cache()
    
    # Load the mapping of PDFs to company names
    company_mapping = load_company_mapping()
    
    # Generate a unique key for this PDF
    file_stats = os.stat(pdf_path)
    file_size = file_stats.st_size
    file_mtime = file_stats.st_mtime
    cache_key = f"{pdf_filename}_{file_size}_{file_mtime}"
    
    # Check if we have already processed this PDF
    if cache_key in company_mapping:
        print(f"Using cached company mapping for {pdf_filename}")
        company_names = company_mapping[cache_key]
    else:
        # Extract text from the PDF
        extracted_text = extract_text_from_pdf(pdf_path, text_cache)
        if not extracted_text:
            print("No text extracted from the PDF.")
            return []
        
        # Log the text length for debugging
        print(f"Extracted {len(extracted_text)} characters of text from PDF")
        
        # Extract company names from the text
        company_names = extract_company_names(extracted_text)
        if not company_names:
            print("No company names found in the extracted text.")
            return []
        
        # Store the extracted company names in the mapping
        company_mapping[cache_key] = company_names
        save_company_mapping(company_mapping)
        
        # Save the updated text cache
        save_text_cache(text_cache)
        
        print(f"Found {len(company_names)} company names: {', '.join(company_names[:5])}{'...' if len(company_names) > 5 else ''}")
    
    # Save to CSV, appending to existing file unless this is the first PDF
    save_to_csv(company_names, csv_file, pdf_filename, is_new_file=is_first_pdf)
    
    return company_names

def process_pdf_directory(directory_path="pdf", csv_file="companylist.csv", force_reprocess=False):
    """
    Process all PDFs in the specified directory.
    Each PDF's company names will be appended to the CSV file.
    
    Args:
        directory_path (str): Path to the directory with PDFs
        csv_file (str): Path to the CSV file
        force_reprocess (bool): Whether to force reprocessing of all PDFs
    """
    if not os.path.exists(directory_path):
        print(f"Directory '{directory_path}' does not exist. Creating it now.")
        os.makedirs(directory_path)
        print(f"Please place your PDF files in the '{directory_path}' directory and run the script again.")
        return
    
    pdf_files = [f for f in os.listdir(directory_path) if f.lower().endswith('.pdf')]
    
    if not pdf_files:
        print(f"No PDF files found in the '{directory_path}' directory.")
        return
    
    # If forcing reprocessing, clear the cached data
    if force_reprocess:
        print("Forcing reprocessing of all PDFs - clearing caches")
        save_text_cache({})
        save_company_mapping({})
        if os.path.exists(csv_file):
            os.remove(csv_file)
            print(f"Removed existing CSV file: {csv_file}")
    
    processed = 0
    all_companies = []
    
    # Process each PDF file
    for i, filename in enumerate(pdf_files):
        file_path = os.path.join(directory_path, filename)
        companies = process_pdf(file_path, csv_file, is_first_pdf=(i==0))
        all_companies.extend(companies)
        processed += 1
    
    print(f"Processed {processed} PDFs from directory: {directory_path}")
    print(f"Found a total of {len(all_companies)} company names across all PDFs")
    print(f"All company names have been saved to {csv_file}")

def main():
    """
    Main function to execute the script.
    """
    # Process all PDFs in the pdf directory
    # Set force_reprocess=True to clear caches and start fresh
    process_pdf_directory(force_reprocess=False)

if __name__ == "__main__":
    main() 