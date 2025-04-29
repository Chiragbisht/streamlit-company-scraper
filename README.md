# Company Information Extractor

A Streamlit application that extracts company names from PDF documents, retrieves contact details via Google Maps API, and scrapes emails from company websites.

## Features

- **PDF Processing**: Extract company names from uploaded PDF documents
- **Company Details**: Fetch company contact information from Google Maps API
- **Email Scraping**: Automatically scrape email addresses from company websites
- **Interactive UI**: Edit extracted company names before processing
- **Customizable Processing**: Choose how many companies to process (10, 25, 50, 100, 200, or All)
- **Results Export**: Download extracted data as CSV files

## Installation

### Prerequisites

- Python 3.7 or higher
- Chrome browser (for email scraping)

### Setup

1. Clone this repository:
   ```
   git clone https://github.com/Chiragbisht/streamlit-company-scraper.git
   cd streamlit-company-scraper
   
   ```

2. Install required packages:
   ```
   pip install -r requirements.txt
   ```

3. Create a `.env` file in the project root and add your API keys:
   ```
   GEMINI_API_KEY="your-gemini-api-key"
   GOOGLE_MAPS_API_KEY="your-google-maps-api-key"
   ```


## Usage

1. Start the Streamlit application:
   ```
   streamlit run app.py
   ```
