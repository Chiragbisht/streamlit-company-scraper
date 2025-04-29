# Company Contact Scraper

A Streamlit application for extracting company names from PDF files, gathering additional information about those companies using Google Maps API, and scraping contact emails from company websites.

## Features

- **PDF Text Extraction**: Extract text from uploaded PDF files
- **Company Name Extraction**: Use Google's Gemini API to identify and extract company names from the text
- **Google Maps Integration**: Find contact information, addresses, and other details for extracted companies
- **Email Scraping**: Automatically visit company websites and extract contact email addresses
- **CSV Export**: Export results to CSV files for further analysis

## Installation

### Local Setup

1. Clone the repository:
   ```
   git clone https://github.com/Chiragbisht/streamlit-company-scraper.git
   cd streamlit-company-scraper
   ```

2. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

3. Create a `.env` file in the root directory with your API keys:
   ```
   GEMINI_API_KEY="your_gemini_api_key"
   GOOGLE_MAPS_API_KEY="your_google_maps_api_key"
   ```

4. Run the Streamlit app:
   ```
   streamlit run app.py
   ```


