#!/usr/bin/env python
"""
Company Contact Information Scraper

This script uses Scrapy to scrape company email addresses and phone numbers 
from company websites and social media profiles.

Requirements:
    - scrapy>=2.7.0
    - google-generativeai>=0.3.0
    - pypdf>=3.15.0
    - pillow>=9.5.0

Install requirements with:
    pip install scrapy google-generativeai pypdf pillow

Usage:
    1. Ensure 'companylist.csv' exists with company names
    2. Run: python company_scraper.py
    3. Results will be saved to 'company_contacts.csv'

Notes:
    - Uses polite crawling with delays between requests
    - Rotates user agents to avoid blocking
    - Checks LinkedIn, IndiaMart, and Facebook if info not found on company website
"""

import re
import csv
import random
import time
import logging
import sys
import subprocess
import os
from urllib.parse import urlparse, urljoin

# Check and install required packages
def check_install_requirements():
    """Check and install required packages if not present."""
    required_packages = ['scrapy']
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"{package} is already installed.")
        except ImportError:
            print(f"Installing {package}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            print(f"{package} has been installed.")

# Install required packages if not already installed
check_install_requirements()

# Now import scrapy-related packages
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.utils.project import get_project_settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler("scraper.log"), logging.StreamHandler()]
)

# Regular expressions for extracting contact information
EMAIL_PATTERN = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
PHONE_PATTERN = r'(?:\+\d{1,3}[- ]?)?\(?(?:\d{3})?\)?[- ]?\d{3}[- ]?\d{4}'
INDIA_PHONE_PATTERN = r'(?:\+91|0)?[6-9]\d{9}'

# List of common contact page URLs
CONTACT_URLS = [
    'contact', 'contact-us', 'contactus', 'about', 'about-us', 'aboutus', 
    'reach-us', 'reachus', 'get-in-touch', 'getintouch', 'contact.html',
    'contact.php', 'contact.aspx', 'contact-us.html', 'about-us.html'
]

# List of user agents for rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1'
]

# Add Google Gemini API integration
import google.generativeai as genai
from concurrent.futures import ThreadPoolExecutor

# API key for Gemini
GEMINI_API_KEY = "AIzaSyBqBw2a7_rRzrw6V8f-abFsxULTREqCmN4"  # Using the API key from scraper.py

# Initialize Google Generativeai
genai.configure(api_key=GEMINI_API_KEY)

# Regular expressions for extracting contact information
# Standard email pattern
EMAIL_PATTERN = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
# Obfuscated email patterns - handles [at], (at), [@], at, etc.
OBFUSCATED_EMAIL_PATTERN = r'[a-zA-Z0-9._%+-]+\s*(?:\[at\]|\(at\)|[@]|@|\s+at\s+)\s*[a-zA-Z0-9.-]+\s*(?:\[dot\]|\(dot\)|[.]|\s+dot\s+)\s*[a-zA-Z]{2,}'
# Combined email pattern for both standard and obfuscated
COMBINED_EMAIL_PATTERN = r'(?:[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})|(?:[a-zA-Z0-9._%+-]+\s*(?:\[at\]|\(at\)|[@]|@|\s*at\s*)\s*[a-zA-Z0-9.-]+\s*(?:\[dot\]|\(dot\)|[.]|\s*dot\s*)\s*[a-zA-Z]{2,})'

# Phone patterns
# Indian mobile pattern: +91 or 0 followed by a number starting with 6-9 and 9 more digits
INDIA_PHONE_PATTERN = r'(?:\+91[\-\s]?|0)?[6-9]\d{9}'
# Indian landline pattern
INDIA_LANDLINE_PATTERN = r'(?:\+91[\-\s]?)?[1-9][0-9]{1,4}[\-\s]?[0-9]{6,8}'
# General phone pattern for international formats
PHONE_PATTERN = r'(?:\+\d{1,3}[\-\s]?)?\(?(?:\d{1,4})?\)?[\-\s]?\d{3,}[\-\s]?\d{3,}'
# Pattern for phone numbers with lots of separators
SEPARATED_PHONE_PATTERN = r'(?:\d[\-\s]?){7,15}'

# Updated email regex based on requirements
EMAIL_PATTERN = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'

# Obfuscated email patterns adjusted to match requirements
OBFUSCATED_EMAIL_PATTERN = r'[a-zA-Z0-9._%+-]+\s*(?:\[at\]|\(at\)|{at}|\s+at\s+)\s*[a-zA-Z0-9.-]+\s*(?:\[dot\]|\(dot\)|{dot}|\s+dot\s+)\s*[a-zA-Z]{2,}'

# Phone pattern adjusted to strictly match numbers starting with + as per requirements
PHONE_PATTERN = r'\+[\d\s\-()]{7,20}'

# For India-specific patterns, we'll still use +91 pattern but make sure it follows the main format
INDIA_PHONE_PATTERN = r'\+91[\d\s\-()]{8,15}'

class CompanyContactSpider(CrawlSpider):
    """Spider for scraping company contact information."""
    
    name = 'company_contact'
    
    # Rules for following links
    rules = (
        # Follow contact and about pages with higher priority
        Rule(LinkExtractor(allow=r'/(contact|about|contactus|reach-us|get-in-touch)', deny=r'/(login|signin|signup)'), 
             callback='parse_item', follow=True, process_request='process_request'),
        
        # Follow other internal links
        Rule(LinkExtractor(deny=r'/(login|signin|signup)'), 
             callback='parse_item', follow=True, process_request='process_request'),
    )
    
    def __init__(self, companies_file='companylist.csv', output_file='company_contacts.csv', *args, **kwargs):
        """Initialize the spider with company data and settings."""
        super(CompanyContactSpider, self).__init__(*args, **kwargs)
        self.companies_file = companies_file
        self.output_file = output_file
        self.companies = self.load_companies()
        self.results = {}
        self.current_company = None
        
        # Remove existing output file to avoid duplicate entries
        if os.path.exists(self.output_file):
            os.remove(self.output_file)
        
        # Write CSV header
        with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Company Name', 'Website', 'Email', 'Phone', 'Source'])
    
    def load_companies(self):
        """Load company names from CSV file."""
        companies = []
        try:
            with open(self.companies_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    company_name = row.get('Company Name', '').strip()
                    if company_name:
                        companies.append({
                            'name': company_name,
                            'url': self.guess_company_url(company_name)
                        })
            logging.info(f"Loaded {len(companies)} companies from {self.companies_file}")
        except Exception as e:
            logging.error(f"Error loading companies file: {e}")
            companies = []
        return companies
    
    def guess_company_url(self, company_name):
        """Make a best guess at the company's website URL."""
        # Replace spaces with hyphens and remove special characters
        name_part = re.sub(r'[^\w\s-]', '', company_name.lower()).strip().replace(' ', '-')
        return f"http://www.{name_part}.com"
    
    def process_request(self, request, spider):
        """Process the request before sending it."""
        # Rotate user agents
        request.headers['User-Agent'] = random.choice(USER_AGENTS)
        return request
    
    def start_requests(self):
        """Start requests for each company with an improved sequential LinkedIn search flow."""
        for company in self.companies:
            self.current_company = company
            
            # STEP 1: Start with LinkedIn search by company name
            # This is the first step in our sequential LinkedIn search flow
            company_name = company['name'].replace(' ', '%20')
            linkedin_search_url = f"https://www.linkedin.com/search/results/companies/?keywords={company_name}"
            
            yield scrapy.Request(
                url=linkedin_search_url,
                callback=self.parse_linkedin_search_results,
                errback=self.handle_error,
                meta={
                    'company': company, 
                    'source': 'linkedin_search',
                    'dont_redirect': False,
                    'linkedin_search_step': 'step1_search'  # Track the step in the LinkedIn flow
                },
                dont_filter=True,
                priority=3  # Highest priority - start with LinkedIn
            )
            
            # As a backup, also try the company website
            # (but with lower priority than LinkedIn search)
            yield scrapy.Request(
                url=company['url'],
                callback=self.parse_start_url,
                errback=self.handle_error,
                meta={'company': company, 'source': 'company_website', 'dont_redirect': False},
                dont_filter=True,
                priority=1  # Lower priority than LinkedIn search
            )
    
    def parse_start_url(self, response):
        """Parse the start URL."""
        company = response.meta.get('company')
        source = response.meta.get('source')
        
        # Extract contact information from the main page
        self.extract_contact_info(response, company, source)
        
        # Check if we already have both email and phone
        company_key = company['name']
        if company_key in self.results and self.results[company_key].get('email') and self.results[company_key].get('phone'):
            # Already have complete information, no need to continue
            return
        
        # Specifically target common contact page paths with higher priority
        high_priority_targets = [
            'contact-us', 'contact', 'contactus', 'about-us', 'about', 'reach-us',
            'contact.html', 'contact.php', 'about.html', 'about.php',
            'contact-us.html', 'about-us.html', 'get-in-touch'
        ]
        
        for target in high_priority_targets:
            contact_link = urljoin(response.url, target)
            yield scrapy.Request(
                url=contact_link,
                callback=self.parse_contact_page,
                errback=self.handle_error,
                meta={'company': company, 'source': f"{source}_contact_page"},
                dont_filter=True,
                priority=3  # Higher priority for contact pages
            )
        
        # Try to find and follow all contact-related links
        contact_links = response.css('a[href*="contact"], a[href*="about"], a:contains("Contact"), a:contains("About"), a:contains("Touch")::attr(href)').getall()
        for link in contact_links:
            full_url = response.urljoin(link)
            yield scrapy.Request(
                url=full_url,
                callback=self.parse_contact_page,
                errback=self.handle_error,
                meta={'company': company, 'source': f"{source}_contact_link"},
                dont_filter=True,
                priority=2
            )
        
        # ENHANCEMENT: Also look for footer links and navigation elements
        footer_links = response.css('footer a, .footer a, #footer a, [class*=footer] a::attr(href)').getall()
        for link in footer_links:
            if 'contact' in link.lower() or 'about' in link.lower():
                full_url = response.urljoin(link)
                yield scrapy.Request(
                    url=full_url,
                    callback=self.parse_contact_page,
                    errback=self.handle_error,
                    meta={'company': company, 'source': f"{source}_footer_link"},
                    dont_filter=True,
                    priority=3  # High priority for footer contact links
                )
                
        # Look for main navigation links
        nav_links = response.css('nav a, .nav a, #nav a, .menu a, #menu a, .navigation a, #navigation a::attr(href)').getall()
        for link in nav_links:
            if 'contact' in link.lower() or 'about' in link.lower():
                full_url = response.urljoin(link)
                yield scrapy.Request(
                    url=full_url,
                    callback=self.parse_contact_page,
                    errback=self.handle_error,
                    meta={'company': company, 'source': f"{source}_nav_link"},
                    dont_filter=True,
                    priority=2
                )
        
        # Now try alternate domains if we don't have results yet
        if company_key not in self.results or not (self.results[company_key].get('email') and self.results[company_key].get('phone')):
            name_part = re.sub(r'[^\w\s-]', '', company['name'].lower()).strip().replace(' ', '-')
            for domain in ['.in', '.co.in', '.co', '.org', '.net']:
                alt_url = f"http://www.{name_part}{domain}"
                yield scrapy.Request(
                    url=alt_url,
                    callback=self.parse_item,
                    errback=self.handle_error,
                    meta={'company': company, 'source': 'company_website_alt', 'dont_redirect': False},
                    dont_filter=True
                )
        
        # If we still don't have contact info, try social platforms as a fallback
        if company_key not in self.results or not (self.results[company_key].get('email') and self.results[company_key].get('phone')):
            # Continue with LinkedIn and other social media checks...
            # (keeping the existing code for LinkedIn, IndiaMart, and Facebook here)
            
            # Try different variations of the company name for LinkedIn
            company_name_variations = [
                company['name'].lower().replace(' ', '-'),
                company['name'].lower().replace(' ', ''),
                ''.join(word[0] for word in company['name'].lower().split()),  # Acronym
            ]
            
            for variation in company_name_variations:
                # Try LinkedIn
                linkedin_search_url = f"https://www.linkedin.com/company/{variation}"
                yield scrapy.Request(
                    url=linkedin_search_url,
                    callback=self.parse_linkedin,
                    errback=self.handle_error,
                    meta={
                        'company': company, 
                        'source': 'linkedin',
                        'dont_redirect': False,
                        'handle_httpstatus_list': [302, 301, 403, 404]
                    },
                    dont_filter=True
                )
            
            # Try IndiaMart with different search queries
            indiamart_variations = [
                company['name'].replace(' ', '+'),
                company['name'].split()[0] + '+' + company['name'].split()[-1] if len(company['name'].split()) > 1 else company['name']
            ]
            
            for variation in indiamart_variations:
                indiamart_search_url = f"https://dir.indiamart.com/search.mp?ss={variation}"
                yield scrapy.Request(
                    url=indiamart_search_url,
                    callback=self.parse_indiamart,
                    errback=self.handle_error,
                    meta={'company': company, 'source': 'indiamart', 'dont_redirect': False},
                    dont_filter=True
                )
            
            # Try Facebook with different variations
            for variation in company_name_variations:
                facebook_search_url = f"https://www.facebook.com/{variation}"
                yield scrapy.Request(
                    url=facebook_search_url,
                    callback=self.parse_facebook,
                    errback=self.handle_error,
                    meta={
                        'company': company, 
                        'source': 'facebook',
                        'dont_redirect': False,
                        'handle_httpstatus_list': [302, 301, 403, 404]
                    },
                    dont_filter=True
                )
    
    def parse_item(self, response):
        """Parse a webpage to extract contact information."""
        company = response.meta.get('company')
        source = response.meta.get('source')
        depth = response.meta.get('depth', 0)
        
        self.extract_contact_info(response, company, source)
        
        # If we're at depth 0, follow additional "about", "contact" links
        if depth == 0:
            # Extract all links and filter for about/contact pages
            links = response.css('a::attr(href)').getall()
            for link in links:
                if any(contact_term in link.lower() for contact_term in ['contact', 'about', 'reach']):
                    full_url = response.urljoin(link)
                    yield scrapy.Request(
                        url=full_url,
                        callback=self.parse_item,
                        errback=self.handle_error,
                        meta={'company': company, 'source': source, 'depth': 1},
                        dont_filter=True
                    )
    
    def parse_linkedin(self, response):
        """Parse LinkedIn company page."""
        company = response.meta.get('company')
        source = 'linkedin'
        
        # LinkedIn specific extraction
        self.extract_contact_info(response, company, source)
        
        # First, try the search approach
        if 'linkedin.com/company/' in response.url:
            # We're on a company page, look for the about section
            about_links = response.css('a[href*="about"]::attr(href)').getall()
            for link in about_links:
                full_url = response.urljoin(link)
                yield scrapy.Request(
                    url=full_url,
                    callback=self.parse_linkedin_about,
                    errback=self.handle_error,
                    meta={
                        'company': company, 
                        'source': 'linkedin_about',
                        'dont_redirect': False,
                        'linkedin_search_step': 'step1_search'  # Track the step in the LinkedIn flow
                    },
                    dont_filter=True
                )
        else:
            # Try LinkedIn search for the company
            search_query = company['name'].replace(' ', '%20')
            search_url = f"https://www.linkedin.com/search/results/companies/?keywords={search_query}"
            yield scrapy.Request(
                url=search_url,
                callback=self.parse_linkedin_search_results,
                errback=self.handle_error,
                meta={'company': company, 'source': 'linkedin_search'},
                dont_filter=True
            )
    
    def parse_linkedin_search_results(self, response):
        """Parse LinkedIn search results to find company pages - STEP 1 in the sequential flow."""
        company = response.meta.get('company')
        search_step = response.meta.get('linkedin_search_step', 'step1_search')
        
        # Check if redirected to login page
        if 'login' in response.url or 'checkpoint' in response.url:
            logging.warning(f"LinkedIn redirected to login page. Trying alternate methods for {company['name']}")
            # Skip and try other sources
            return
        
        # Extract links to company pages from search results
        company_links = response.css('a[href*="/company/"]::attr(href), .search-result__result-link::attr(href), [data-tracking-control-name="search_srp_result"]::attr(href)').getall()
        
        # Process and filter company links
        relevant_links = []
        for link in company_links:
            if '/company/' in link:
                # Clean the URL
                if '?' in link:
                    link = link.split('?')[0]
                full_url = response.urljoin(link)
                
                # Extract company name from URL to check relevance
                try:
                    url_company_name = link.split('/company/')[1].split('/')[0].replace('-', ' ')
                    # Check similarity to our target company
                    if self.similar_names(url_company_name, company['name']):
                        relevant_links.append((url_company_name, full_url))
                except:
                    continue
        
        # Sort by similarity to target company name (most similar first)
        relevant_links.sort(key=lambda x: self.similarity_score(x[0], company['name']), reverse=True)
        
        # STEP 2: Follow the most relevant company pages (up to 3)
        for i, (name, url) in enumerate(relevant_links[:3]):
            # For the most relevant result, prioritize it
            priority = 3 if i == 0 else 2
            
            logging.info(f"Following LinkedIn company page for {company['name']}: {url}")
            
            yield scrapy.Request(
                url=url,
                callback=self.parse_linkedin_company,
                errback=self.handle_error,
                meta={
                    'company': company, 
                    'source': 'linkedin_company',
                    'linkedin_search_step': 'step2_company_page',  # Next step in the flow
                    'company_url': url,
                    'ranked_position': i+1  # Track which result this was
                },
                dont_filter=True,
                priority=priority
            )
        
        # If no relevant links found, try direct company name variants
        if not relevant_links:
            logging.info(f"No relevant LinkedIn company pages found for {company['name']}. Trying direct variants.")
            name_variants = [
                company['name'].lower().replace(' ', '-'),
                company['name'].lower().replace(' ', ''),
                ''.join(word[0] for word in company['name'].lower().split() if word),  # Acronym
            ]
            
            for variant in name_variants:
                if variant:  # Ensure we don't try empty strings
                    linkedin_company_url = f"https://www.linkedin.com/company/{variant}"
                    yield scrapy.Request(
                        url=linkedin_company_url,
                        callback=self.parse_linkedin_company,
                        errback=self.handle_error,
                        meta={
                            'company': company, 
                            'source': 'linkedin_company_direct',
                            'linkedin_search_step': 'step2_company_page',
                            'dont_redirect': False,
                            'handle_httpstatus_list': [302, 301, 403, 404]
                        },
                        dont_filter=True
                    )
    
    def similarity_score(self, name1, name2):
        """Calculate a similarity score between two company names."""
        # Convert to lowercase and remove common words
        name1 = name1.lower()
        name2 = name2.lower()
        
        # Simple similarity score based on shared words
        words1 = set(name1.split())
        words2 = set(name2.split())
        common_words = words1.intersection(words2)
        
        # Calculate Jaccard similarity
        if len(words1) == 0 or len(words2) == 0:
            return 0
        return len(common_words) / len(words1.union(words2))
    
    def similar_names(self, name1, name2):
        """Check if two company names are similar enough."""
        # Convert to lowercase and remove spaces
        name1 = name1.lower().replace(' ', '')
        name2 = name2.lower().replace(' ', '')
        
        # Check if one name is contained in the other
        if name1 in name2 or name2 in name1:
            return True
        
        # Calculate similarity - can use more advanced methods if needed
        # Simple method: get first 5 chars if available
        name1_prefix = name1[:5] if len(name1) >= 5 else name1
        name2_prefix = name2[:5] if len(name2) >= 5 else name2
        
        return name1_prefix in name2 or name2_prefix in name1
        
    def parse_linkedin_company(self, response):
        """Parse LinkedIn company page - STEP 2 in the sequential flow."""
        company = response.meta.get('company')
        search_step = response.meta.get('linkedin_search_step', 'step2_company_page')
        
        # Check if redirected to login page
        if 'login' in response.url or 'checkpoint' in response.url:
            logging.warning(f"LinkedIn redirected to login page for {company['name']}")
            # Skip and try other sources
            return
        
        # First, check if this is a valid company page
        is_valid_company_page = '/company/' in response.url
        if not is_valid_company_page:
            logging.warning(f"Not a valid LinkedIn company page for {company['name']}: {response.url}")
            return
            
        logging.info(f"Processing LinkedIn company page for {company['name']}: {response.url}")
        
        # STEP 3: Find and follow the About section link
        # Look for about section with various selectors
        about_links = response.css('a[href*="about"]::attr(href), a:contains("About")::attr(href), a.ember-view:contains("about")::attr(href)').getall()
        
        # If about links found, follow them
        about_links_found = False
        for link in about_links:
            if 'about' in link.lower():
                full_url = response.urljoin(link)
                about_links_found = True
                
                logging.info(f"Following LinkedIn about page for {company['name']}: {full_url}")
                
                yield scrapy.Request(
                    url=full_url,
                    callback=self.parse_linkedin_about,
                    errback=self.handle_error,
                    meta={
                        'company': company, 
                        'source': 'linkedin_about',
                        'linkedin_search_step': 'step3_about_page',  # Next step in the flow
                        'company_url': response.url
                    },
                    dont_filter=True,
                    priority=3  # High priority
                )
                break  # Just follow the first valid about link
        
        # If no about links found, try to construct the about URL
        if not about_links_found and '/company/' in response.url:
            # Extract the base company URL
            base_url = response.url.split('/life')[0].split('/about')[0].rstrip('/')
            about_url = f"{base_url}/about/"
            
            logging.info(f"Constructing LinkedIn about URL for {company['name']}: {about_url}")
            
            yield scrapy.Request(
                url=about_url,
                callback=self.parse_linkedin_about,
                errback=self.handle_error,
                meta={
                    'company': company, 
                    'source': 'linkedin_about_constructed',
                    'linkedin_search_step': 'step3_about_page',
                    'company_url': response.url
                },
                dont_filter=True,
                priority=3  # High priority
            )
        
        # Also try to extract contact info directly from the company page as a fallback
        contact_data = self.extract_linkedin_specific_contact(response)
        
        if contact_data.get('email') or contact_data.get('phone'):
            company_key = company['name']
            if company_key not in self.results:
                self.results[company_key] = {
                    'name': company['name'],
                    'website': company['url'],
                    'email': '',
                    'phone': '',
                    'source': ''
                }
            
            if contact_data.get('email') and not self.results[company_key].get('email'):
                self.results[company_key]['email'] = contact_data['email']
                self.results[company_key]['source'] = 'linkedin_company'
                logging.info(f"Found email for {company['name']} from LinkedIn company page: {contact_data['email']}")
            
            if contact_data.get('phone') and not self.results[company_key].get('phone'):
                self.results[company_key]['phone'] = contact_data['phone']
                self.results[company_key]['source'] = 'linkedin_company'
                logging.info(f"Found phone for {company['name']} from LinkedIn company page: {contact_data['phone']}")
            
            self.save_result(company_key)
    
    def extract_linkedin_specific_contact(self, response):
        """Extract contact information using LinkedIn-specific patterns."""
        contact_data = {'email': '', 'phone': ''}
        
        # LinkedIn often formats contact info in specific ways
        
        # Method 1: Look for elements with contact-info classes
        contact_elements = response.css('.org-contact-info-card, .org-about-company-module__container')
        for element in contact_elements:
            element_text = element.get()
            emails = re.findall(EMAIL_PATTERN, element_text)
            phones = re.findall(PHONE_PATTERN, element_text) + re.findall(INDIA_PHONE_PATTERN, element_text)
            
            if emails:
                valid_emails = [email for email in emails if not any(domain in email.lower() for domain in [
                    'example.com', 'yourdomain.com', 'domain.com', 'email.com',
                    'someone@', 'user@', 'name@', 'your@', 'info@example'
                ])]
                if valid_emails:
                    contact_data['email'] = valid_emails[0]
            
            if phones:
                filtered_phones = []
                for phone in phones:
                    cleaned = re.sub(r'[^\d+]', '', phone)
                    if len(cleaned) >= 8:
                        filtered_phones.append(phone)
                if filtered_phones:
                    contact_data['phone'] = filtered_phones[0]
        
        # Method 2: Look for specific contact information sections
        contact_sections = response.css('.org-about-company-module__company-page-url, .org-about-company-module__phone')
        for section in contact_sections:
            section_text = section.get()
            if '@' in section_text:
                emails = re.findall(EMAIL_PATTERN, section_text)
                if emails:
                    contact_data['email'] = emails[0]
            
            phones = re.findall(PHONE_PATTERN, section_text) + re.findall(INDIA_PHONE_PATTERN, section_text)
            if phones:
                contact_data['phone'] = phones[0]
        
        # Method 3: Try to find "tel:" and "mailto:" links which LinkedIn often uses
        mailto_links = response.css('a[href^="mailto:"]::attr(href)').getall()
        for link in mailto_links:
            email = link.replace('mailto:', '').split('?')[0].strip()
            if '@' in email and '.' in email.split('@')[1]:
                contact_data['email'] = email
                break
        
        tel_links = response.css('a[href^="tel:"]::attr(href)').getall()
        for link in tel_links:
            phone = link.replace('tel:', '').strip()
            if re.sub(r'[^\d+]', '', phone):  # Ensure there are digits
                contact_data['phone'] = phone
                break
                
        return contact_data
        
    def parse_linkedin_about(self, response):
        """Parse LinkedIn about page - STEP 3 (final) in the sequential flow."""
        company = response.meta.get('company')
        search_step = response.meta.get('linkedin_search_step', 'step3_about_page')
        company_url = response.meta.get('company_url', '')
        
        # Check if redirected to login page
        if 'login' in response.url or 'checkpoint' in response.url:
            logging.warning(f"LinkedIn redirected to login page for {company['name']} about page")
            # Skip and try other sources
            return
            
        logging.info(f"Processing LinkedIn about page for {company['name']}: {response.url}")
        
        # This is the main target page where contact information should be found
        # Focus intensively on extracting contact data here
        
        # 1. First look for structured contact information sections
        contact_sections = response.css('.org-contact-info-card, .org-about-company-module__container, .org-page-details__card')
        contact_data_found = False
        
        for section in contact_sections:
            section_html = section.get()
            if 'contact' in section_html.lower() or 'phone' in section_html.lower() or 'email' in section_html.lower():
                # Extract all potential emails and phones from this section with high priority
                email_candidates = self.extract_emails_from_content(section_html)
                phone_candidates = self.extract_phones_from_content(section_html)
                
                company_key = company['name']
                if company_key not in self.results:
                    self.results[company_key] = {
                        'name': company['name'],
                        'website': company['url'],
                        'email': '',
                        'phone': '',
                        'source': ''
                    }
                
                # Update email if found
                if email_candidates and not self.results[company_key].get('email'):
                    self.results[company_key]['email'] = email_candidates[0]
                    self.results[company_key]['source'] = 'linkedin_about'
                    logging.info(f"Found email for {company['name']} from LinkedIn about page: {email_candidates[0]}")
                    contact_data_found = True
                
                # Update phone if found
                if phone_candidates and not self.results[company_key].get('phone'):
                    self.results[company_key]['phone'] = phone_candidates[0]
                    self.results[company_key]['source'] = 'linkedin_about'
                    logging.info(f"Found phone for {company['name']} from LinkedIn about page: {phone_candidates[0]}")
                    contact_data_found = True
                
                if contact_data_found:
                    self.save_result(company_key)
        
        # 2. If no structured contact data found, look at the entire about page
        if not contact_data_found:
            # Use our specialized LinkedIn-specific contact extraction
            contact_data = self.extract_linkedin_specific_contact(response)
            
            if contact_data.get('email') or contact_data.get('phone'):
                company_key = company['name']
                if company_key not in self.results:
                    self.results[company_key] = {
                        'name': company['name'],
                        'website': company['url'],
                        'email': '',
                        'phone': '',
                        'source': ''
                    }
                
                # Update email if found
                if contact_data.get('email') and not self.results[company_key].get('email'):
                    self.results[company_key]['email'] = contact_data['email']
                    self.results[company_key]['source'] = 'linkedin_about'
                    logging.info(f"Found email for {company['name']} from LinkedIn about page: {contact_data['email']}")
                
                # Update phone if found
                if contact_data.get('phone') and not self.results[company_key].get('phone'):
                    self.results[company_key]['phone'] = contact_data['phone']
                    self.results[company_key]['source'] = 'linkedin_about'
                    logging.info(f"Found phone for {company['name']} from LinkedIn about page: {contact_data['phone']}")
                
                self.save_result(company_key)
            
            # 3. Check for website links that might lead to the company's own site for contact info
            website_links = response.css('a.link-without-visited-state::attr(href), a.ember-view.org-top-card-primary-actions__action::attr(href)').getall()
            company_website_found = False
            
            for link in website_links:
                if link.startswith('http') and 'linkedin.com' not in link:
                    # Visit the company website to find more contact info
                    company_website_found = True
                    
                    logging.info(f"Found company website from LinkedIn for {company['name']}: {link}")
                    
                    yield scrapy.Request(
                        url=link,
                        callback=self.parse_item,
                        errback=self.handle_error,
                        meta={
                            'company': company, 
                            'source': 'company_website_from_linkedin',
                            'dont_redirect': False
                        },
                        dont_filter=True,
                        priority=2
                    )
                    break  # Just follow the first valid website
            
            # 4. If we still don't have contact info and found no website link, try general page extraction
            if not company_website_found and not contact_data.get('email') and not contact_data.get('phone'):
                # Extract all contact information from the about page content as a last resort
                self.extract_contact_info(response, company, 'linkedin_about_general')
    
    def parse_indiamart(self, response):
        """Enhanced IndiaMart parsing to extract accurate contact information."""
        company = response.meta.get('company')
        source = 'indiamart'
        
        # Extract contact information from the IndiaMart page
        self.extract_contact_info(response, company, source)
        
        # Try to find company results and follow them
        company_links = response.css('a.prd-name::attr(href), a.title::attr(href)').getall()
        if company_links:
            # Only follow the first few results
            for link in company_links[:3]:
                yield scrapy.Request(
                    url=link,
                    callback=self.parse_indiamart_company,
                    errback=self.handle_error,
                    meta={'company': company, 'source': 'indiamart_company_page'},
                    dont_filter=True
                )
        
        # IndiaMart-specific: Look for contact info in search results
        # IndiaMart often displays phone numbers directly in search results
        phone_elements = response.css('.pns-number, .bo, .pns-digit, .cm-fs14, span[data-mnum]::attr(data-mnum)').getall()
        phones = []
        for elem in phone_elements:
            # Clean the text
            phone = elem.strip()
            if re.match(r'\d+', phone) and self.validate_phone_number(phone):
                phones.append(phone)
        
        # Extract email addresses that might be visible in search results
        email_elements = response.css('*::text').getall()
        for text in email_elements:
            if '@' in text:
                found_emails = re.findall(EMAIL_PATTERN, text)
                for email in found_emails:
                    if not any(domain in email.lower() for domain in [
                        'example.com', 'yourdomain.com', 'domain.com', 'email.com',
                        'someone@', 'user@', 'name@', 'your@', 'info@example'
                    ]):
                        company_key = company['name']
                        if company_key not in self.results:
                            self.results[company_key] = {
                                'name': company['name'],
                                'website': company['url'],
                                'email': '',
                                'phone': '',
                                'source': ''
                            }
                        
                        if not self.results[company_key].get('email'):
                            self.results[company_key]['email'] = email
                            self.results[company_key]['source'] = source
                            logging.info(f"Found email for {company['name']} from IndiaMart search: {email}")
        
        # Look specifically for mobile/phone numbers in IndiaMart's format
        if phones:
            company_key = company['name']
            if company_key not in self.results:
                self.results[company_key] = {
                    'name': company['name'],
                    'website': company['url'],
                    'email': '',
                    'phone': '',
                    'source': ''
                }
            
            # Update phone if found
            if not self.results[company_key].get('phone') and phones:
                self.results[company_key]['phone'] = phones[0]
                self.results[company_key]['source'] = source
                logging.info(f"Found phone for {company['name']} from IndiaMart search: {phones[0]}")
                self.save_result(company_key)
        
        # Also look for "Contact Us" or "Contact Details" links in search results
        contact_links = response.css('a::attr(href)').getall()
        for link in contact_links:
            if 'contact' in link.lower() or 'sendInquiry' in link:
                full_url = response.urljoin(link)
                yield scrapy.Request(
                    url=full_url,
                    callback=self.parse_indiamart_company,
                    errback=self.handle_error,
                    meta={'company': company, 'source': 'indiamart_contact'},
                    dont_filter=True
                )
    
    def parse_indiamart_company(self, response):
        """Enhanced IndiaMart company page parsing."""
        company = response.meta.get('company')
        source = response.meta.get('source')
        
        # IndiaMart company pages often have structured contact info
        self.extract_contact_info(response, company, source)
        
        # Look specifically for mobile/phone numbers in IndiaMart's format
        # These are IndiaMart-specific CSS selectors that typically contain phone numbers
        phone_elements = response.css('.pns-number::text, .bo::text, .pns-digit::text, .pns-numer::text, .pns-ligh::text, span[data-mnum]::attr(data-mnum)').getall()
        phones = []
        for elem in phone_elements:
            # Clean the text
            phone = elem.strip()
            if re.match(r'\d+', phone) and self.validate_phone_number(phone):
                phones.append(phone)
        
        # IndiaMart also often has email addresses in specific places
        email_links = response.css('a[href^="mailto:"]::attr(href)').getall()
        emails = []
        for link in email_links:
            email = link.replace('mailto:', '').split('?')[0].strip()
            if '@' in email and '.' in email.split('@')[1]:
                emails.append(email)
        
        # Also try direct text extraction for emails
        email_elements = response.css('*::text').getall()
        for text in email_elements:
            if '@' in text:
                found_emails = re.findall(EMAIL_PATTERN, text)
                valid_emails = [e for e in found_emails if not any(domain in e.lower() for domain in [
                    'example.com', 'yourdomain.com', 'domain.com', 'email.com',
                    'someone@', 'user@', 'name@', 'your@', 'info@example'
                ])]
                emails.extend(valid_emails)
        
        # If we found emails or phones
        if emails or phones:
            company_key = company['name']
            if company_key not in self.results:
                self.results[company_key] = {
                    'name': company['name'],
                    'website': company['url'],
                    'email': '',
                    'phone': '',
                    'source': ''
                }
            
            # Update email if found
            if emails and not self.results[company_key].get('email'):
                self.results[company_key]['email'] = emails[0]
                self.results[company_key]['source'] = source
                logging.info(f"Found email for {company['name']} from IndiaMart company page: {emails[0]}")
            
            # Update phone if found
            if phones and not self.results[company_key].get('phone'):
                self.results[company_key]['phone'] = phones[0]
                self.results[company_key]['source'] = source
                logging.info(f"Found phone for {company['name']} from IndiaMart company page: {phones[0]}")
            
            self.save_result(company_key)
            
        # Try additional IndiaMart-specific extraction using Gemini API
        if not (self.results.get(company_key, {}).get('email') and self.results.get(company_key, {}).get('phone')):
            gemini_results = self.verify_contact_info(company['name'], company['url'], response.text)
            company_key = company['name']
            
            if gemini_results.get('email') and not self.results.get(company_key, {}).get('email'):
                if company_key not in self.results:
                    self.results[company_key] = {
                        'name': company['name'],
                        'website': company['url'],
                        'email': '',
                        'phone': '',
                        'source': ''
                    }
                self.results[company_key]['email'] = gemini_results['email']
                self.results[company_key]['source'] = f"{source} (gemini)"
                logging.info(f"Found email for {company['name']} from IndiaMart using Gemini: {gemini_results['email']}")
            
            if gemini_results.get('phone') and not self.results.get(company_key, {}).get('phone'):
                if company_key not in self.results:
                    self.results[company_key] = {
                        'name': company['name'],
                        'website': company['url'],
                        'email': '',
                        'phone': '',
                        'source': ''
                    }
                if self.validate_phone_number(gemini_results['phone']):
                    self.results[company_key]['phone'] = gemini_results['phone']
                    self.results[company_key]['source'] = f"{source} (gemini)"
                    logging.info(f"Found phone for {company['name']} from IndiaMart using Gemini: {gemini_results['phone']}")
                
            if (gemini_results.get('email') or gemini_results.get('phone')) and company_key in self.results:
                self.save_result(company_key)
    
    def verify_contact_info(self, company_name, company_website, content):
        """Use Google Gemini to extract and verify contact information."""
        try:
            # Initialize Gemini model
            model = genai.GenerativeModel('gemini-1.5-flash')
            
            # Create a prompt that focuses on extracting accurate contact information
            prompt = f"""
            You are an expert at extracting ACCURATE contact information from website content.
            
            COMPANY NAME: {company_name}
            WEBSITE: {company_website}
            
            Please analyze the following HTML/text and extract ONLY VALID:
            1. Email address - only valid business emails, no examples or placeholders
            2. Phone number - only valid phone numbers that match these requirements:
            
            VALID PHONE NUMBER REQUIREMENTS:
            - Must be at least 10 digits total (including country code)
            - Must be actually near text indicating it's a contact number (like "Call us", "Phone:", "Contact:", "Tel:")
            - For Indian numbers:
                * Must start with +91 followed by a 10-digit number
                * OR start with 0 followed by a 10-digit number
                * OR be a 10-digit number starting with 6, 7, 8, or 9
            - For international numbers:
                * Must include a country code (like +1, +44, etc.)
                * Must have the correct number of digits for that country
            - DO NOT extract:
                * Short numbers like 1234567 (7 digits or less)
                * Sequences like 123456789
                * Numbers that all have the same digit (like 9999999999)
                * Numbers that appear to be postal/zip codes, years, or other random numbers
                * Numbers that don't appear near contact-related text
            
            If you find multiple potential phone numbers, choose the most likely legitimate contact number based on context.
            
            Respond with ONLY this JSON format:
            {"email": "extracted_email@example.com", "phone": "extracted_phone_number"}
            
            If no valid email or phone is found, use empty string for that field.
            
            HTML/TEXT:
            {content[:30000]}  # Limit content size
            """
            
            # Create a prompt that focuses on extracting accurate contact information
            prompt = f"""
            You are an expert at extracting ACCURATE contact information from website content.
            
            COMPANY NAME: {company_name}
            WEBSITE: {company_website}
            
            Please analyze the following HTML/text and extract ONLY VALID contact information following these STRICT requirements:
            
            EMAIL REQUIREMENTS:
            1. Must match pattern: [a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{{2,}}
            2. Must only be from visible text elements (p, span, a, div) or in href="mailto:..."
            3. If obfuscated (like "info (at) company (dot) com"), convert to standard format:
               - Replace: [at], (at), {{at}}, at → @
               - Replace: [dot], (dot), {{dot}}, dot → .
            4. Must use common TLDs (.com, .in, .org, .co.in, .net, etc.)
            5. Length must be reasonable (between 6-100 characters)
            6. Must NOT be a placeholder or fake email
            
            PHONE NUMBER REQUIREMENTS:
            1. Must START WITH "+" symbol (like +91, +1, +44)
            2. Format should be: \+[\d\s\-()]{{7,20}}
            3. After cleaning (removing spaces, hyphens, parentheses), must be 10-15 digits
            4. Phone must be in visible text near words like "Call", "Phone", "Tel", etc.
            5. Example of valid formats:
               - +91 9876543210
               - +1 (123) 456-7890
            
            Your response MUST be ONLY this JSON format:
            {{"email": "extracted_email@example.com", "phone": "+919876543210"}}
            
            If no valid contact is found, use empty string for that field.
            
            HTML/TEXT:
            {content[:30000]}
            """
            
            # Call Gemini API
            response = model.generate_content(prompt)
            
            # Extract JSON from response
            response_text = response.text
            # Remove markdown code blocks if present
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0].strip()
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0].strip()
            
            import json
            try:
                result = json.loads(response_text)
                
                # Additional validation on extracted phone number
                if "phone" in result and result["phone"]:
                    phone = result["phone"]
                    # Validate the phone number with our validator
                    if not self.validate_phone_number(phone):
                        # If it doesn't pass our validation, discard it
                        result["phone"] = ""
                        logging.info(f"Discarded invalid phone from Gemini for {company_name}: {phone}")
                
                # Additional validation on extracted email
                if "email" in result and result["email"]:
                    email = result["email"]
                    # Validate using our strict email validation
                    valid_emails = self.extract_emails_from_content(email)
                    if not valid_emails:
                        result["email"] = ""
                        logging.info(f"Discarded invalid email from Gemini for {company_name}: {email}")
                
                return result
            except json.JSONDecodeError:
                # If JSON parsing fails, use regex to extract the values
                import re
                email_match = re.search(r'"email":\s*"([^"]*)"', response_text)
                phone_match = re.search(r'"phone":\s*"([^"]*)"', response_text)
                
                email = email_match.group(1) if email_match else ""
                phone = phone_match.group(1) if phone_match else ""
                
                # Validate the extracted phone number
                if phone and not self.validate_phone_number(phone):
                    phone = ""
                    logging.info(f"Discarded invalid phone from Gemini regex for {company_name}")
                
                # Validate the extracted email
                if email:
                    valid_emails = self.extract_emails_from_content(email)
                    if not valid_emails:
                        email = ""
                        logging.info(f"Discarded invalid email from Gemini regex for {company_name}")
                
                return {"email": email, "phone": phone}
        
        except Exception as e:
            logging.error(f"Error using Gemini API for {company_name}: {str(e)}")
            return {"email": "", "phone": ""}

    def validate_phone_number(self, phone):
        """Validate if a phone number meets the required conditions."""
        # Must start with +
        if not phone.startswith('+'):
            return False
            
        # Get digits only (excluding the +)
        digits_only = re.sub(r'[^\d]', '', phone[1:])
        
        # Check length requirements (10-15 digits)
        if len(digits_only) < 10 or len(digits_only) > 15:
            return False
            
        # Check for repeating digits (more than 7 of the same digit is suspicious)
        for digit in '0123456789':
            if digit * 7 in digits_only:
                return False
                
        # Check for sequential patterns (like 12345678)
        ascending = '0123456789'
        descending = '9876543210'
        if any(ascending[i:i+8] in digits_only for i in range(len(ascending)-7)):
            return False
        if any(descending[i:i+8] in digits_only for i in range(len(descending)-7)):
            return False
            
        # All checks passed
        return True

    def extract_contact_info(self, response, company, source):
        """Extract email and phone information from a webpage with improved accuracy."""
        content = response.text
        company_key = company['name']
        
        # Initialize result for this company if not exists
        if company_key not in self.results:
            self.results[company_key] = {
                'name': company['name'],
                'website': company['url'],
                'email': '',
                'phone': '',
                'source': ''
            }
        
        # 1. TARGET SPECIFIC SECTIONS FIRST - these are more likely to contain legitimate contact info
        
        # Check footer section - often contains contact info
        footer_sections = response.css('footer, .footer, #footer, [class*=footer], [id*=footer]').getall()
        for section in footer_sections:
            self.extract_from_section(section, company_key, source, "footer")
            
        # Check contact sections
        contact_sections = response.css('.contact, #contact, [class*=contact], [id*=contact], .get-in-touch, #get-in-touch').getall()
        for section in contact_sections:
            self.extract_from_section(section, company_key, source, "contact section")
            
        # Check about sections
        about_sections = response.css('.about, #about, [class*=about-us], [id*=about-us]').getall()
        for section in about_sections:
            self.extract_from_section(section, company_key, source, "about section")
        
        # ENHANCEMENT: Check specifically for address sections, which often contain contact details
        address_sections = response.css('address, .address, .location, .company-address, .contact-address').getall()
        for section in address_sections:
            self.extract_from_section(section, company_key, source, "address section")
        
        # ENHANCEMENT: Look for schema.org structured data which often contains formatted contact info
        schema_sections = response.css('script[type="application/ld+json"]::text').getall()
        for schema in schema_sections:
            try:
                import json
                schema_data = json.loads(schema)
                # Extract contact information from schema data
                if isinstance(schema_data, dict):
                    self.extract_from_schema(schema_data, company_key, source)
                elif isinstance(schema_data, list):
                    for item in schema_data:
                        if isinstance(item, dict):
                            self.extract_from_schema(item, company_key, source)
            except Exception as e:
                logging.debug(f"Error parsing schema.org data: {e}")
        
        # 2. EXTRACT FROM SPECIAL HTML ELEMENTS
        html_contacts = self.extract_contacts_from_html(response)
        
        # Update results with HTML element contacts
        if html_contacts['emails'] and not self.results[company_key].get('email'):
            self.results[company_key]['email'] = html_contacts['emails'][0]
            self.results[company_key]['source'] = f"{source} (html element)"
            logging.info(f"Found email for {company['name']} from HTML element: {html_contacts['emails'][0]}")
        
        if html_contacts['phones'] and not self.results[company_key].get('phone'):
            self.results[company_key]['phone'] = html_contacts['phones'][0]
            self.results[company_key]['source'] = f"{source} (html element)"
            logging.info(f"Found phone for {company['name']} from HTML element: {html_contacts['phones'][0]}")
        
        # 3. USE GEMINI API FOR BETTER EXTRACTION
        # Only use Gemini if we still don't have both email and phone
        if not (self.results[company_key].get('email') and self.results[company_key].get('phone')):
            gemini_results = self.verify_contact_info(company['name'], company['url'], content)
            
            if gemini_results.get('email') and not self.results[company_key].get('email'):
                self.results[company_key]['email'] = gemini_results['email']
                self.results[company_key]['source'] = f"{source} (gemini)"
                logging.info(f"Found email for {company['name']} using Gemini: {gemini_results['email']}")
            
            if gemini_results.get('phone') and not self.results[company_key].get('phone'):
                # Validate the phone number
                if self.validate_phone_number(gemini_results['phone']):
                    self.results[company_key]['phone'] = gemini_results['phone']
                    self.results[company_key]['source'] = f"{source} (gemini)"
                    logging.info(f"Found phone for {company['name']} using Gemini: {gemini_results['phone']}")
        
        # 4. FALLBACK TO TRADITIONAL EXTRACTION IF STILL NEEDED
        if not (self.results[company_key].get('email') and self.results[company_key].get('phone')):
            # Extract all email formats
            emails = self.extract_emails_from_content(content)
            
            # Extract all phone formats
            phones = self.extract_phones_from_content(content)
            
            # Update results if we found new information
            if emails and not self.results[company_key].get('email'):
                self.results[company_key]['email'] = emails[0]
                self.results[company_key]['source'] = source
                logging.info(f"Found email for {company['name']}: {emails[0]}")
            
            if phones and not self.results[company_key].get('phone'):
                self.results[company_key]['phone'] = phones[0]
                self.results[company_key]['source'] = source
                logging.info(f"Found phone for {company['name']}: {phones[0]}")
        
        # Save results to CSV if we have found both email and phone or if we've checked all sources
        if ((self.results[company_key].get('email') and self.results[company_key].get('phone')) or 
            source in ['facebook', 'indiamart_company_page', 'linkedin_about']):
            self.save_result(company_key)
            
    def extract_from_schema(self, schema_data, company_key, source):
        """Extract contact information from schema.org structured data."""
        if not isinstance(schema_data, dict):
            return
            
        # Check for email in schema data
        email = None
        if 'email' in schema_data:
            email = schema_data['email']
        elif 'contactPoint' in schema_data and isinstance(schema_data['contactPoint'], dict):
            email = schema_data['contactPoint'].get('email')
        elif 'contactPoint' in schema_data and isinstance(schema_data['contactPoint'], list):
            for point in schema_data['contactPoint']:
                if isinstance(point, dict) and 'email' in point:
                    email = point['email']
                    break
                    
        # Clean and validate email
        if email and '@' in email:
            cleaned_email = email.strip()
            if not any(domain in cleaned_email.lower() for domain in [
                'example.com', 'yourdomain.com', 'domain.com', 'email.com',
                'someone@', 'user@', 'name@', 'your@', 'info@example'
            ]):
                if not self.results[company_key].get('email'):
                    self.results[company_key]['email'] = cleaned_email
                    self.results[company_key]['source'] = f"{source} (schema)"
                    logging.info(f"Found email for {self.results[company_key]['name']} in schema.org data: {cleaned_email}")
        
        # Check for phone in schema data
        phone = None
        if 'telephone' in schema_data:
            phone = schema_data['telephone']
        elif 'phone' in schema_data:
            phone = schema_data['phone']
        elif 'contactPoint' in schema_data and isinstance(schema_data['contactPoint'], dict):
            phone = schema_data['contactPoint'].get('telephone') or schema_data['contactPoint'].get('phone')
        elif 'contactPoint' in schema_data and isinstance(schema_data['contactPoint'], list):
            for point in schema_data['contactPoint']:
                if isinstance(point, dict) and ('telephone' in point or 'phone' in point):
                    phone = point.get('telephone') or point.get('phone')
                    break
                    
        # Clean and validate phone
        if phone:
            cleaned_phone = self.clean_phone_number(phone)
            if cleaned_phone and not self.results[company_key].get('phone'):
                self.results[company_key]['phone'] = cleaned_phone
                self.results[company_key]['source'] = f"{source} (schema)"
                logging.info(f"Found phone for {self.results[company_key]['name']} in schema.org data: {cleaned_phone}")

    def extract_from_section(self, section_html, company_key, source, section_type):
        """Extract contact information from a specific HTML section with improved validation"""
        # Extract all email formats
        emails = self.extract_emails_from_content(section_html)
        
        # Extract all phone formats
        phones = self.extract_phones_from_content(section_html)
        
        # Also check for mailto and tel links in this section
        section_html_lower = section_html.lower()
        
        # Extract mailto links more reliably using regex
        mailto_matches = re.findall(r'href=["\']mailto:([^"\']+)["\']', section_html_lower)
        for match in mailto_matches:
            email = match.split('?')[0].strip()
            if '@' in email and '.' in email.split('@')[1]:
                emails.append(email)
                
        # Extract tel links more reliably using regex
        tel_matches = re.findall(r'href=["\']tel:([^"\']+)["\']', section_html_lower)
        for match in tel_matches:
            phone = match.strip()
            cleaned = self.clean_phone_number(phone)
            if cleaned:
                phones.append(cleaned)
        
        # ENHANCEMENT: Look for contact labels that might have contact info nearby
        contact_labels = ['contact us', 'email us', 'mail us', 'call us', 'phone', 'telephone', 'mobile', 'email', 'e-mail', 'get in touch']
        for label in contact_labels:
            if label in section_html_lower:
                # Find the position of the label
                pos = section_html_lower.find(label)
                if pos != -1:
                    # Check nearby text (within 150 characters) for contact information
                    search_start = max(0, pos - 30)
                    search_end = min(len(section_html_lower), pos + 150)
                    nearby_text = section_html_lower[search_start:search_end]
                    
                    # Look for emails in nearby text
                    nearby_emails = self.extract_emails_from_content(nearby_text)
                    emails.extend(nearby_emails)
                    
                    # Look for phones in nearby text
                    nearby_phones = self.extract_phones_from_content(nearby_text)
                    phones.extend(nearby_phones)
        
        # For footer sections specifically, look for text within span, p, div elements
        if 'footer' in section_type:
            # Extract text within common footer elements
            footer_elements = re.findall(r'<(?:span|p|div|li|address)[^>]*>(.*?)</(?:span|p|div|li|address)>', section_html, re.DOTALL)
            for element in footer_elements:
                # Look for emails in footer elements
                footer_emails = self.extract_emails_from_content(element)
                emails.extend(footer_emails)
                
                # Look for phones in footer elements
                footer_phones = self.extract_phones_from_content(element)
                phones.extend(footer_phones)
        
        # Update results if we found new information
        if emails and not self.results[company_key].get('email'):
            self.results[company_key]['email'] = emails[0]
            self.results[company_key]['source'] = f"{source} ({section_type})"
            logging.info(f"Found email for {self.results[company_key]['name']} in {section_type}: {emails[0]}")
        
        if phones and not self.results[company_key].get('phone'):
            self.results[company_key]['phone'] = phones[0]
            self.results[company_key]['source'] = f"{source} ({section_type})"
            logging.info(f"Found phone for {self.results[company_key]['name']} in {section_type}: {phones[0]}")

    def parse_contact_page(self, response):
        """Parse a contact page which is more likely to contain contact information."""
        company = response.meta.get('company')
        source = response.meta.get('source')
        
        # Extract contact information with higher confidence since this is a contact page
        company_key = company['name']
        
        # CONTACT VALIDATION - Ensure we're actually on a contact page
        url_lower = response.url.lower()
        page_title = response.css('title::text').get('').lower()
        body_text = ' '.join(response.css('body ::text').getall()).lower()
        
        is_contact_page = (
            'contact' in url_lower or 'about' in url_lower or
            'contact' in page_title or 'about' in page_title or
            ('contact' in body_text and ('email' in body_text or 'phone' in body_text or 'call' in body_text))
        )
        
        if is_contact_page:
            # This appears to be an actual contact/about page, proceed with extraction
            # Use the general extraction method first
            self.extract_contact_info(response, company, source)
            
            # ENHANCEMENT: Search specifically for contact sections or containers
            contact_sections = response.css('.contact-info, .contact-details, .contact-us, .address, .location, .get-in-touch, .contact-form, .contact-data').getall()
            for section in contact_sections:
                self.extract_from_section(section, company_key, source, "contact section")
            
            # Try to find contact info in contact forms - they often have labels or placeholders
            # that indicate an email field or phone field
            email_field = response.css('input[type="email"], input[name*="email"], input[placeholder*="email"]')
            if email_field and not self.results[company_key].get('email'):
                # Look for surrounding text that might contain an email address
                form_text = ' '.join(email_field.xpath('ancestor::form//text()').getall())
                emails = self.extract_emails_from_content(form_text)
                
                if emails:
                    self.results[company_key]['email'] = emails[0]
                    self.results[company_key]['source'] = f"{source} (contact form)"
                    logging.info(f"Found email for {company['name']} in contact form: {emails[0]}")
            
            phone_field = response.css('input[type="tel"], input[name*="phone"], input[placeholder*="phone"], input[name*="mobile"], input[placeholder*="mobile"]')
            if phone_field and not self.results[company_key].get('phone'):
                # Look for surrounding text that might contain a phone number
                form_text = ' '.join(phone_field.xpath('ancestor::form//text()').getall())
                phones = self.extract_phones_from_content(form_text)
                
                if phones:
                    self.results[company_key]['phone'] = phones[0]
                    self.results[company_key]['source'] = f"{source} (contact form)"
                    logging.info(f"Found phone for {company['name']} in contact form: {phones[0]}")
            
            # Look for structured contact info in paragraphs, divs, or list items
            contact_blocks = response.css('p, div, li')
            for block in contact_blocks:
                block_text = block.get()
                # Check if this block has contact-related keywords
                if any(keyword in block_text.lower() for keyword in ['email', 'mail', 'phone', 'call', 'contact', 'tel']):
                    # Process this block for emails
                    emails = self.extract_emails_from_content(block_text)
                    if emails and not self.results[company_key].get('email'):
                        self.results[company_key]['email'] = emails[0]
                        self.results[company_key]['source'] = f"{source} (contact block)"
                        logging.info(f"Found email for {company['name']} in contact block: {emails[0]}")
                    
                    # Process this block for phones
                    phones = self.extract_phones_from_content(block_text)
                    if phones and not self.results[company_key].get('phone'):
                        self.results[company_key]['phone'] = phones[0]
                        self.results[company_key]['source'] = f"{source} (contact block)"
                        logging.info(f"Found phone for {company['name']} in contact block: {phones[0]}")
            
            # ENHANCEMENT: Check for contact cards or vcard elements
            contact_cards = response.css('.vcard, .contact-card, .business-card, .card-contact, address, .address').getall()
            for card in contact_cards:
                emails = self.extract_emails_from_content(card)
                if emails and not self.results[company_key].get('email'):
                    self.results[company_key]['email'] = emails[0]
                    self.results[company_key]['source'] = f"{source} (contact card)"
                    logging.info(f"Found email for {company['name']} in contact card: {emails[0]}")
                
                phones = self.extract_phones_from_content(card)
                if phones and not self.results[company_key].get('phone'):
                    self.results[company_key]['phone'] = phones[0]
                    self.results[company_key]['source'] = f"{source} (contact card)"
                    logging.info(f"Found phone for {company['name']} in contact card: {phones[0]}")
            
            # Save the result
            if self.results[company_key].get('email') or self.results[company_key].get('phone'):
                self.save_result(company_key)
    
    def save_result(self, company_key):
        """Save the current result to the CSV file only if it hasn't been saved before."""
        if company_key in self.results:
            result = self.results[company_key]
            if (result.get('email') or result.get('phone')) and not hasattr(self, f"saved_{company_key}"):
                with open(self.output_file, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        result.get('name', ''),
                        result.get('website', ''),
                        result.get('email', ''),
                        result.get('phone', ''),
                        result.get('source', '')
                    ])
                # Mark as saved to prevent duplicates
                setattr(self, f"saved_{company_key}", True)
                logging.info(f"Saved contact info for {company_key}")
    
    def handle_error(self, failure):
        """Handle request errors."""
        company = failure.request.meta.get('company')
        if company:
            url = failure.request.url
            error_type = type(failure.value).__name__
            logging.error(f"Failed to scrape {url} for {company['name']}: {error_type}")

    def parse_facebook(self, response):
        """Parse Facebook company page with improved extraction."""
        company = response.meta.get('company')
        source = 'facebook'
        
        # Check if redirected to login page
        if 'login' in response.url or 'checkpoint' in response.url:
            logging.warning(f"Facebook redirected to login page for {company['name']}")
            # Skip and try other sources
            return
        
        # Facebook specific extraction
        self.extract_contact_info(response, company, source)
        
        # Facebook often has contact info in specific sections
        # Look for about/info/contact sections first
        about_links = response.css('a[href*="about"], a[href*="info"], a[href*="contact"]::attr(href)').getall()
        for link in about_links:
            full_url = response.urljoin(link)
            yield scrapy.Request(
                url=full_url,
                callback=self.parse_facebook_about,
                errback=self.handle_error,
                meta={'company': company, 'source': 'facebook_about'},
                dont_filter=True
            )
        
        # Facebook-specific: Also try to construct the about URL
        if '/pages/' in response.url and not about_links:
            base_url = response.url.split('?')[0].rstrip('/')
            about_url = f"{base_url}/about/"
            yield scrapy.Request(
                url=about_url,
                callback=self.parse_facebook_about,
                errback=self.handle_error,
                meta={'company': company, 'source': 'facebook_about_constructed'},
                dont_filter=True
            )
        
        # Try Gemini API extraction specifically for Facebook pages
        gemini_results = self.verify_contact_info(company['name'], company['url'], response.text)
        
        company_key = company['name']
        if company_key not in self.results:
            self.results[company_key] = {
                'name': company['name'],
                'website': company['url'],
                'email': '',
                'phone': '',
                'source': ''
            }
        
        if gemini_results.get('email') and not self.results[company_key].get('email'):
            self.results[company_key]['email'] = gemini_results['email']
            self.results[company_key]['source'] = f"{source} (gemini)"
            logging.info(f"Found email for {company['name']} from Facebook using Gemini: {gemini_results['email']}")
        
        if gemini_results.get('phone') and not self.results[company_key].get('phone'):
            if self.validate_phone_number(gemini_results['phone']):
                self.results[company_key]['phone'] = gemini_results['phone']
                self.results[company_key]['source'] = f"{source} (gemini)"
                logging.info(f"Found phone for {company['name']} from Facebook using Gemini: {gemini_results['phone']}")
            
        if (gemini_results.get('email') or gemini_results.get('phone')):
            self.save_result(company_key)

    def parse_facebook_about(self, response):
        """Parse Facebook about/info page which often contains contact information."""
        company = response.meta.get('company')
        source = response.meta.get('source')
        
        # Extract contact information
        self.extract_contact_info(response, company, source)
        
        # Facebook often displays contact info in specific sections
        # Look for elements with words like "email", "contact", "phone"
        contact_sections = response.css('div:contains("Contact"), div:contains("Email"), div:contains("Phone"), div:contains("Info")').getall()
        
        for section in contact_sections:
            # Extract emails from this section
            emails = re.findall(EMAIL_PATTERN, section)
            # Filter out common false positives
            emails = [email for email in emails if not any(domain in email.lower() for domain in [
                'example.com', 'yourdomain.com', 'domain.com', 'email.com',
                'someone@', 'user@', 'name@', 'your@', 'info@example'
            ])]
            
            # Extract phone numbers
            phones = re.findall(PHONE_PATTERN, section)
            india_phones = re.findall(INDIA_PHONE_PATTERN, section)
            phones = list(set(phones + india_phones))
            
            # Filter out invalid phone numbers
            filtered_phones = []
            for phone in phones:
                if self.validate_phone_number(phone):
                    filtered_phones.append(phone)
            phones = filtered_phones
            
            # Update results if we found new information
            company_key = company['name']
            if company_key not in self.results:
                self.results[company_key] = {
                    'name': company['name'],
                    'website': company['url'],
                    'email': '',
                    'phone': '',
                    'source': ''
                }
            
            if emails and not self.results[company_key].get('email'):
                self.results[company_key]['email'] = emails[0]
                self.results[company_key]['source'] = source
                logging.info(f"Found email for {company['name']} from Facebook about: {emails[0]}")
            
            if phones and not self.results[company_key].get('phone'):
                self.results[company_key]['phone'] = phones[0]
                self.results[company_key]['source'] = source
                logging.info(f"Found phone for {company['name']} from Facebook about: {phones[0]}")
            
        # Save results
        if company_key in self.results and (self.results[company_key].get('email') or self.results[company_key].get('phone')):
            self.save_result(company_key)

    def clean_obfuscated_email(self, email):
        """Clean and format obfuscated email addresses."""
        if not email:
            return ""
            
        # Replace common obfuscations with standard characters
        email = re.sub(r'\s*\[at\]\s*|\s*\(at\)\s*|\s*[@]\s*|\s+at\s+', '@', email)
        email = re.sub(r'\s*\[dot\]\s*|\s*\(dot\)\s*|\s*\[\.]\s*|\s*\(\.?\)\s*|\s+dot\s+', '.', email)
        
        # Remove extra spaces
        email = re.sub(r'\s+', '', email)
        
        # Validate the cleaned email
        if re.match(EMAIL_PATTERN, email):
            return email
        return ""

    def clean_obfuscated_email(self, email):
        """Clean and format obfuscated email addresses according to requirements."""
        if not email:
            return ""
            
        # Step 1: Replace all obfuscation patterns for '@'
        email = re.sub(r'\s*\[at\]\s*|\s*\(at\)\s*|\s*\{at\}\s*|\s*[@]\s*|\s+at\s+', '@', email)
        
        # Step 2: Replace all obfuscation patterns for '.'
        email = re.sub(r'\s*\[dot\]\s*|\s*\(dot\)\s*|\s*\{dot\}\s*|\s*\[\.]\s*|\s*\(\.?\)\s*|\s+dot\s+', '.', email)
        
        # Step 3: Remove all spaces
        email = re.sub(r'\s+', '', email)
        
        # Step 4: Validate that it matches the standard email pattern
        if re.match(EMAIL_PATTERN, email):
            return email
        
        return ""

    def extract_obfuscated_email(self, content):
        """Extract obfuscated email from content and clean it."""
        # Find all potential obfuscated emails
        obfuscated_matches = re.findall(OBFUSCATED_EMAIL_PATTERN, content)
        
        cleaned_emails = []
        for match in obfuscated_matches:
            cleaned = self.clean_obfuscated_email(match)
            if cleaned:
                cleaned_emails.append(cleaned)
            
        return cleaned_emails

    def extract_emails_from_content(self, content):
        """Extract all email formats from content."""
        # Standard emails
        standard_emails = re.findall(EMAIL_PATTERN, content)
        
        # Obfuscated emails
        obfuscated_emails = self.extract_obfuscated_email(content)
        
        # Combine unique emails
        all_emails = list(set(standard_emails + obfuscated_emails))
        
        # Filter out common false positives
        valid_emails = [email for email in all_emails if not any(domain in email.lower() for domain in [
            'example.com', 'yourdomain.com', 'domain.com', 'email.com',
            'someone@', 'user@', 'name@', 'your@', 'info@example'
        ])]
        
        return valid_emails

    def extract_emails_from_content(self, content):
        """Extract all email formats from content according to strict requirements."""
        if not content:
            return []
        
        # Standard emails using the specified regex
        standard_emails = re.findall(EMAIL_PATTERN, content)
        
        # Get obfuscated emails
        obfuscated_emails = self.extract_obfuscated_email(content)
        
        # Combine all found emails
        all_emails = list(set(standard_emails + obfuscated_emails))
        
        # Apply the filtering conditions
        valid_emails = []
        for email in all_emails:
            # Check reasonable length (between 6 to 100 characters)
            if len(email) < 6 or len(email) > 100:
                continue
                
            # Check for allowed domains
            allowed_tlds = ['.com', '.in', '.org', '.co.in', '.net', '.edu', '.gov', '.io', '.info', '.biz']
            if not any(email.lower().endswith(tld) for tld in allowed_tlds):
                continue
                
            # Filter out common false positives and placeholder emails
            if any(domain in email.lower() for domain in [
                'example.com', 'yourdomain.com', 'domain.com', 'email.com', 'test.com',
                'someone@', 'user@', 'name@', 'your@', 'info@example', 'email@', 'test@'
            ]):
                continue
                
            valid_emails.append(email)
            
        return valid_emails

    def clean_phone_number(self, phone):
        """Clean and format phone numbers according to strict requirements."""
        if not phone:
            return ""
        
        # Per requirements, phone must start with +
        if not phone.startswith('+'):
            return ""
                
        # Clean the number by removing spaces, hyphens, and parentheses
        cleaned = '+' + re.sub(r'[\s\-()]', '', phone[1:])
        
        # Check reasonable length (after cleaning, excluding +, should be 10-15 digits)
        digits_only = re.sub(r'[^\d]', '', cleaned[1:])
        if len(digits_only) < 10 or len(digits_only) > 15:
            return ""
            
        # Additional validation
        if self.validate_phone_number(cleaned):
            return cleaned
            
        return ""

    def extract_phones_from_content(self, content):
        """Extract all phone number formats from content with improved filtering."""
        if not content:
            return []
            
        # Pre-clean content by removing excessive spaces between digits
        # This helps with numbers like "9 8 7 6 5 4 3 2 1 0"
        content_for_separated = re.sub(r'(\d)\s+(\d)', r'\1\2', content)
        
        # First look for clear phone indicators in text
        phone_indicators = ['phone', 'mobile', 'cell', 'call', 'tel', 'telephone', 'contact', 'dial', 'whatsapp']
        high_confidence_phones = []
        
        # Search for phone numbers near indicators
        for indicator in phone_indicators:
            if indicator in content.lower():
                # Find positions of all occurrences of the indicator
                positions = [m.start() for m in re.finditer(r'\b' + re.escape(indicator) + r'\b', content.lower())]
                
                for pos in positions:
                    # Search in the nearby text (30 chars before and 50 chars after)
                    start = max(0, pos - 30)
                    end = min(len(content), pos + 50)
                    nearby_text = content[start:end]
                    
                    # Extract using all phone patterns
                    india_phones = re.findall(INDIA_PHONE_PATTERN, nearby_text)
                    general_phones = re.findall(PHONE_PATTERN, nearby_text)
                    landline_phones = re.findall(INDIA_LANDLINE_PATTERN, nearby_text)
                    separated_phones = re.findall(SEPARATED_PHONE_PATTERN, nearby_text)
                    
                    # Clean and validate these phones with high confidence
                    for phone in set(india_phones + general_phones + landline_phones + separated_phones):
                        cleaned = self.clean_phone_number(phone)
                        if cleaned and cleaned not in high_confidence_phones:
                            high_confidence_phones.append(cleaned)
        
        # If we found high confidence phones, prioritize those
        if high_confidence_phones:
            # Sort by length - longer numbers often have country/area codes and are more complete
            return sorted(high_confidence_phones, key=len, reverse=True)
        
        # Otherwise, fall back to extracting all phone-like numbers from the content
        # Extract using all phone patterns
        india_phones = re.findall(INDIA_PHONE_PATTERN, content)
        general_phones = re.findall(PHONE_PATTERN, content)
        landline_phones = re.findall(INDIA_LANDLINE_PATTERN, content)
        separated_phones = re.findall(SEPARATED_PHONE_PATTERN, content_for_separated)
        
        # Combine, clean and validate all potential phone numbers
        all_phones = []
        for phone in set(india_phones + general_phones + landline_phones + separated_phones):
            cleaned = self.clean_phone_number(phone)
            if cleaned and cleaned not in all_phones:
                all_phones.append(cleaned)
        
        # Sort by length - longer numbers often have country/area codes and are more complete
        return sorted(all_phones, key=len, reverse=True)

    def extract_phones_from_content(self, content):
        """Extract phone numbers that strictly match the required format (must start with +)."""
        if not content:
            return []
            
        # According to requirements, only extract phone numbers that start with +
        plus_phones = re.findall(PHONE_PATTERN, content)
        
        # Find phone numbers near contact indicators for higher confidence
        phone_indicators = ['phone', 'mobile', 'cell', 'call', 'tel', 'telephone', 'contact']
        high_confidence_phones = []
        
        for indicator in phone_indicators:
            if indicator in content.lower():
                # Find positions of indicators
                positions = [m.start() for m in re.finditer(r'\b' + re.escape(indicator) + r'\b', content.lower())]
                
                for pos in positions:
                    # Check nearby text
                    start = max(0, pos - 30)
                    end = min(len(content), pos + 50)
                    nearby_text = content[start:end]
                    
                    # Look specifically for + numbers in this region
                    nearby_phones = re.findall(PHONE_PATTERN, nearby_text)
                    for phone in nearby_phones:
                        cleaned = self.clean_phone_number(phone)
                        if cleaned and cleaned not in high_confidence_phones:
                            high_confidence_phones.append(cleaned)
        
        # Process all found phone numbers
        valid_phones = []
        for phone in set(plus_phones):
            cleaned = self.clean_phone_number(phone)
            if cleaned and cleaned not in valid_phones:
                valid_phones.append(cleaned)
                
        # Prioritize high confidence phones if found
        if high_confidence_phones:
            return high_confidence_phones
            
        return valid_phones

    def extract_contacts_from_html(self, response):
        """Extract both email and phone from special HTML elements like links."""
        contacts = {'emails': [], 'phones': []}
        
        # Extract from mailto: links
        mailto_links = response.css('a[href^="mailto:"]::attr(href)').getall()
        for link in mailto_links:
            email = link.replace('mailto:', '').split('?')[0].strip()
            if '@' in email and '.' in email.split('@')[1]:
                contacts['emails'].append(email)
        
        # Extract from tel: links
        tel_links = response.css('a[href^="tel:"]::attr(href)').getall()
        for link in tel_links:
            phone = link.replace('tel:', '').strip()
            cleaned = self.clean_phone_number(phone)
            if cleaned:
                contacts['phones'].append(cleaned)
        
        # Extract from meta tags
        meta_emails = response.css('meta[name*="email"]::attr(content), meta[property*="email"]::attr(content)').getall()
        for email in meta_emails:
            if '@' in email:
                contacts['emails'].append(email)
        
        # Extract from meta tags with phone
        meta_phones = response.css('meta[name*="phone"]::attr(content), meta[property*="phone"]::attr(content)').getall()
        for phone in meta_phones:
            cleaned = self.clean_phone_number(phone)
            if cleaned:
                contacts['phones'].append(cleaned)
        
        return contacts

    def extract_contacts_from_html(self, response):
        """Extract both email and phone from special HTML elements like links according to strict requirements."""
        contacts = {'emails': [], 'phones': []}
        
        # 1. Extract emails from visible text elements
        visible_text_elements = response.css('p::text, span::text, a::text, div::text, h1::text, h2::text, h3::text, h4::text, h5::text, h6::text, li::text').getall()
        for text in visible_text_elements:
            emails = self.extract_emails_from_content(text)
            for email in emails:
                if email not in contacts['emails']:
                    contacts['emails'].append(email)
        
        # 2. Extract emails from mailto: links (high confidence)
        mailto_links = response.css('a[href^="mailto:"]::attr(href)').getall()
        for link in mailto_links:
            email = link.replace('mailto:', '').split('?')[0].strip()
            if '@' in email and '.' in email.split('@')[1]:
                # Validate the email
                if re.match(EMAIL_PATTERN, email):
                    if email not in contacts['emails']:
                        contacts['emails'].append(email)
        
        # 3. Extract phone numbers from tel: links
        tel_links = response.css('a[href^="tel:"]::attr(href)').getall()
        for link in tel_links:
            # Add + if not present (tel: links sometimes omit it)
            phone = link.replace('tel:', '').strip()
            if not phone.startswith('+'):
                # See if it's an Indian number that should have +91
                if len(phone) == 10 and phone[0] in '6789':
                    phone = '+91' + phone
                # Add + to international format
                elif phone.startswith('00'):
                    phone = '+' + phone[2:]
                else:
                    # Add + by default
                    phone = '+' + phone
                    
            # Clean and validate
            cleaned = self.clean_phone_number(phone)
            if cleaned and cleaned not in contacts['phones']:
                contacts['phones'].append(cleaned)
        
        # 4. Extract from meta tags (emails only)
        meta_emails = response.css('meta[name*="email"]::attr(content), meta[property*="email"]::attr(content)').getall()
        for email in meta_emails:
            if '@' in email and re.match(EMAIL_PATTERN, email):
                valid_emails = self.extract_emails_from_content(email)
                for valid_email in valid_emails:
                    if valid_email not in contacts['emails']:
                        contacts['emails'].append(valid_email)
        
        # Filter emails by additional requirements
        filtered_emails = []
        for email in contacts['emails']:
            # Check length requirements
            if 6 <= len(email) <= 100:
                # Check for allowed domains
                allowed_tlds = ['.com', '.in', '.org', '.co.in', '.net', '.edu', '.gov', '.io', '.info', '.biz']
                if any(email.lower().endswith(tld) for tld in allowed_tlds):
                    filtered_emails.append(email)
                    
        contacts['emails'] = filtered_emails
        
        return contacts

def main():
    """Main function to run the spider."""
    print("Starting Company Contact Scraper...")
    print("This script will:")
    print("1. Load company names from companylist.csv")
    print("2. Visit company websites to find contact information")
    print("3. Check LinkedIn, IndiaMart, and Facebook if needed")
    print("4. Save results to company_contacts.csv")
    
    # Configure Scrapy settings
    settings = get_project_settings()
    settings.update({
        'BOT_NAME': 'company_contact_scraper',
        'ROBOTSTXT_OBEY': False,  # Set to False to avoid restrictions from robots.txt
        'DOWNLOAD_DELAY': 1,  # 1 second delay between requests
        'RANDOMIZE_DOWNLOAD_DELAY': True,  # Randomize delay
        'CONCURRENT_REQUESTS': 16,  # Increase concurrent requests
        'COOKIES_ENABLED': False,
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en',
        },
        'RETRY_TIMES': 3,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 408, 429],
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
        },
        'DOWNLOAD_TIMEOUT': 15,  # Timeout for requests
        'HTTPERROR_ALLOW_ALL': True,  # Allow all HTTP errors to be processed
        'LOG_LEVEL': 'INFO'
    })
    
    # Run the spider
    process = CrawlerProcess(settings)
    process.crawl(CompanyContactSpider)
    process.start()
    
    print("Scraping complete. Results saved to company_contacts.csv")

if __name__ == "__main__":
    main() 