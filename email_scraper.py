from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import re
import concurrent.futures
import os
import platform

def setup_driver():
    # Setup Selenium with Chrome options
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # Run Chrome in headless mode (without GUI)
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    # Check if running on Streamlit Cloud (Linux-based environment)
    if platform.system() == 'Linux' and os.path.exists('/home/appuser'):
        # Streamlit Cloud configuration
        try:
            # Use system Chromium browser and driver on Streamlit Cloud
            return webdriver.Chrome(
                service=Service("/usr/bin/chromedriver"),
                options=options
            )
        except Exception as e:
            print(f"Failed to use system chromedriver: {e}")
            # Fall back to normal setup
    
    # Default for local environment or fallback
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

def scrape_emails_with_selenium(url):
    driver = setup_driver()  # Initialize the WebDriver for each URL
    try:
        driver.get(url)
        
        # Use WebDriverWait to wait for the page to load more effectively
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )

        # Get page source after JavaScript rendering
        page_source = driver.page_source

        # Updated regex to handle domains with numbers and better filtering
        email_pattern = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
        emails = set(email_pattern.findall(page_source))

        # Post-process to remove unwanted results like "aos@2.3.1"
        filtered_emails = [email for email in emails if not re.search(r"[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}", email)]  # Remove IP addresses as email domains
        filtered_emails = [email for email in filtered_emails if not email.startswith(('aos', 'jquery', 'bootstrap'))]  # Remove common library names

        return filtered_emails

    except Exception as e:
        print(f"[Error] Failed to fetch {url}: {e}")
        return []
    finally:
        driver.quit()  # Quit the driver after scraping the page
    
def scrape_emails_from_multiple_urls(urls):
    all_emails = {}
    
    # Use ThreadPoolExecutor to handle URLs concurrently, but create a new driver for each URL
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_url = {executor.submit(scrape_emails_with_selenium, url): url for url in urls}
        
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                emails = future.result()
                if emails:
                    all_emails[url] = emails
                else:
                    all_emails[url] = "No emails found"
            except Exception as e:
                all_emails[url] = f"Error: {e}"

    return all_emails

# Example usage
if __name__ == "__main__":
    target_urls = [
        "https://www.globalcnc.in/contact-us/",
        "https://advancetestinglab.com/",
        "https://www.akiropes.com",
        "https://www.ap.com/"
    ]
    
    email_results = scrape_emails_from_multiple_urls(target_urls)
    
    for url, emails in email_results.items():
        print(f"Emails found on {url}:")
        if isinstance(emails, list):
            for email in emails:
                print(email)
        else:
            print(emails) 