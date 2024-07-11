import time
import psycopg2
import pandas as pd
from bs4 import BeautifulSoup
import re
import psycopg2
import json

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.select import Select
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

from docx import Document
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import signal
import tempfile
import sys
import logging
import requests
import urllib.parse

def connect_to_db():
    engine = create_engine('postgresql+psycopg2://postgres:Ellipsispostgres42$@localhost/BDCFabric')
    return engine

def fetch_addresses():
    engine = connect_to_db()
    query = "SELECT location_id, brand_name, address_primary, city, zip, address_full, checked_sp FROM public.scraper_test;"
    df = pd.read_sql(query, engine)
    return df

def update_database_sp(location_id, brand_name, eligible_sp, no_service_sp, need_unit_sp, has_account_sp, unknown_sp, unit, checked_sp):
    engine = connect_to_db()
    # Create a connection object
    with engine.connect() as conn:
        # Begin a transaction
        with conn.begin():
            # Define the SQL update statement
            query = text("""
                UPDATE public.scraper_test
                SET eligible_sp = :eligible_sp, no_service_sp = :no_service_sp, need_unit_sp = :need_unit_sp, has_account_sp = :has_account_sp, unknown_sp = :unknown_sp, unit = :unit, checked_sp = :checked_sp
                WHERE location_id = :location_id AND brand_name = :brand_name;
            """)
            # Execute the update statement with parameters
            conn.execute(query, {
                'eligible_sp': eligible_sp,
                'no_service_sp': no_service_sp,
                'need_unit_sp': need_unit_sp,
                'has_account_sp': has_account_sp,
                'unknown_sp': unknown_sp,
                'location_id': location_id,
                'brand_name': brand_name,
                'unit': unit,
                'checked_sp': checked_sp
            })

def address_entry_xf_1(driver, address):
    #print(f"address_entry_xf_1 opened for {address}")
    fullAddressInputArray = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, '//input[@class="input text contained body1 sc-prism-input-text" and @name="localizationAddressField"]')))
    full_address_field = fullAddressInputArray[0]
    driver.execute_script("arguments[0].value = arguments[1];", full_address_field, address)

    #time.sleep(1)
    #check_and_close_popup(driver)  # Check for pop-up after page load

    submit_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//button[@aria-label='Check availability']"))
    )
    submit_button.click()
    #submitButtonArray = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, "//button[contains(., 'Check availability')]")))
    #submit_button =  submitButtonArray[0]
    #driver.execute_script("arguments[0].click();", submit_button)
    #print(f"address_entry_xf_1 closed for {address}")

def check_and_close_popup(driver):
    try:
        # Try to find the 'No thanks' button and click it
        no_thanks_button = WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable((By.XPATH, "//button[@data-click='close' and contains(text(), 'No thanks')]"))
        )
        no_thanks_button.click()
        #print("Debug: Pop-up closed")
    except Exception as e:
        # If the pop-up is not present, just pass
        pass

def dismiss_cookies_xf(driver):
    try:
        cookie_button = WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable((By.ID, "onetrust-reject-all-handler"))
        )
        cookie_button.click()
        #print("Debug: Cookie banner declined")
    except Exception as e:
        pass
        #print("Debug: Failed to click cookie banner:", str(e))

def mdu_suggestion_xf(driver,street,zipcode,address):
    # Check if any autofill suggestions are present
    #print(f"Suggestions found. Processing...")

    # Check if the input field with the class 'choose-address-checkbox' is present
    checkbox_input = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "input.choose-address-checkbox"))
    )
    #print("Checkbox input field is present.")

    # Get the value of the input with id='address-0'
    address_input = driver.find_element(By.ID, "address-1")
    address_value = address_input.get_attribute('value')
    #print(f"Value of 'address-1': {address_value}")
    return address_value

def address_entry_xf_2(driver, street, zipcode):
    #print(f"address_entry_xf_2 opened for {address}")
    check_and_close_popup(driver)  # Check for pop-up after page load
    addressInputArray = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, '//input[@class="input text contained body1 sc-prism-input-text" and @name="localizationAddressField"]')))
    address_field = addressInputArray[0]
    driver.execute_script("arguments[0].value = arguments[1];", address_field, street)
    
    zipInputArray = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, '//input[@class="input text contained body1 sc-prism-input-text" and @name="localizationZipField"]')))
    zip_field = zipInputArray[0]
    driver.execute_script("arguments[0].value = arguments[1];", zip_field, zipcode)
    #check_and_close_popup(driver)  # Check for pop-up after page load
    time.sleep(1)
    submit_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'sc-prism-button') and normalize-space(.)='Check availability']"))
    )
    submit_button.click() 
    #print(f"address_entry_xf_2 closed for {address}") 

def address_entry_xf_3(driver, street, zipcode, unit):
    #print(f"address_entry_xf_3 opened for {address}")
    check_and_close_popup(driver)  # Check for pop-up after page load
    addressInputArray = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, '//input[@class="input text contained body1 sc-prism-input-text" and @name="localizationAddressField"]')))
    address_field = addressInputArray[0]
    driver.execute_script("arguments[0].value = arguments[1];", address_field, street)

    unitInputArray = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, '//input[@class="input text contained body1 sc-prism-input-text" and @name="localizationUnitField"]')))
    unit_field = unitInputArray[0]
    driver.execute_script("arguments[0].value = arguments[1];", unit_field, unit)
    
    zipInputArray = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, '//input[@class="input text contained body1 sc-prism-input-text" and @name="localizationZipField"]')))
    zip_field = zipInputArray[0]
    driver.execute_script("arguments[0].value = arguments[1];", zip_field, zipcode)
    #check_and_close_popup(driver)  # Check for pop-up after page load
    time.sleep(1)
    submit_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'sc-prism-button') and normalize-space(.)='Check availability']"))
    )
    submit_button.click()
    #print(f"address_entry_xf_3 closed for {address}")

def home_or_business_xf(driver):
    print(f"home_or_business_xf opened")
    #print(f"Debug: Home or business select for ",address)
    #check_and_close_popup(driver)  # Check for pop-up after page load
    content=driver.page_source
    document = Document()
    document.add_paragraph(content)
    document.save("debug_page_source.docx")
    submit_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'sc-prism-button') and normalize-space(.)='Home']")))
    submit_button.click()
    #print(f"home_or_business_xf opened for {address}")

def mdu_xf(driver, street, zipcode, address):
    driver.get('https://www.xfinity.com/national')
    wait = WebDriverWait(driver, 10)
    wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
    
    dismiss_cookies_xf(driver)

    address_entry_xf_1(driver, address)

    time.sleep(1)
    wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
    url = driver.current_url
    
    if '<span class="localization-container__header" data-testid="localization-fallback-final-header">Hmm, that address wasn' in driver.page_source :
        #print (f"Address not found block 1 triggered")  

        if driver.find_elements(By.XPATH, "//input[@class='choose-address-checkbox' and @type='radio']"):
            #print (f"Unit suggestion block 1 triggered") 
            address_revised = mdu_suggestion_xf(driver,street,zipcode,address)
            #print(f"Full address is {address_revised}")
            pattern = r',\s*(\w*\s*\w*),'

            # Use re.search() to find the first occurrence of the pattern in the address
            match = re.search(pattern, address_revised)
            #print(f"match is {match}")

            if match:
                #print(f"if match triggered")
                unit = match.group(1)  # Extract the [unit] from the match
                return unit
            else:
                #print(f"else triggered")
                return None

        address_entry_xf_2(driver, street, zipcode)                  

        time.sleep(3)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        url = driver.current_url

        if driver.find_elements(By.XPATH, "//button[contains(@class, 'sc-prism-button') and .//span[contains(text(), 'Home')]]"):
            #print (f"Home/business if block triggered") 
            home_or_business_xf(driver)

            time.sleep(2)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            url = driver.current_url

        if driver.find_elements(By.XPATH, "//input[@class='choose-address-checkbox' and @type='radio']"):
            #print (f"Unit suggestion block 2 triggered") 
            address_revised = mdu_suggestion_xf(driver,street,zipcode,address)
            #print(f"Full address is {address_revised}")
            pattern = r',\s*(\w*\s*\w*),'

            # Use re.search() to find the first occurrence of the pattern in the address
            match = re.search(pattern, address_revised)

            if match:
                unit = match.group(1)  # Extract the [unit] from the match
                return unit
            else:
                return None

        if '<span class="localization-container__header" data-testid="localization-fallback-final-header">Hmm, that address wasn' in driver.page_source:
            #print (f"Address not found block 3 triggered") 

            if driver.find_elements(By.XPATH, "//input[@class='choose-address-checkbox' and @type='radio']"):
                #print (f"Unit suggestion block 3 triggered") 
                address_revised = mdu_suggestion_xf(driver,street,zipcode,address)
                #print(f"Full address is {address_revised}")
                pattern = r',\s*(\w*\s*\w*),'

                # Use re.search() to find the first occurrence of the pattern in the address
                match = re.search(pattern, address_revised)

                if match:
                    unit = match.group(1)  # Extract the [unit] from the match
                    return unit
                else:
                    return None
    
    elif driver.find_elements(By.XPATH, "//input[@class='choose-address-checkbox' and @type='radio']"):
        #print (f"Unit suggestion block 4 triggered") 
        address_revised = mdu_suggestion_xf(driver,street,zipcode,address)
        #print(f"Full address is {address_revised}")
        pattern = r',\s*(\w*\s*\w*),'

        # Use re.search() to find the first occurrence of the pattern in the address
        match = re.search(pattern, address_revised)

        if match:
            unit = match.group(1)  # Extract the [unit] from the match
            return unit
        else:
            return None
    
    else:
        return None

def process_browser_logs_for_network_events(logs):
    for entry in logs:
        log = json.loads(entry["message"])["message"]
        if log["method"] == "Network.requestWillBeSent":
            print(f"Request: {log['params']['request']['url']}")
        elif log["method"] == "Network.responseReceived":
            print(f"Response: {log['params']['response']['url']} Status: {log['params']['response']['status']}")
        elif log["method"] == "Network.loadingFailed":
            if log['params']['canceled']:
                print(f"Request canceled: {log['params']['requestId']}")
            else:
                print(f"Loading Failed: {log['params']['requestId']} Error: {log['params']['errorText']}")

def main():
    data = fetch_addresses()
    # Set up Selenium Chrome driver
    chrome_options = Options()
    #chrome_options.add_argument('--headless')
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.119 Safari/537.36"
    chrome_options.add_argument('--disable-dev-shm-usage')  # prevent Chrome crashes
    chrome_options.add_argument(f'user-agent={user_agent}')
    chrome_options.add_argument('--no-sandbox')  # bypass OS security model, required by Docker if used
    chrome_options.add_argument('--disable-gpu')  # applicable to windows os only
    chrome_options.add_argument('--disable-extensions')  # disabling extensions
    #chrome_options.add_argument("--remote-debugging-port=9222")  # specify a fixed port for debugging
    #chrome_options.add_argument('--disable-logging')  # should disable additional logging
    #chrome_options.add_argument('--log-level=3')  # This sets the logging level to only include fatal errors
    chrome_options.set_capability('goog:loggingPrefs', {'performance': 'ALL', "browser": "ALL"})
    #chrome_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])  # suppresses logging
    #chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])  # This suppresses additional logging
    service = Service('C:\Program Files\Chrome Driver\chromedriver.exe',log_path=os.devnull)

    driver = webdriver.Chrome(service=service, options=chrome_options)

    # Set up Selenium Firefox driver
    # Path to GeckoDriver executable
    #geckodriver_path = Service('C:\Program Files\Chrome Driver\geckodriver.exe')

    # Set up Firefox options (optional)
    #firefox_options = webdriver.FirefoxOptions()
    # Add any options you need, such as headless mode or profile settings

    # Initialize Firefox WebDriver
    #driver = webdriver.Firefox(service=geckodriver_path, options=firefox_options)


    for index, row in data.iterrows():
        # Extract each data point into a variable
        isp = row['brand_name']           # ISP (Internet Service Provider) name
        street = row['address_primary']   # Street address
        city = row['city']                # City
        #zipcode = str(row['zip']).strip() # ZIP code, converted to string and stripped of any excess whitespace
        zipcode = row['zip']
        address = row['address_full']     # Full address
        location_id = row['location_id']
        checked_sp = row['checked_sp']

        need_unit_sp = False
        unit = None

        if isp == "Spectrum": 
            encoded_address = urllib.parse.quote_plus(street)
            test_url = f"https://www.spectrum.com/address/localization?zip={zipcode}&a={encoded_address}"
            driver.get(test_url)
            print(f"Test url is {test_url}")

            #response = requests.get(test_url)
            #final_url = response.url

            #print(f"Response.url is {final_url}")

            url = driver.current_url
            while "localization?" in url:
                time.sleep(10)
                url = driver.current_url

                logs = driver.get_log("performance")
                process_browser_logs_for_network_events(logs)
                
                pending_requests = {}
                completed_requests = set()

                for entry in logs:
                    log = json.loads(entry["message"])["message"]
                    if log["method"] == "Network.requestWillBeSent":
                        pending_requests[log['params']['requestId']] = log['params']['request']['url']
                    elif log["method"] in ["Network.responseReceived", "Network.loadingFinished", "Network.loadingFailed"]:
                        completed_requests.add(log['params']['requestId'])

                # Find requests that were not completed
                for request_id, url in pending_requests.items():
                    if request_id not in completed_requests:
                        print(f"Pending request: {url}")

            if "required-apt" in driver.current_url:
                driver = webdriver.Chrome(service=service, options=chrome_options)
                unit = mdu_xf(driver, street, zipcode, address)
                need_unit_sp = True
                #print(f"The unit is {unit}")

                if unit is None:
                    
                    need_unit_sp = True
                    eligible_sp = False
                    no_service_sp = False
                    has_account_sp = False
                    unknown_sp = True
                    checked_sp = True

                    update_database_sp(location_id, isp, eligible_sp, no_service_sp, need_unit_sp, has_account_sp, unknown_sp, unit, checked_sp)

                    address_status = {
                        "Eligible": eligible_sp,
                        "No Service": no_service_sp,
                        "Need Unit Number": need_unit_sp,
                        "Has Account": has_account_sp,
                        "Unknown": unknown_sp
                    }

                    for status, check in address_status.items():
                        if check:
                            print(f"Updated database for {address}. Status is {status}")

                    # Screenshot logic
                    if no_service_sp or unknown_sp:
                        # Define the filename for the screenshot
                        screenshot_filename = f"{isp}_{address}_{location_id}.png"
                        screenshot_path = f"A:/KCDD/KCMO Challenge/MDU/{screenshot_filename}"  # Adjust the path as necessary
                        driver.save_screenshot(screenshot_path)
                        print(f"Screenshot saved for address {street} under {screenshot_path}")   

                    continue

                else:    
                    encoded_address = urllib.parse.quote_plus(street)
                    encoded_unit = urllib.parse.quote_plus(unit)
                    test_url = f"https://www.spectrum.com/address/localization?zip={zipcode}&aptnum={encoded_unit}&a={encoded_address}"

                    response = requests.get(test_url)
                    driver.current_url = response.url

                    '''driver.get(test_url)
                    url = driver.current_url
                    while "localization?" in url:
                        time.sleep(1)
                        url = driver.current_url'''
            
            eligible_sp = "buy/featured" in driver.current_url
            no_service_sp = ('house-not-found' in driver.current_url) or ('address/out-of-footprint' in driver.current_url) or ('address/buyflow-ineligible' in driver.current_url)
            has_account_sp = "existing-coverage" in driver.current_url
            unknown_sp = not any([eligible_sp, no_service_sp, has_account_sp])
            checked_sp = True

            update_database_sp(location_id, isp, eligible_sp, no_service_sp, need_unit_sp, has_account_sp, unknown_sp, unit, checked_sp)

            address_status = {
                "Eligible": eligible_sp,
                "No Service": no_service_sp,
                "Need Unit Number": need_unit_sp,
                "Has Account": has_account_sp,
                "Unknown": unknown_sp
            }

            for status, check in address_status.items():
                if check:
                    print(f"Updated database for {address}. Status is {status}")

            # Screenshot logic
            if no_service_sp or unknown_sp:
                # Define the filename for the screenshot
                screenshot_filename = f"{isp}_{address}_{location_id}.png"
                screenshot_path = f"A:/KCDD/KCMO Challenge/MDU/{screenshot_filename}"  # Adjust the path as necessary
                driver.save_screenshot(screenshot_path)
                print(f"Screenshot saved for address {street} under {screenshot_path}")

    driver.quit()

# Save the updated DataFrame back to the CSV file and quit the driver
if __name__ == "__main__":
    main()

print("Database updated successfully.")

