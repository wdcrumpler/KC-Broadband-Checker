import time
import psycopg2
import pandas as pd
from bs4 import BeautifulSoup
import re
import psycopg2
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.select import Select
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.support import expected_conditions as EC
from docx import Document
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import signal
import tempfile
import sys
import logging
import urllib.parse

shutdown_flag = False


'''def connect_to_db():
    engine = create_engine('postgresql+psycopg2://postgres:Ellipsispostgres42$@localhost/BDCFabric')
    return engine'''

def connect_to_db():
    engine = create_engine(
        'postgresql+psycopg2://postgres:Ellipsispostgres42$@localhost/BDCFabric',
        #echo=True,  # Log SQL statements (for debugging purposes; disable in production)
        pool_size=1,  # Number of connections to maintain in the pool
        max_overflow=20,  # Allow up to 30 connections in total (10 + 20)
        pool_timeout=30,  # Number of seconds to wait before giving up on returning a connection
        pool_recycle=1800  # Recycle connections after 30 minutes
    )
    return engine

def fetch_addresses():
    engine = connect_to_db()
    query = "SELECT location_id, brand_name, address_primary, city, zip, address_full, checked_gf FROM public.broadband_kcmo_served_gf_ordered;"
    df = pd.read_sql(query, engine)
    return df

def update_database_gf(location_id, brand_name, eligible_gf, no_service_gf, need_unit_gf, has_account_gf, unknown_gf, business_gf, unit, checked_gf):
    engine = connect_to_db()
    # Create a connection object
    with engine.connect() as conn:
        # Begin a transaction
        with conn.begin():
            # Define the SQL update statement
            query = text("""
                UPDATE public.broadband_kcmo_mdu
                SET eligible_gf = :eligible_gf, no_service_gf = :no_service_gf, need_unit_gf = :need_unit_gf, has_account_gf = :has_account_gf, unknown_gf = :unknown_gf, business_gf = :business_gf, unit = :unit, checked_gf = :checked_gf
                WHERE location_id = :location_id AND brand_name = :brand_name;
            """)
            # Execute the update statement with parameters
            conn.execute(query, {
                'eligible_gf': eligible_gf,
                'no_service_gf': no_service_gf,
                'need_unit_gf': need_unit_gf,
                'has_account_gf': has_account_gf,
                'unknown_gf': unknown_gf,
                'business_gf': business_gf,
                'location_id': location_id,
                'brand_name': brand_name,
                'unit': unit,
                'checked_gf': checked_gf
            })

def update_database_xf(address_primary, zip, eligible_xf, no_service_xf, need_unit_xf, has_account_xf, unknown_xf, unit):

    engine = connect_to_db()
    # Create a connection object
    with engine.connect() as conn:
        # Begin a transaction
        with conn.begin():
            # Define the SQL update statement
            query = text("""
                UPDATE public.scraper_test
                SET eligible_xf = :eligible_xf, no_service_xf = :no_service_xf, need_unit_xf = :need_unit_xf, has_account_xf = :has_account_xf, unknown_xf = :unknown_xf, unit = :unit
                WHERE address_primary = :address_primary AND zip = :zip;
            """)
            # Execute the update statement with parameters
            conn.execute(query, {
                'eligible_xf': eligible_xf,
                'no_service_xf': no_service_xf,
                'need_unit_xf': need_unit_xf,
                'has_account_xf': has_account_xf,
                'unknown_xf': unknown_xf,
                'address_primary': address_primary,
                'zip': zip,
                'unit': unit
            })

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

def check_address(index, row):
    if shutdown_flag:
        if driver:
            driver.quit()
        return "Operation cancelled by user"
    
    driver = None
    
    # Set up Selenium Chrome driver
    chrome_options = Options()
    #chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-dev-shm-usage')  # prevent Chrome crashes
    chrome_options.add_argument('--no-sandbox')  # bypass OS security model, required by Docker if used
    chrome_options.add_argument('--disable-gpu')  # applicable to windows os only
    chrome_options.add_argument('--disable-extensions')  # disabling extensions
    chrome_options.add_argument("--remote-debugging-port=9222")  # specify a fixed port for debugging
    chrome_options.add_argument('--disable-logging')  # should disable additional logging
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])  # suppresses logging
    chrome_options.add_argument('--log-level=3')  # This sets the logging level to only include fatal errors
    user_data_dir = f"--user-data-dir={tempfile.mkdtemp()}"
    chrome_options.add_argument(user_data_dir)
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])  # This suppresses additional logging
    service = Service('C:\Program Files\Chrome Driver\chromedriver.exe',log_path=os.devnull)
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        # Extract each data point into a variable
        isp = row['brand_name']           # ISP (Internet Service Provider) name
        street = row['address_primary']   # Street address
        city = row['city']                # City
        #zipcode = str(row['zip']).strip() # ZIP code, converted to string and stripped of any excess whitespace
        zipcode = row['zip']
        address = row['address_full']     # Full address
        location_id = row['location_id']
        brand_name = row['brand_name']
        checked_gf = row['checked_gf']

        if shutdown_flag:
            if driver:
                driver.quit()
            return "Operation cancelled by user"
        
        if isp == "Google Fiber" and checked_gf is not True:
            unit = None
            
            try: 
                driver.get('https://fiber.google.com/db/')
                time.sleep(1)

                # Find and fill the address and zip fields, then submit the form
                submitButtonArray = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, "//button[contains(., 'Check availability')]")))
                submit_button =  submitButtonArray[1]
                driver.execute_script("arguments[0].click();", submit_button)
                time.sleep(1)
                addressInputArray = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.NAME, 'street_address')))
                address_field = addressInputArray[0]
                address_field.click()
                address_field.clear()
                address_field.send_keys(street)
                #driver.execute_script("arguments[0].value = arguments[1];", address_field, address)
                time.sleep(1)
                # Ensuring the zip code is clean and displayed for debugging

                if shutdown_flag:
                    if driver:
                        driver.quit()
                    return "Operation cancelled by user"

            except Exception as e:
                print("An error occurred bringing up initial address entry field:", e)
                # Consider taking a screenshot or logging the current page HTML
                debug_screenshot_filename = f"{street}_{isp}_address_entry_field.png"
                debug_screenshot_path = f"A:/KCDD/KCMO Challenge/Debug/{debug_screenshot_filename}"  # Adjust the path as necessary
                driver.save_screenshot(debug_screenshot_path)

            try:
                zipInputArray = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.NAME, 'zip_code'))
                )
                zip_field = zipInputArray[0]
                zip_field.click()
                zip_field.clear()
                zip_field.send_keys(zipcode)

                # Additional explicit wait to check the state of the input or page
                WebDriverWait(driver, 10).until(
                    EC.text_to_be_present_in_element_value((By.NAME, 'zip_code'), zipcode)
                )

                # Click the submit button
                submit_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Check availability')]"))
                )
                driver.execute_script("arguments[0].click();", submit_button)

            except Exception as e:
                print("An error occurred submitting address:", e)
                # Consider taking a screenshot or logging the current page HTML
                driver.save_screenshot(f"debug_screenshot.png")
                debug_screenshot_filename_2 = f"{street}_{isp}_address_submission.png"
                debug_screenshot_path_2 = f"A:/KCDD/KCMO Challenge/Debug/{debug_screenshot_filename_2}"  # Adjust the path as necessary
                driver.save_screenshot(debug_screenshot_path_2)

            try:
                submit_button =  submitButtonArray[2]
                driver.execute_script("arguments[0].click();", submit_button)
                
            except StaleElementReferenceException:
                time.sleep(2)
                submit_button =  submitButtonArray[2]
                driver.execute_script("arguments[0].click();", submit_button)
            
            # Wait for page to load, then grab page source
            time.sleep(2)
            wait = WebDriverWait(driver, 10)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            #content = driver.page_source

            # Update spreadsheet according to the content found on the page.

            if "unit-number text-input-error ng-star-inserted" in driver.page_source:
                unit = mdu_xf(driver, street, zipcode, address)
                need_unit_gf = True
                #print(f"The unit is {unit}")

                if unit is None:
                    
                    need_unit_gf = True
                    eligible_gf = False
                    no_service_gf = False
                    has_account_gf = False
                    business_gf = False
                    unknown_gf = True
                    checked_gf = True

                    update_database_gf(location_id, brand_name, eligible_gf, no_service_gf, need_unit_gf, has_account_gf, unknown_gf, business_gf, unit, checked_gf)

                    address_status = {
                        "Eligible": eligible_gf,
                        "No Service": no_service_gf,
                        "Need Unit Number": need_unit_gf,
                        "Has Account": has_account_gf,
                        "Business Address": business_gf,
                        "Unknown": unknown_gf
                    }

                    for status, check in address_status.items():
                        if check:
                            print(f"Updated database for {address}. Status is {status}")

                    # Screenshot logic
                    if no_service_gf:
                        # Define the filename for the screenshot
                        screenshot_filename = f"{isp}_{address}_{location_id}.png"
                        screenshot_path = f"A:/KCDD/KCMO Challenge/MDU/{screenshot_filename}"  # Adjust the path as necessary
                        driver.save_screenshot(screenshot_path)
                        print(f"Screenshot saved for address {street} under {screenshot_path}")   

                    return None

                else:    
                    try: 
                        driver.get('https://fiber.google.com/db/')
                        time.sleep(1)

                        # Find and fill the address and zip fields, then submit the form
                        submitButtonArray = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, "//button[contains(., 'Check availability')]")))
                        submit_button =  submitButtonArray[1]
                        driver.execute_script("arguments[0].click();", submit_button)
                        time.sleep(1)
                        addressInputArray = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.NAME, 'street_address')))
                        address_field = addressInputArray[0]
                        address_field.click()
                        address_field.clear()
                        address_field.send_keys(street)

                        unitInputArray = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.NAME, 'unit_number')))
                        unit_field = unitInputArray[0]
                        unit_field.click()
                        unit_field.clear()
                        unit_field.send_keys(unit)

                        zipInputArray = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.NAME, 'zip_code')))
                        zip_field = zipInputArray[0]
                        zip_field.click()
                        zip_field.clear()
                        zip_field.send_keys(zipcode)

                        # Additional explicit wait to check the state of the input or page
                        WebDriverWait(driver, 10).until(
                            EC.text_to_be_present_in_element_value((By.NAME, 'zip_code'), zipcode)
                        )

                        # Click the submit button
                        submit_button = WebDriverWait(driver, 10).until(
                            EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Check availability')]"))
                        )
                        driver.execute_script("arguments[0].click();", submit_button)

                    except Exception as e:
                        print("An error occurred submitting the full address with unit number", e)
                        # Consider taking a screenshot or logging the current page HTML
                        debug_screenshot_filename = f"{street}_{isp}_address_entry_field.png"
                        debug_screenshot_path = f"A:/KCDD/KCMO Challenge/Debug/{debug_screenshot_filename}"  # Adjust the path as necessary
                        driver.save_screenshot(debug_screenshot_path)

                    try:
                        submit_button =  submitButtonArray[2]
                        driver.execute_script("arguments[0].click();", submit_button)
                        
                    except StaleElementReferenceException:
                        time.sleep(2)
                        submit_button =  submitButtonArray[2]
                        driver.execute_script("arguments[0].click();", submit_button)
                    
                    # Wait for page to load, then grab page source
                    time.sleep(2)
                    wait = WebDriverWait(driver, 10)
                    wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
                    #content = driver.page_source
            
            else:
                need_unit_gf = False

            eligible_gf = "You’re eligible to get Google Fiber Internet." in driver.page_source
            no_service_gf = ('Google Fiber isn’t available for this area' in driver.page_source) or ('street-address text-input-error ng-star-inserted' in driver.page_source)
            has_account_gf = "This address has a Google Fiber account" in driver.page_source
            business_gf = "Business 1 Gig" in driver.page_source
            unknown_gf = not any([eligible_gf, no_service_gf, need_unit_gf, has_account_gf, business_gf])
            checked_gf = True

            update_database_gf(location_id, brand_name, eligible_gf, no_service_gf, need_unit_gf, has_account_gf, unknown_gf, business_gf, unit, checked_gf)

            address_status = {
                "Eligible": eligible_gf,
                "No Service": no_service_gf,
                "Need Unit Number": need_unit_gf,
                "Has Account": has_account_gf,
                "Business Address": business_gf,
                "Unknown": unknown_gf
            }

            for status, check in address_status.items():
                if check:
                    print(f"Updated database for {address}. Status is {status}")

            # Screenshot logic
            if no_service_gf:
                # Define the filename for the screenshot
                screenshot_filename = f"{isp}_{address}_{location_id}.png"
                screenshot_path = f"A:/KCDD/KCMO Challenge/MDU/{screenshot_filename}"  # Adjust the path as necessary
                driver.save_screenshot(screenshot_path)
                print(f"Screenshot saved for address {street} under {screenshot_path}")   

    except Exception as e:
        # Handle any exceptions that occur within the function
        error_msg = f"Failed to process {row['address_primary']}: {str(e)}"
        print(error_msg)  # or use logging to record this
        return error_msg  # Optionally return the error message

    finally:
        if driver:
            driver.quit()

def main():
    global data
    data = fetch_addresses()

    with ThreadPoolExecutor(max_workers=1) as executor:  # Adjust number of workers as needed
        # Submit tasks to the executor
        futures = [executor.submit(check_address, i, row) for i, row in data.iterrows()]

        # Collect results (optional, could also handle exceptions or logging here)
        for future in futures:
            if shutdown_flag:
                break
            try:
                result = future.result()
                print(result)
            except Exception as e:
                print(f"An error occurred: {str(e)}")
    

if __name__ == "__main__":
    main()

print("Database file updated successfully.")