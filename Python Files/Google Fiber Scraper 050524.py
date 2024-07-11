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
import sys
import logging

shutdown_flag = False

def connect_to_db():
    engine = create_engine(
        'postgresql+psycopg2://postgres:Ellipsispostgres42$@localhost/BDCFabric',
        #echo=True,  # Log SQL statements (for debugging purposes; disable in production)
        pool_size=10,  # Number of connections to maintain in the pool
        max_overflow=20,  # Allow up to 30 connections in total (10 + 20)
        pool_timeout=30,  # Number of seconds to wait before giving up on returning a connection
        pool_recycle=1800  # Recycle connections after 30 minutes
    )
    return engine

def signal_handler(signum, frame):
    global shutdown_flag
    shutdown_flag = True
    print("Shutdown signal received. Stopping threads...")

# Set up signal handling
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def fetch_addresses():
    df = pd.read_csv('addresses.csv')
    return df


def update_database_gf(address_primary, zip, eligible_gf, no_service_gf, need_unit_gf, has_account_gf, unknown_gf, business_gf):
    engine = connect_to_db()
    # Create a connection object
    with engine.connect() as conn:
        # Begin a transaction
        with conn.begin():
            # Define the SQL update statement
            query = text("""
                UPDATE public.broadband_kcmo_served_gf_ordered
                SET eligible_gf = :eligible_gf, no_service_gf = :no_service_gf, need_unit_gf = :need_unit_gf, has_account_gf = :has_account_gf, unknown_gf = :unknown_gf, business_gf = :business_gf
                WHERE address_primary = :address_primary AND zip = :zip;
            """)
            # Execute the update statement with parameters
            conn.execute(query, {
                'eligible_gf': eligible_gf,
                'no_service_gf': no_service_gf,
                'need_unit_gf': need_unit_gf,
                'has_account_gf': has_account_gf,
                'unknown_gf': unknown_gf,
                'business_gf': business_gf,
                'address_primary': address_primary,
                'zip': zip
            })

def update_database_xf(address_primary, zip, eligible_xf, no_service_xf, need_unit_xf, has_account_xf, unknown_xf):
    engine = connect_to_db()
    # Create a connection object
    with engine.connect() as conn:
        # Begin a transaction
        with conn.begin():
            # Define the SQL update statement
            query = text("""
                UPDATE public.scraper_test
                SET eligible_xf = :eligible_xf, no_service_xf = :no_service_xf, need_unit_xf = :need_unit_xf, has_account_xf = :has_account_xf, unknown_xf = :unknown_xf
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
                'zip': zip
            })

def check_address(row):
    if shutdown_flag:
        if driver:
            driver.quit()
        return "Operation cancelled by user"

    driver = None
    
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-dev-shm-usage')  # prevent Chrome crashes
    chrome_options.add_argument('--no-sandbox')  # bypass OS security model, required by Docker if used
    chrome_options.add_argument('--disable-gpu')  # applicable to windows os only
    chrome_options.add_argument('--disable-extensions')  # disabling extensions
    chrome_options.add_argument('--disable-logging')  # should disable additional logging
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])  # suppresses logging
    chrome_options.add_argument('--log-level=3')  # This sets the logging level to only include fatal errors
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])  # This suppresses additional logging
    service = Service('C:\Program Files\Chrome Driver\chromedriver.exe',log_path=os.devnull)
    driver = webdriver.Chrome(service=service, options=chrome_options)

    location_id = row['location_id']
    isp = row['brand_name']           # ISP (Internet Service Provider) name
    street = row['address_primary']   # Street address
    city = row['city']                # City
    #zipcode = str(row['zip']).strip() # ZIP code, converted to string and stripped of any excess whitespace
    zipcode = row['zip']
    address = row['address_full']     # Full address
    eligible_gf = row['eligible_gf']

    if shutdown_flag:
        if driver:
            driver.quit()
        return "Operation cancelled by user"

    try:
        if isp == "Google Fiber" and eligible_gf is None:
            driver.get('https://fiber.google.com/db/')
            time.sleep(1)

            try:
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

                if shutdown_flag:
                    if driver:
                        driver.quit()
                    return "Operation cancelled by user"

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

                if shutdown_flag:
                    if driver:
                        driver.quit()
                    return "Operation cancelled by user"
                
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
            eligible_gf = "You’re eligible to get Google Fiber Internet." in driver.page_source
            no_service_gf = ('Google Fiber isn’t available for this area' in driver.page_source) or ('street-address text-input-error ng-star-inserted' in driver.page_source)
            need_unit_gf = "unit-number text-input-error ng-star-inserted" in driver.page_source
            has_account_gf = "This address has a Google Fiber account" in driver.page_source
            business_gf = "Business 1 Gig" in driver.page_source
            unknown_gf = not any([eligible_gf, no_service_gf, need_unit_gf, has_account_gf, business_gf])

            update_database_gf(street, zipcode, eligible_gf, no_service_gf, need_unit_gf, has_account_gf, unknown_gf, business_gf)

            if shutdown_flag:
                if driver:
                    driver.quit()
                return "Operation cancelled by user"

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
                    return_statement=f"Updated database for {address}. Status is {status}"

            # Screenshot logic
            if no_service_gf or unknown_gf:
                # Define the filename for the screenshot
                screenshot_filename = f"{isp}_{address}_{location_id}.png"
                screenshot_path = f"A:/KCDD/KCMO Challenge/{screenshot_filename}"  # Adjust the path as necessary
                driver.save_screenshot(screenshot_path)
                print(f"Screenshot saved for address {street} under {screenshot_path}")   

            driver.quit()
            return return_statement
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

    with ThreadPoolExecutor(max_workers=10) as executor:  # Adjust number of workers as needed
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
    data.to_csv('updated_addresses.csv', index=False)


# Save the updated DataFrame back to the CSV file and quit the driver
if __name__ == "__main__":
    main()

print("Spreadsheet updated successfully.")

