from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time
from bs4 import BeautifulSoup
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Setup Selenium
chrome_driver_path = r"C:\Users\richardjoeld\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe"
options = Options()
options.add_experimental_option("detach", True)
service = Service(chrome_driver_path)
driver = webdriver.Chrome(service=service, options=options)

# Step 1: Login
driver.get("https://research.cdsalpha.com/Account/LogOn?ReturnUrl=%2f")
driver.find_element(By.ID, "UserName").send_keys("CDS1511")
driver.find_element(By.ID, "Password").send_keys("confidential")
driver.find_element(By.CSS_SELECTOR, "input.btn.btn-info").click()



# Step 3: Navigate to Entities page
driver.get("https://research.cdsalpha.com/EPS/Admin/Entities")
input("Once the Entities page is fully loaded with filters applied, press ENTER to begin scraping...")

# Step 4: Scrape
data = []
current_page = 1

def get_first_row_id(driver):
    """Get the Matrix ID of the first row."""
    try:
        first_row_id = driver.execute_script("""
            const firstRow = document.querySelector("#tblQueue tbody tr td[data-entityid]");
            return firstRow ? firstRow.getAttribute("data-entityid") : null;
        """)
        return first_row_id
    except Exception as e:
        print(f"Error fetching first row ID: {e}")
        return None

while True:
    print(f"\n📄 Scraping page {current_page}...")

    # Wait for table to load
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "#tblQueue tbody tr"))
    )

    # Extract rows
    rows_html = driver.execute_script("""
        const rows = document.querySelectorAll("#tblQueue tbody tr");
        return Array.from(rows).map(row => row.innerHTML);
    """)

    if not rows_html:
        print("✅ No rows found. Stopping.")
        break

    for i, row_html in enumerate(rows_html, start=1):
        row_soup = BeautifulSoup(f"<tr>{row_html}</tr>", "html.parser")
        cols = row_soup.find_all("td")
        if len(cols) >= 5:
            try:
                matrix_id = cols[4].get("data-entityid", "")
                raw_html = cols[4].get("data-original-title") or ""
                parsed_title = BeautifulSoup(raw_html, "html.parser")
                entity_name = parsed_title.find("span", class_="primary-title").text.strip()
            except Exception as e:
                print(f"⚠️ Error parsing row {i}: {e}")
                matrix_id = ""
                entity_name = ""
            print(f"✅ Matrix ID: {matrix_id}, Entity Name: {entity_name}")
            data.append([matrix_id, entity_name])
        else:
            print(f"⚠️ Skipping row {i}: Not enough columns")

        # Step 4.5: Pagination Handling
    try:
        first_row_before = get_first_row_id(driver)
        print(f"🔎 First row Matrix ID before clicking next page: {first_row_before}")

        # Check if next page number exists
        next_page_number = current_page + 1

        pagination_numbers = driver.find_elements(By.CSS_SELECTOR, "div.dataTables_paginate ul li a")
        found_next = False

        for page_link in pagination_numbers:
            if page_link.text.strip() == str(next_page_number):
                found_next = True
                driver.execute_script("arguments[0].scrollIntoView();", page_link)
                time.sleep(0.5)  # slight wait
                driver.execute_script("arguments[0].click();", page_link)
                print(f"➡️ Clicked page {next_page_number}")
                break

        if not found_next:
            print("🚫 No further pages available. Stopping.")
            break

        # Wait for first row Matrix ID to change
        WebDriverWait(driver, 20).until(
            lambda d: get_first_row_id(d) != first_row_before
        )

        print("✅ Successfully moved to next page.")

        current_page += 1
        time.sleep(1)

    except Exception as e:
        print(f"🚫 Pagination error: {e}")
        break
# Step 5: Save to CSV
if data:
    df = pd.DataFrame(data, columns=["Matrix ID", "Entity Name"])
    df.to_csv("matrix_data.csv", index=False)
    print("✅ Data saved to matrix_data.csv")
else:
    print("⚠️ No data collected.")

# Cleanup
driver.quit()
