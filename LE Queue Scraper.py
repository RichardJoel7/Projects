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
driver.get("https://research.relsci.com/Account/LogOn?ReturnUrl=%2f")
driver.find_element(By.ID, "UserName").send_keys("CDS1511")
driver.find_element(By.ID, "Password").send_keys("123456789z$")
driver.find_element(By.CSS_SELECTOR, "input.btn.btn-info").click()

# Step 2: Wait for MFA manually
input("After entering MFA manually in browser, press ENTER here to continue...")

# Step 3: Navigate to Entities page
driver.get("https://research.relsci.com/EPS/Admin/Entities")
input("Once the Entities page is fully loaded with filters applied, press ENTER to begin scraping...")

# Step 4: Scrape using Selenium + JavaScript DOM
data = []
page_num = 1

while True:
    print(f"📄 Scraping page {page_num}...")

    # Wait for table to load
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "#tblQueue tbody tr"))
    )

    # Extract rows using JavaScript
    rows_html = driver.execute_script("""
        const rows = document.querySelectorAll("#tblQueue tbody tr");
        return Array.from(rows).map(row => row.innerHTML);
    """)

    if not rows_html:
        print("✅ No rows found on this page. Stopping.")
        break

    for i, row_html in enumerate(rows_html, start=1):
        row_soup = BeautifulSoup(f"<tr>{row_html}</tr>", "html.parser")
        cols = row_soup.find_all("td")
        print(f"Row {i} has {len(cols)} columns")

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

    # Step 4.5: Pagination Handling (Improved Logic)
    try:
        current_page = driver.execute_script("""
            const active = document.querySelector("div.dataTables_paginate li.active a");
            return active ? parseInt(active.innerText) : null;
        """)

        next_page_text = driver.execute_script("""
            const active = document.querySelector("div.dataTables_paginate li.active");
            const nextLi = active?.nextElementSibling;
            return nextLi?.innerText ?? null;
        """)

        if not next_page_text or "Last" in next_page_text:
            print("🚫 Last page reached.")
            break

        print(f"➡️ Clicking to go to page {next_page_text}...")

        driver.execute_script("""
            const active = document.querySelector("div.dataTables_paginate li.active");
            const nextA = active?.nextElementSibling?.querySelector("a");
            nextA?.click();
        """)

        # Wait until the page number actually changes
        WebDriverWait(driver, 10).until(lambda d: int(d.execute_script("""
            const active = document.querySelector("div.dataTables_paginate li.active a");
            return active ? parseInt(active.innerText) : 0;
        """)) > current_page)

        page_num += 1

    except Exception as e:
        print(f"🚫 Pagination error: {e}")
        break

# Step 5: Save to CSV
if data:
    df = pd.DataFrame(data, columns=["Matrix ID", "Entity Name"])
    df.to_csv("LE_Mozenda.csv", index=False)
    print("✅ Data saved to matrix_data.csv")
else:
    print("⚠️ No data collected.")

# Cleanup
driver.quit()