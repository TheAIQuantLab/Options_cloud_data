from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import pandas as pd
import time

# Set up Selenium (headless Chrome)
options = Options()
options.add_argument("--headless")  # Remove this line if you want to see the browser
driver = webdriver.Chrome(options=options)

# Go to the MEFF options page
driver.get("https://www.meff.es/esp/Derivados-Financieros/Ficha/FIEM_MiniIbex_35")
time.sleep(3)  # Wait for JS to load the table

# Find the table by ID
table = driver.find_element(By.ID, "tblOpciones")
rows = table.find_elements(By.TAG_NAME, "tr")

# Extract rows into a list
data = []
for row in rows:
    cols = [col.text.strip().replace(",", ".") for col in row.find_elements(By.TAG_NAME, "td")]
    if cols:  # Skip empty rows
        data.append(cols)

# Convert to DataFrame
df = pd.DataFrame(data)
print(df)

driver.quit()
