import pandas as pd
import requests
import time
from pathlib import Path
from config import ROOT_DIR, EXCHANGE_API_KEY

# === Load silver_expenses ===
root_dir = ROOT_DIR
silver_dir = root_dir / "silver_outputs"
silver_expenses = pd.read_parquet(silver_dir / "silver_expenses.parquet")

# === Setup ===
API_KEY = EXCHANGE_API_KEY
unique_currencies = silver_expenses["currency"].unique()

fx_rates = []

for currency in unique_currencies:
    if currency == "EUR":
        fx_rates.append({"currency": "EUR", "fx_to_eur": 1.0})
        print(f"✅ Skipping conversion for EUR (rate = 1.0)")
        continue

    try:
        url = f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/{currency}"
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            rate = data["conversion_rates"].get("EUR")
            if rate:
                fx_rates.append({"currency": currency, "fx_to_eur": rate})
                print(f"✅ {currency}: 1 {currency} = {rate} EUR")
            else:
                print(f"⚠ EUR not in response for {currency}")
        else:
            print(f"❌ API error {response.status_code} for {currency}")
    except Exception as e:
        print(f"❌ Request failed for {currency}: {e}")
    
    time.sleep(1)  # Respect API rate limits

# === Output the result
fx_df = pd.DataFrame(fx_rates)
print("\n📊 Exchange Rates Fetched:")
print(fx_df)