import pandas as pd
import requests
import time
from config import ROOT_DIR, EXCHANGE_API_KEY
from pathlib import Path

# === Setup Paths ===
root_dir = ROOT_DIR
silver_dir = root_dir / "silver_outputs"
bronze_dir = root_dir / "bronze_outputs"
gold_dir = root_dir / "gold_outputs"
gold_dir.mkdir(parents=True, exist_ok=True)

# === Load Silver Layer Tables ===
silver_expenses = pd.read_parquet(silver_dir / "silver_expenses.parquet")
silver_budgets = pd.read_parquet(silver_dir / "silver_budgets.parquet")

# === Fetch Unique Currencies for FX Conversion ===
API_KEY = EXCHANGE_API_KEY
unique_currencies = silver_expenses["currency"].unique()

fx_rates = []

for currency in unique_currencies:
    if currency == "EUR":
        fx_rates.append({"currency": "EUR", "fx_to_eur": 1.0})
        print(f"✅ Skipping EUR (rate = 1.0)")
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
                print(f"⚠ EUR not found in rates for {currency}")
        else:
            print(f"❌ API error {response.status_code} for {currency}")
    except Exception as e:
        print(f"❌ Request failed for {currency}: {e}")
    
    time.sleep(1)

# === Convert Local Amounts to EUR ===
fx_df = pd.DataFrame(fx_rates)
expenses_fx = silver_expenses.merge(fx_df, on="currency", how="left")
expenses_fx["amount_eur"] = expenses_fx["amount_local"] * expenses_fx["fx_to_eur"]

# === Merge Budgets and Expenses into gold_fact ===
fact_budget = silver_budgets[["date", "project_code", "department", "category", "budget_eur"]]
fact_expense = expenses_fx[["date", "project_code", "department", "category", "amount_eur"]]

gold_fact = pd.merge(fact_budget, fact_expense,
                     on=["date", "project_code", "department", "category"],
                     how="outer")

gold_fact["budget_eur"] = gold_fact["budget_eur"].fillna(0)
gold_fact["amount_eur"] = gold_fact["amount_eur"].fillna(0)

# === Retrieve Project Metadata from Bronze ===
# Although we follow medallion architecture, project metadata is clean and static,
# so it's acceptable to reuse from Bronze layer directly for dimension building.
bronze_projects = pd.read_parquet(bronze_dir / "bronze_projects.parquet")
dim_project = bronze_projects[["project_code", "country", "name"]].drop_duplicates()

# Merge into fact
gold_fact = gold_fact.merge(dim_project, on="project_code", how="left")

# === Create dim_date Table ===
date_range = pd.date_range(start="2023-01-01", end="2025-12-31", freq="MS")
dim_date = pd.DataFrame({
    "date": date_range,
    "month_id": date_range.strftime("%b %Y"),
    "year": date_range.year,
    "month": date_range.month,
    "month_name": date_range.strftime("%B"),
})

# === Save Final Gold Layer Outputs ===
gold_fact.to_parquet(gold_dir / "gold_fact.parquet", index=False)
dim_date.to_parquet(gold_dir / "dim_date.parquet", index=False)
dim_project.to_parquet(gold_dir / "dim_project.parquet", index=False)

# === Summary Output ===
print("✅ Gold Layer completed and saved.")
print(gold_fact.head())
print(dim_date.head())
print(dim_project.head())