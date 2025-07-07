import pandas as pd
from pathlib import Path
from config import ROOT_DIR, EXCHANGE_API_KEY

# Load Bronze Layer
root_dir = ROOT_DIR
bronze_dir = root_dir / "bronze_outputs"

bronze_expenses = pd.read_parquet(bronze_dir / "bronze_expenses.parquet")
bronze_budgets = pd.read_parquet(bronze_dir / "bronze_budgets.parquet")

# === Clean bronze_expenses ===
silver_expenses = bronze_expenses.copy()

# Fix types
silver_expenses["year"] = silver_expenses["year"].astype(int)
silver_expenses["month"] = silver_expenses["month"].astype(int)

# Create date column
silver_expenses["date"] = pd.to_datetime(
    silver_expenses["year"].astype(str) + "-" + silver_expenses["month"].astype(str).str.zfill(2) + "-01"
)

# Normalize text columns
silver_expenses["department"] = silver_expenses["department"].str.strip().str.title()
silver_expenses["category"] = silver_expenses["category"].str.strip().str.title()

# === Clean bronze_budgets ===
silver_budgets = bronze_budgets.copy()

# Fix types
silver_budgets["year"] = silver_budgets["year"].astype(int)
silver_budgets["month"] = silver_budgets["month"].astype(int)

# Create date column
silver_budgets["date"] = pd.to_datetime(
    silver_budgets["year"].astype(str) + "-" + silver_budgets["month"].astype(str).str.zfill(2) + "-01"
)

# Normalize text columns
silver_budgets["department"] = silver_budgets["department"].str.strip().str.title()
silver_budgets["category"] = silver_budgets["category"].str.strip().str.title()

# === Save Silver Layer ===
silver_dir = root_dir / "silver_outputs"
silver_dir.mkdir(parents=True, exist_ok=True)

silver_expenses.to_parquet(silver_dir / "silver_expenses.parquet", index=False)
silver_budgets.to_parquet(silver_dir / "silver_budgets.parquet", index=False)

print("✅ Silver Layer Saved")
print(silver_expenses.head())
print(silver_budgets.head())