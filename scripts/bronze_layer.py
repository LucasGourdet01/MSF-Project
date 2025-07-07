import sqlite3
from pathlib import Path
from config import ROOT_DIR, EXCHANGE_API_KEY
import pandas as pd
import pyarrow

# === Setup Paths ===
root_dir = ROOT_DIR

# Define project codes
projects = ["BE01", "BE55", "BF01", "BF02", "KE01", "KE02", "SN01", "SN02"]

# DB and CSV paths
db_paths = {code: root_dir / f"{code}.db" for code in projects}
csv_paths = {
    "BE01": root_dir / "BE01_budget.csv",
    "BE55": root_dir / "BE55_budget.csv",
    "BF01": root_dir / "BF01_budget.csv",
    "BF02": root_dir / "BF02_budget.csv",
    "KE01": root_dir / "KE01_budget.csv",
    "KE02": root_dir / "KEO2_budget.csv",  # Typo preserved
    "SN01": root_dir / "SN01_budget.csv",
    "SN02": root_dir / "SN02_budget.csv"
}

# Optional: check for missing files
for code, path in db_paths.items():
    if not path.exists():
        print(f"❌ Missing DB file: {path}")
for code, path in csv_paths.items():
    if not path.exists():
        print(f"❌ Missing CSV file: {path}")

# === Load from SQLite ===
def load_tables_from_db(project_code, db_path):
    conn = sqlite3.connect(db_path)
    project_df = pd.read_sql_query("SELECT * FROM project", conn)
    expenses_df = pd.read_sql_query("SELECT * FROM expenses", conn)
    conn.close()

    # Tag project_code
    project_df["project_code"] = project_code
    expenses_df["project_code"] = project_code

    return project_df, expenses_df

all_projects = []
all_expenses = []

for code in projects:
    proj_df, exp_df = load_tables_from_db(code, db_paths[code])
    all_projects.append(proj_df)
    all_expenses.append(exp_df)

bronze_projects = pd.concat(all_projects, ignore_index=True)
bronze_expenses = pd.concat(all_expenses, ignore_index=True)

# === Load Budgets from CSV ===
bronze_budgets_list = []

for project_code, path in csv_paths.items():
    df = pd.read_csv(path)
    df["project_code"] = project_code
    bronze_budgets_list.append(df)

bronze_budgets = pd.concat(bronze_budgets_list, ignore_index=True)

# === Save Bronze Outputs ===
output_dir = root_dir / "bronze_outputs"
output_dir.mkdir(parents=True, exist_ok=True)

bronze_expenses.to_parquet(output_dir / "bronze_expenses.parquet", index=False)
bronze_budgets.to_parquet(output_dir / "bronze_budgets.parquet", index=False)
bronze_projects.to_parquet(output_dir / "bronze_projects.parquet", index=False)
# === Preview Output ===
print("✅ Bronze Layer: Extracted & Saved")
print("\nBronze Expenses Sample:")
print(bronze_expenses.head())

print("\nBronze Budgets Sample:")
print(bronze_budgets.head())