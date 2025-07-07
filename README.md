# MSF Data Engineering Project

This project implements a full medallion architecture (Bronze → Silver → Gold) for reporting on multi-country budget vs. actual expenses. The final output is a Power BI-ready dataset.

---

## 📁 Directory Structure

```
.
├── config.py             # Configurable paths and secrets (excluded from Git)
├── bronze_layer.py       # Raw ingestion from DBs and CSVs
├── silver_layer.py       # Cleaning and normalization
├── gold_layer.py         # Currency conversion and data modeling
├── requirements.txt      # Python dependencies
├── README.md             # Project documentation
```

---

## 🔧 Setup Instructions

1. **Install dependencies**

```bash
pip install -r requirements.txt
```

2. **Create a `config.py` file in the project root**

```python
# config.py (excluded from version control)
from pathlib import Path

ROOT_DIR = Path(r"C:/Users/YOUR_NAME/OneDrive/Documents/MSF Project/Files")
EXCHANGE_API_KEY = "your-api-key-here"
```

---

## 🚀 Execution Steps

Run the following scripts in order to execute the full medallion pipeline:

```bash
python bronze_layer.py
python silver_layer.py
python gold_layer.py
```

This will generate:

- `bronze_outputs/*.parquet` → raw data
- `silver_outputs/*.parquet` → cleaned and typed data
- `gold_outputs/*.parquet` → modeled and currency-normalized dataset

These can be directly imported into Power BI.

---

## ✅ Best Practices Implemented

- Modular medallion architecture
- Environment-based configuration via `config.py`
- .gitignore used to prevent tracking:
  - Secrets (config.py, .env)
  - Data files (.parquet, .csv, .db)
  - Images and outputs (*.png)
- Secure handling of API keys (never stored in main scripts)
- Clean and typed outputs with Parquet
- Logging and print status for traceability
- Power BI optimized dimensional modeling (`dim_date`, `dim_project`, `gold_fact`)
---

## 🔭 Future-Readiness & Production Considerations

Although time constraints limited full implementation, here’s what I would do in a production setup:

### 🧱 Architecture & Orchestration
- Implement a `main.py` orchestrator to run all steps in sequence
- Use a scheduler like **Apache Airflow** or **Prefect** for DAG execution and monitoring

### 🧪 Testing & Validation
- Add data validation checks (schema validations on the table for example)
- Unit tests for transformation logic with `pytest`

## 🔒 Security Notes

- API keys are stored in `config.py`, which is excluded from Git
- .gitignore used to prevent tracking:
  - Secrets (config.py, .env)
  - Data files (.parquet, .csv, .db)
  - Images and outputs (*.png)
- Always avoid hardcoding secrets or paths in scripts
- Sensitive configuration should be handled via `.env` or secret managers in production