# Load cleaned CSV into SQL Server (DB: finalexam) using pyodbc
# pip install pandas pyodbc

import pandas as pd
import numpy as np
import pyodbc

# -------- CONFIG --------
CSV_PATH = r"C:\Users\jyoro\Downloads\pumf_clean.csv"
SERVER   = r"Jyoti\SQLEXPRESS"
DATABASE = "finalexam"
TABLE    = "dbo.PUMF_Clean"
DRIVERS  = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"]

def connect_to_db():
    last_err = None
    for drv in DRIVERS:
        try:
            print(f"Trying driver: {drv}")
            conn = pyodbc.connect(
                f"DRIVER={{{drv}}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;",
                autocommit=False
            )
            print(f"Connected using {drv}")
            return conn
        except Exception as e:
            last_err = e
            continue
    raise last_err

def load_data():
    # 1) Read cleaned CSV
    df = pd.read_csv(CSV_PATH, low_memory=False)
    print("Loaded cleaned CSV:", CSV_PATH, "shape=", df.shape)

    # 2) Connect to SQL Server (finalexam)
    conn = connect_to_db()
    cur = conn.cursor()

    # 3) Build INSERT statement dynamically from CSV columns
    cols = list(df.columns)
    collist = ", ".join("[" + c + "]" for c in cols)
    placeholders = ", ".join(["?"] * len(cols))
    insert_sql = f"INSERT INTO {TABLE} ({collist}) VALUES ({placeholders})"

    # 4) Convert NaN -> None (so they become SQL NULL)
    rows = [
        tuple(None if (isinstance(v, float) and np.isnan(v)) else v for v in row)
        for row in df.itertuples(index=False, name=None)
    ]

    # 5) Fast insert
    cur.fast_executemany = True
    cur.executemany(insert_sql, rows)
    conn.commit()
    cur.close()
    conn.close()
    print(f"Insert complete: {len(rows)} rows into {DATABASE}.{TABLE}")

if __name__ == "__main__":
    load_data()
