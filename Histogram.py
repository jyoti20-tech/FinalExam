import pandas as pd
import matplotlib.pyplot as plt
import pyodbc
from pathlib import Path

# --------------------- CONFIG ---------------------
SERVER   = r"Jyoti\SQLEXPRESS"
DATABASE = "finalexam"
TABLE    = "dbo.PUMF_Clean"        # table created earlier
OUT_DIR  = Path(r"C:\Users\jyoro\Downloads")  # where to save outputs
PNG_PATH = OUT_DIR / "respondents_by_province_sql.png"
CSV_COUNTS_PATH = OUT_DIR / "respondents_by_province_counts_sql.csv"

# Try these drivers in order
DRIVERS = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"]

# Province code → short name (used if ProvinceName not present)
PROV_MAP = {
    10: "NL", 11: "PE", 12: "NS", 13: "NB",
    24: "QC", 35: "ON", 46: "MB", 47: "SK",
    48: "AB", 59: "BC", 60: "YT", 61: "NT", 62: "NU"
}
# --------------------------------------------------

def connect_sql():
    last = None
    for drv in DRIVERS:
        try:
            print(f"Trying driver: {drv}")
            conn = pyodbc.connect(
                f"DRIVER={{{drv}}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;",
                autocommit=False,
            )
            print(f"Connected using {drv}")
            return conn
        except Exception as e:
            last = e
            continue
    raise last

def main():
    conn = connect_sql()
    # Pull only the columns we need
    q = f"SELECT ProvinceName, PROV_C FROM {TABLE};"
    df = pd.read_sql(q, conn)
    conn.close()

    # Choose label series (prefer ProvinceName if present)
    if "ProvinceName" in df.columns and df["ProvinceName"].notna().any():
        prov = df["ProvinceName"]
    else:
        prov = pd.to_numeric(df["PROV_C"], errors="coerce").map(PROV_MAP)

    # Count respondents by province (unweighted)
    counts = prov.dropna().value_counts().sort_index()

    # Plot a simple labeled bar chart (matplotlib; single plot; no custom colors)
    plt.figure(figsize=(10, 5))
    ax = counts.plot(kind="bar")
    ax.set_xlabel("Province")
    ax.set_ylabel("Respondents (count)")
    ax.set_title("Distribution of Survey Respondents by Province (Unweighted)")
    for idx, value in enumerate(counts.values):
        ax.text(idx, value, str(int(value)), ha="center", va="bottom", rotation=0)
    plt.tight_layout()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    plt.savefig(PNG_PATH, dpi=300, bbox_inches="tight")
    plt.close()

    # Save counts and print the answer
    counts.to_csv(CSV_COUNTS_PATH, header=["count"])
    top_prov = counts.idxmax()
    top_val = int(counts.max())
    print("Saved chart to:", PNG_PATH)
    print("Saved counts to:", CSV_COUNTS_PATH)
    print(f"Top province: {top_prov} with {top_val} respondents.")

if __name__ == "__main__":
    main()
