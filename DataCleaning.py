import pandas as pd
import numpy as np

# ======================== CONFIG ========================
# Use your GitHub raw OR a local path
# SOURCE = "https://raw.githubusercontent.com/jyoti20-tech/FinalExam/main/DataSource/pumf.csv"
SOURCE = r"C:\Users\jyoro\Downloads\pumf.csv"        # <- change if needed
OUT_PATH = r"C:\Users\jyoro\Downloads\pumf_clean.csv" # <- cleaned CSV saved here

# If you want to keep only some columns, list them here; otherwise keep all:
KEEP_COLS = None 

# Province mapping present in CTNS-style files
PROV_MAP = {
    10: "NL", 11: "PE", 12: "NS", 13: "NB",
    24: "QC", 35: "ON", 46: "MB", 47: "SK",
    48: "AB", 59: "BC", 60: "YT", 61: "NT", 62: "NU"
}
# Common StatCan sentinel codes for “valid skip / not stated / refusal”
SENTINELS = {96, 97, 98, 99}

# Columns that should never be altered when checking negatives, etc.
PROTECTED_COLS = {"PUMFID"}  # add more IDs if needed
# ===================================================================

def step(title):
    print("\n" + "="*88)
    print(title)
    print("="*88)

# ---------------- STEP 0: LOAD ----------------
step("STEP 0: Load")
df = pd.read_csv(SOURCE, low_memory=False)
print(f"Loaded: {SOURCE}  → shape={df.shape}  columns={len(df.columns)}")
print("Head:\n", df.head(3))

# ---------------- STEP 1: IRRELEVANT DATA ----------------
step("STEP 1: Irrelevant Data (drop all-null/constant columns; optional subset; trim text)")
before_cols = df.shape[1]

# (1a) Optional keep-list
if KEEP_COLS is not None:
    missing = [c for c in KEEP_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")
    df = df[KEEP_COLS].copy()

# (1b) Trim whitespace on object/text columns
obj_cols = df.select_dtypes(include=["object"]).columns.tolist()
for c in obj_cols:
    s = df[c].astype(str).str.strip()
    s = s.replace({"": np.nan, "nan": np.nan, "None": np.nan})
    df[c] = s

# (1c) Drop columns that are entirely null
all_null = [c for c in df.columns if df[c].isna().all()]
df = df.drop(columns=all_null) if all_null else df

# (1d) Drop quasi-constant columns (≥ 99.5% same value)
quasi_constant = []
for c in df.columns:
    vc = df[c].value_counts(dropna=False)
    if len(vc) > 0 and (vc.iloc[0] / len(df) >= 0.995):
        quasi_constant.append(c)
df = df.drop(columns=quasi_constant) if quasi_constant else df

after_cols = df.shape[1]
print(f"Dropped all-null cols: {len(all_null)} → {all_null}")
print(f"Dropped quasi-constant cols (≥99.5% same value): {len(quasi_constant)} → {quasi_constant}")
print(f"Columns reduced: {before_cols} → {after_cols}. Shape: {df.shape}")

# ---------------- STEP 2: DUPLICATE RECORDS ----------------
step("STEP 2: Duplicate Records")
if "PUMFID" in df.columns:
    dup_count = int(df.duplicated(subset=["PUMFID"]).sum())
    df = df.drop_duplicates(subset=["PUMFID"], keep="first")
    print(f"Duplicates removed by PUMFID: {dup_count} → shape {df.shape}")
else:
    dup_count = int(df.duplicated().sum())
    df = df.drop_duplicates(keep="first")
    print(f"PUMFID not found. Row-level duplicates removed: {dup_count} → shape {df.shape}")

# ---------------- STEP 3: MISSING VALUES ----------------
step("STEP 3: Missing Values (normalize text 'NA' etc.; convert sentinel codes to NaN)")
# Try numeric conversion for object columns (keeps strings if not numeric)
for c in df.columns:
    if df[c].dtype == "object":
        df[c] = pd.to_numeric(df[c], errors="ignore")

# Convert sentinel codes to NaN across numeric columns (except PROTECTED_COLS)
numeric_cols = [c for c in df.columns if c not in PROTECTED_COLS and pd.api.types.is_numeric_dtype(df[c])]
sentinel_cells = 0
for c in numeric_cols:
    mask = df[c].isin(SENTINELS)
    cnt = int(mask.sum())
    if cnt:
        df.loc[mask, c] = np.nan
        sentinel_cells += cnt
print(f"Sentinel codes replaced with NaN (numeric cells): {sentinel_cells}")
print(f"Total remaining NaNs: {int(df.isna().sum().sum())}")

# ---------------- STEP 4: INCONSISTENT DATA ----------------
step("STEP 4: Inconsistent Data (types, province codes, dates if present)")
# Province codes → numeric → validate set → add ProvinceName
if "PROV_C" in df.columns:
    df["PROV_C"] = pd.to_numeric(df["PROV_C"], errors="coerce")
    invalid_prov = int((~df["PROV_C"].isin(PROV_MAP.keys())).sum())
    df.loc[~df["PROV_C"].isin(PROV_MAP.keys()), "PROV_C"] = np.nan
    df["ProvinceName"] = df["PROV_C"].map(PROV_MAP)
    print(f"Invalid PROV_C set to NaN: {invalid_prov}")
else:
    print("PROV_C not present → skipped province normalization.")

# If a date column like VERDATE exists, parse it and blank impossible values
for date_col in [c for c in df.columns if c.upper().startswith("VERDATE") or c.lower().endswith("date")]:
    before_nat = int(df[date_col].isna().sum()) if not pd.api.types.is_datetime64_any_dtype(df[date_col]) else 0
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    after_nat = int(df[date_col].isna().sum())
    print(f"Parsed {date_col}. NaT after parsing: {after_nat} (was {before_nat})")

# ---------------- STEP 5: NEGATIVE VALUES ----------------
step("STEP 5: Negative Values (set negatives to NaN, except protected columns)")
neg_cells = 0
for c in numeric_cols:
    if c in PROTECTED_COLS:
        continue
    mask = df[c] < 0
    cnt = int(mask.sum())
    if cnt:
        df.loc[mask, c] = np.nan
        neg_cells += cnt
print(f"Negative numeric cells set to NaN: {neg_cells}")

# ---------------- STEP 6: ILLOGICAL VALUES ----------------
step("STEP 6: Illogical Values (basic business rules if columns exist)")
# Example rules; only applied if the columns exist. Adjust or add rules as needed.
if "WTPP" in df.columns:
    nonpos_w = int((df["WTPP"] <= 0).sum())
    df.loc[df["WTPP"] <= 0, "WTPP"] = np.nan
    print(f"WTPP non-positive → NaN: {nonpos_w}")
else:
    print("WTPP not present.")

#Gender
if "GENDER" in df.columns:
    invalid_gender = int((~pd.Series(df["GENDER"]).isin([0,1,2])).sum())  # 0=missing/by design, 1/2=valid
    print(f"GENDER left unchanged. (Invalid codes observed: {invalid_gender})")
else:
    print("GENDER not present.")

# If AGE-like column exists: set impossible ages to NaN
for age_col in [c for c in df.columns if c.upper().startswith("AGE")]:
    try:
        bad_age = int(((df[age_col] < 0) | (df[age_col] > 120)).sum())
        df.loc[(df[age_col] < 0) | (df[age_col] > 120), age_col] = np.nan
        print(f"{age_col}: out-of-range set to NaN: {bad_age}")
    except Exception:
        pass

print(f"Final NaNs total: {int(df.isna().sum().sum())}  | Final shape: {df.shape}")

# ---------------- SAVE CLEANED CSV ----------------
step("SAVE CLEANED CSV")
df.to_csv(OUT_PATH, index=False)
print(f"Saved cleaned file to: {OUT_PATH}")
