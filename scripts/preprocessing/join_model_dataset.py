import os
import pandas as pd
from pathlib import Path

# ==========================================================
# JOIN GPT SENTIMENT + FINBERT FEATURES + PRICE DATA
# Creates: master_opec_price_model_dataset.csv
# ==========================================================

# ---------------------
# Base directories
# ---------------------
BASE = Path(r"C:\Users\wpmpo\OneDrive\Documents\DSSB - SEM 3\Digital Transformation\commodity_analysis 1")

OPEC_DIR = BASE / "data" / "reports" / "energy" / "opec"
PRICE_DIR = BASE / "data" / "processed"
OUTPUT_DIR = BASE / "data" / "processed"
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

GPT_FILE = OPEC_DIR / "opec_comparison_scores_gpt.csv"
FINBERT_FILE = OPEC_DIR / "opec_features_finbert_chunked.csv"
PRICE_FILE = PRICE_DIR / "master_monthly_prices.csv"

OUT_FILE = OUTPUT_DIR / "master_opec_price_model_dataset.csv"


# ==========================================================
# Load and clean data
# ==========================================================

print(" Loading GPT comparison sentiment...")
gpt = pd.read_csv(GPT_FILE)
gpt["date"] = pd.to_datetime(gpt["date"], errors="coerce")
gpt["date"] = gpt["date"] + pd.offsets.MonthEnd(0)   # FIXED alignment to month-end

gpt = gpt[["date", "comparison_score", "tone_change", "summary"]]


print(" Loading FinBERT chunked features...")
fin = pd.read_csv(FINBERT_FILE)

# Convert year + month_name → datetime
fin["date"] = pd.to_datetime(
    fin["year"].astype(str) + "-" + fin["month_name"].astype(str) + "-01",
    errors="coerce"
)
fin["date"] = fin["date"] + pd.offsets.MonthEnd(0)    # FIXED alignment to month-end


print(" Loading monthly price dataset...")
prices = pd.read_csv(PRICE_FILE)
prices["Date"] = pd.to_datetime(prices["Date"], errors="coerce")
prices = prices.rename(columns={"Date": "date"})  # make naming consistent



# ==========================================================
# MERGE 1 — FinBERT + GPT sentiment
# ==========================================================

print("\n Merging FinBERT and GPT sentiment...")
df = pd.merge(
    fin,
    gpt,
    on="date",
    how="left"
)



# ==========================================================
# MERGE 2 — Add Price Data (PP, Brent, WTI, NatGas)
# ==========================================================

print(" Adding PP, Brent, WTI, NatGas prices...")
df = pd.merge(
    df,
    prices,
    on="date",
    how="left"
)


# ==========================================================
# Final Cleaning
# ==========================================================

df = df.sort_values("date").reset_index(drop=True)

# Create NEXT-MONTH targets
df["PP_EU_next_month"] = df["PP_EU"].shift(-1)
df["Brent_next_month"] = df["Brent"].shift(-1)


# ==========================================================
# Select Important Columns
# ==========================================================

cols_to_keep = [
    "date",

    # GPT sentiment
    "comparison_score",

    # FinBERT sentiment
    "finbert_sentiment",
]

# add keyword density columns
keyword_cols = [c for c in df.columns if any(k in c for k in ["supply_", "demand_", "price_"])]
cols_to_keep += keyword_cols

# add market prices
cols_to_keep += ["PP_EU", "Brent", "WTI", "NatGas"]

# add future prediction targets
cols_to_keep += ["PP_EU_next_month", "Brent_next_month"]

final_df = df[cols_to_keep]


# ==========================================================
# Save Output
# ==========================================================

final_df.to_csv(OUT_FILE, index=False)

print("\n MASTER MODELING DATASET SAVED:")
print(OUT_FILE)

print("\n Preview:")
print(final_df.head(12))
