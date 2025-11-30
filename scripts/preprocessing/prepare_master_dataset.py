# ============================================
# Prepare Master Monthly Dataset for Analysis
# ============================================
# Combines:
#  - Polypropylene monthly prices (EUR/t)
#  - Crude Oil (Brent & WTI) weekly → monthly
#  - Natural Gas weekly → monthly
# Outputs: data/processed/master_monthly_prices.csv
# ============================================

import pandas as pd
from pathlib import Path

# -------------------------------
# 1. Define base paths
# -------------------------------
base = Path(r"C:\Users\wpmpo\OneDrive\Documents\DSSB - SEM 3\Digital Transformation\commodity_analysis")
price_dir = base / "data" / "prices"
processed_dir = base / "data" / "processed"
processed_dir.mkdir(parents=True, exist_ok=True)

# -------------------------------
# 2. Load POLYPROPYLENE data (monthly)
# -------------------------------
pp = pd.read_csv(price_dir / "polypropylene_primary_avg_prices.csv")

# Convert Month to datetime (month-end)
pp["Date"] = pd.to_datetime(pp["Month"], format="%b %Y") + pd.offsets.MonthEnd(0)
pp = pp.rename(columns={"PP_Avg_EUR_per_t": "PP_EU"})
pp = pp[["Date", "PP_EU"]].set_index("Date").sort_index()

# Ensure monthly index (fills missing months as NaN)
pp_m = pp.resample("M").mean()

# -------------------------------
# 3. Load CRUDE OIL weekly data (Brent + WTI)
# -------------------------------
oil = pd.read_csv(price_dir / "crude_oil_weekly_clean.csv", parse_dates=["Date"])

# Separate Brent and WTI
brent = oil[oil["Commodity"] == "Brent_Crude"].copy()
wti = oil[oil["Commodity"] == "WTI_Crude"].copy()

# Keep Close column and convert to monthly averages
brent = brent[["Date", "Close"]].set_index("Date").resample("M").mean().rename(columns={"Close": "Brent"})
wti = wti[["Date", "Close"]].set_index("Date").resample("M").mean().rename(columns={"Close": "WTI"})

# Combine
oil_m = pd.concat([brent, wti], axis=1)

# -------------------------------
# 4. Load NATURAL GAS weekly data
# -------------------------------
gas = pd.read_csv(price_dir / "natgas_weekly.csv", parse_dates=["Date"])

# The file has duplicate headers like NG=F; we just need Close price
# Ensure numeric conversion
gas = gas[["Date", "Close"]].copy()
gas["Close"] = pd.to_numeric(gas["Close"], errors="coerce")  # convert strings → numbers, set bad values as NaN

# Rename and resample
gas = gas.rename(columns={"Close": "NatGas"})
gas_m = gas.set_index("Date").resample("ME").mean()  # use month-end

# -------------------------------
# 5. Merge all datasets
# -------------------------------
df = pd.concat([pp_m, oil_m, gas_m], axis=1)

# Optional: filter from 2019 onward
df = df[df.index >= "2019-01-01"]

# Fill small gaps, drop remaining NaNs
df = df.ffill().dropna(how="any")

# -------------------------------
# Round numeric columns to whole numbers for easy analysis
# -------------------------------
df = df.round(2)
# -------------------------------
# 6. Quick summary check
# -------------------------------
print("\n✅ Combined dataset preview:")
print(df.head(10))
print("\nData coverage:", df.index.min().strftime("%b %Y"), "→", df.index.max().strftime("%b %Y"))
print("\nColumns:", list(df.columns))
print("\nDescription:\n", df.describe())

# -------------------------------
# 7. Save processed dataset
# -------------------------------
output_path = processed_dir / "master_monthly_prices.csv"
df.to_csv(output_path, index=True)
print(f"\n✅ Master monthly dataset saved to:\n{output_path}")
