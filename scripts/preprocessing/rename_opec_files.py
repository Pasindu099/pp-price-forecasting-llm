import os, re

path = r"C:\Users\wpmpo\OneDrive\Documents\DSSB - SEM 3\Digital Transformation\commodity_analysis 1\data\reports\energy\opec"
months = {
    "jan": "January","feb": "February","mar": "March","apr": "April","may": "May","jun": "June",
    "jul": "July","aug": "August","sep": "September","sept": "September","oct": "October",
    "nov": "November","dec": "December"
}

for f in os.listdir(path):
    if not f.lower().endswith(".pdf"):
        continue
    base = os.path.splitext(f)[0]
    m = re.search(r"(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*", base, re.I)
    y = re.search(r"20\d{2}", base)
    if not (m and y):
        print("⚠️  skipped:", f)
        continue
    month = months[m.group(1).lower()]
    year = y.group(0)
    new = f"MOMR_{month}_{year}.pdf"
    oldp, newp = os.path.join(path, f), os.path.join(path, new)
    if not os.path.exists(newp):
        os.rename(oldp, newp)
        print("✅", f, "→", new)
