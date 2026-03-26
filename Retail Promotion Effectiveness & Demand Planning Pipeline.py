import pandas as pd

# Show all columns during inspection
pd.set_option('display.max_columns', None)

# Load source tables
camping = pd.read_csv("Data\campaigns.csv")
campaign_descriptions = pd.read_csv("Data\campaign_descriptions.csv")
products = pd.read_csv("Data\products.csv")
coupons = pd.read_csv("Data\coupons.csv")
coupon_redemptions = pd.read_csv("Data\coupon_redemptions.csv")
promotions_sample = pd.read_csv("Data\promotions_sample.csv")
demographics = pd.read_csv("Data\demographics.csv")
transactions_sample = pd.read_csv("Data\\transactions_sample.csv")

# Inspect sample records
print(camping.head(10))
print(campaign_descriptions.head(10))
print(products.head(10))
print(coupons.head(10))
print(coupon_redemptions.head(10))
print(promotions_sample.head(10))
print(demographics.head(10))
print(transactions_sample.head(10))

# Review structure and data types
print(camping.info())
print(campaign_descriptions.info())
print(products.info())
print(coupons.info())
print(coupon_redemptions.info())
print(promotions_sample.info())
print(demographics.info())
print(transactions_sample.info())

# Check whether float IDs contain real decimals
print(((products["product_id"].dropna() % 1) != 0).sum())
print(((coupons["product_id"].dropna() % 1) != 0).sum())
print(((transactions_sample["basket_id"].dropna() % 1) != 0).sum())

# Standardize ID fields as integers
products["product_id"] = products["product_id"].astype(int)
coupons["product_id"] = coupons["product_id"].astype(int)
transactions_sample["basket_id"] = transactions_sample["basket_id"].astype(int)

# Convert date and datetime fields
campaign_descriptions["start_date"] = pd.to_datetime(
    campaign_descriptions["start_date"],
    format="%Y-%m-%d",
    errors="coerce"
)
campaign_descriptions["end_date"] = pd.to_datetime(
    campaign_descriptions["end_date"],
    format="%Y-%m-%d",
    errors="coerce"
)
coupon_redemptions["redemption_date"] = pd.to_datetime(
    coupon_redemptions["redemption_date"],
    format="%Y-%m-%d",
    errors="coerce"
)
transactions_sample["transaction_timestamp"] = pd.to_datetime(
    transactions_sample["transaction_timestamp"],
    format="%Y-%m-%dT%H:%M:%SZ",
    errors="coerce",
    utc=True
)

# Define numeric profiling fields
numeric_checks = {
    "campaign_descriptions": ["campaign_id"],
    "products": ["product_id", "manufacturer_id"],
    "coupons": ["coupon_upc", "product_id", "campaign_id"],
    "coupon_redemptions": ["household_id", "coupon_upc", "campaign_id"],
    "promotions_sample": ["product_id", "store_id", "week"],
    "demographics": ["household_id"],
    "transactions_sample": [
        "household_id", "store_id", "basket_id", "product_id", "quantity",
        "sales_value", "retail_disc", "coupon_disc", "coupon_match_disc", "week"
    ]
}

# Define temporal profiling fields
date_checks = {
    "campaign_descriptions": ["start_date", "end_date"],
    "coupon_redemptions": ["redemption_date"],
    "transactions_sample": ["transaction_timestamp"]
}

# Define categorical profiling fields
category_checks = {
    "campaign_descriptions": ["campaign_type"],
    "products": ["department", "brand", "product_category", "product_type", "package_size"],
    "promotions_sample": ["display_location", "mailer_location"],
    "demographics": [
        "age", "income", "home_ownership", "marital_status",
        "household_size", "household_comp", "kids_count"
    ]
}

# Profile numeric variables for ranges and outliers
for df_name, cols in numeric_checks.items():
    print(f"\n{'='*20} {df_name.upper()} | NUMERIC {'='*20}")
    print(globals()[df_name][cols].describe().T)

# Profile date fields for temporal coverage
for df_name, cols in date_checks.items():
    print(f"\n{'='*20} {df_name.upper()} | DATES {'='*20}")
    df = globals()[df_name][cols]
    print(df.agg(["count", "min", "max"]).T)

# Profile categories for duplicates and missing values
for df_name, cols in category_checks.items():
    print(f"\n{'='*20} {df_name.upper()} | CATEGORIES {'='*20}")
    df = globals()[df_name]
    for col in cols:
        s = df[col]
        s_norm = s.astype(str).str.strip().str.lower().replace({"nan": pd.NA})
        print(f"\n{col}")
        print("unique_raw:", s.nunique(dropna=False))
        print("unique_normalized:", s_norm.nunique(dropna=False))
        print("nulls:", s.isna().sum())
        print(s.value_counts(dropna=False).head(15))

import sqlalchemy
import psycopg
from sqlalchemy import create_engine, text

# Define PostgreSQL connection settings
user = "postgres"
password = "88724800"
host = "localhost"
port = "5432"
database = "retail_promo"

# Create a database engine to connect Python with PostgreSQL
engine = create_engine(f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}")

# Group all cleaned DataFrames for export
tables = {
    "campaigns": camping,
    "campaign_descriptions": campaign_descriptions,
    "products": products,
    "coupons": coupons,
    "coupon_redemptions": coupon_redemptions,
    "promotions_sample": promotions_sample,
    "demographics": demographics,
    "transactions_sample": transactions_sample
}

# Load each DataFrame into PostgreSQL as a table
for table_name, df in tables.items():
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=5000
    )

# Create indexes to improve join and filter performance
with engine.begin() as conn:
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_campaigns_household_id ON campaigns(household_id)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_campaigns_campaign_id ON campaigns(campaign_id)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_campaign_descriptions_campaign_id ON campaign_descriptions(campaign_id)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_products_product_id ON products(product_id)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_coupons_product_id ON coupons(product_id)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_coupons_campaign_id ON coupons(campaign_id)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_coupon_redemptions_household_id ON coupon_redemptions(household_id)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_coupon_redemptions_coupon_upc ON coupon_redemptions(coupon_upc)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_coupon_redemptions_campaign_id ON coupon_redemptions(campaign_id)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_promotions_product_store_week ON promotions_sample(product_id, store_id, week)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_transactions_household_id ON transactions_sample(household_id)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_transactions_product_id ON transactions_sample(product_id)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_transactions_store_id ON transactions_sample(store_id)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_transactions_week ON transactions_sample(week)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_transactions_basket_id ON transactions_sample(basket_id)"))