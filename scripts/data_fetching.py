import requests
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# -------------------------------
# CONFIG
# -------------------------------
BEARER_TOKEN = ""
CUSTOMERS_URL = "https://api.housecallpro.com/customers"

headers = {
    "Authorization": f"Bearer {BEARER_TOKEN}",
    "Accept": "application/json"
}

# -------------------------------
# FUNCTION: Fetch all customers
# -------------------------------


def fetch_customers(page=1, page_size=50):
    params = {"page": page, "page_size": page_size}
    response = requests.get(CUSTOMERS_URL, headers=headers, params=params)
    print(f"🔍 Status: {response.status_code}")

    if response.status_code == 200:
        data = response.json()
        customers = data.get("customers") or data.get("data") or []
        return customers
    else:
        print(f"❌ Error {response.status_code}: {response.text}")
        return []


# -------------------------------
# MAIN FLOW
# -------------------------------
customers = fetch_customers()

if customers:
    df = pd.DataFrame(customers)
    print(f"✅ Customers fetched: {len(df)}")

    # -------------------------------
    # FLATTEN ADDRESS FIELD
    # -------------------------------
    def extract_address_field(addresses, key):
        """Safely extract a field from the first address in the list"""
        if isinstance(addresses, list) and len(addresses) > 0:
            return addresses[0].get(key, "")
        return ""

    df["street"] = df["addresses"].apply(
        lambda x: extract_address_field(x, "street"))
    df["city"] = df["addresses"].apply(
        lambda x: extract_address_field(x, "city"))
    df["state"] = df["addresses"].apply(
        lambda x: extract_address_field(x, "state"))
    df["zip"] = df["addresses"].apply(
        lambda x: extract_address_field(x, "zip"))
    df["country"] = df["addresses"].apply(
        lambda x: extract_address_field(x, "country"))

    # Drop original JSON columns
    df.drop(columns=["addresses", "tags"], inplace=True, errors="ignore")

    # Replace NaN
    df = df.fillna("")

    # -------------------------------
    # CONNECT TO SNOWFLAKE
    # -------------------------------
    conn = snowflake.connector.connect(
        user="own user",
        password="use your own pw",
        account="own account",
        warehouse="ABDUL",
        database="ABDUL",
        schema="PUBLIC"
    )
    print("✅ Connected to Snowflake")

    # -------------------------------
    # CREATE TABLE WITH CLEAN SCHEMA
    # -------------------------------
    table_schema = {
        "id": "STRING",
        "first_name": "STRING",
        "last_name": "STRING",
        "email": "STRING",
        "mobile_number": "STRING",
        "home_number": "STRING",
        "work_number": "STRING",
        "company": "STRING",
        "notifications_enabled": "BOOLEAN",
        "lead_source": "STRING",
        "notes": "STRING",
        "created_at": "TIMESTAMP_NTZ",
        "updated_at": "TIMESTAMP_NTZ",
        "company_name": "STRING",
        "company_id": "STRING",
        "street": "STRING",
        "city": "STRING",
        "state": "STRING",
        "zip": "STRING",
        "country": "STRING"
    }

    columns_sql = ", ".join(
        [f'"{col}" {table_schema.get(col, "VARCHAR")}' for col in df.columns]
    )

    create_table_query = f"CREATE OR REPLACE TABLE CUSTOMERS ({columns_sql});"
    cur = conn.cursor()
    cur.execute(create_table_query)
    print("✅ Table CUSTOMERS created or replaced with readable address columns.")

    # -------------------------------
    # UPLOAD TO SNOWFLAKE
    # -------------------------------
    success, nchunks, nrows, _ = write_pandas(conn, df, "CUSTOMERS")
    print(f"✅ Uploaded {nrows} rows to Snowflake successfully!")

    cur.close()
    conn.close()

else:
    print("⚠️ No customers available or token has no access.")

