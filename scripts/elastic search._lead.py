import pandas as pd
import snowflake.connector
from elasticsearch import Elasticsearch, helpers

# -------------------------------
# CONFIGURATIONS
# -------------------------------
SNOWFLAKE_CONFIG = {
    "user": "wahab675",
    "password": "@",
    "account": "",
    "warehouse": "ABDUL",
    "database": "ABDUL",
    "schema": "PUBLIC"
}

ELASTIC_CONFIG = {
    "endpoint": "https://cloud-c04f65.es.us-central1.gcp.elastic.cloud:443",
    "api_key": "VHptbzQ1a0I2RTZWeTQzdy1zQ0o6UHVIY3JfcElHejBpaWl0Ty03cW9lZw==",
    "index": "jobs_emp"
}

# -------------------------------
# FETCH DATA FROM SNOWFLAKE
# -------------------------------
print("📡 Connecting to Snowflake...")
conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
cur = conn.cursor()

query = "SELECT * FROM JOBS;"
cur.execute(query)
df = cur.fetch_pandas_all()

print(f"✅ Data fetched from Snowflake: {len(df)} rows")
print("📋 Columns fetched:", df.columns.tolist())

cur.close()
conn.close()

# -------------------------------
# CONNECT TO ELASTICSEARCH CLOUD
# -------------------------------
print("🔍 Connecting to Elasticsearch Cloud...")
try:
    es = Elasticsearch(
        ELASTIC_CONFIG["endpoint"],
        api_key=ELASTIC_CONFIG["api_key"],
        verify_certs=True
    )

    if es.ping():
        print("✅ Connected successfully to Elasticsearch Cloud!")
    else:
        raise ConnectionError(
            "❌ Unable to connect. Check endpoint or API key.")
except Exception as e:
    print(f"🚨 Connection error: {e}")
    exit()

# -------------------------------
# CREATE INDEX IF NOT EXISTS
# -------------------------------
index_name = ELASTIC_CONFIG["index"]

if not es.indices.exists(index=index_name):
    mapping = {
        "mappings": {
            "properties": {
                "job_id": {"type": "keyword"},
                "data": {"type": "object"}
            }
        }
    }
    es.indices.create(index=index_name, body=mapping)
    print(f"🆕 Created new index with mapping: {index_name}")
else:
    print(f"ℹ️ Index '{index_name}' already exists.")

# -------------------------------
# UPLOAD DATA TO ELASTICSEARCH
# -------------------------------
if not df.empty:
    actions = []
    for i, row in df.iterrows():
        # Pick a usable ID column if it exists, else use row index
        id_col = next((col for col in df.columns if "id" in col.lower()), None)
        record_id = str(row[id_col]) if id_col and pd.notna(
            row[id_col]) else str(i)

        doc = {
            "_index": index_name,
            "_id": record_id,
            "_source": row.to_dict()  # Upload the full record dynamically
        }
        actions.append(doc)

    helpers.bulk(es, actions)
    print(
        f"🚀 Successfully uploaded {len(actions)} records to Elasticsearch index '{index_name}'.")
else:
    print("⚠️ No data to upload.")
