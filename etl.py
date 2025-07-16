import os
import pandas as pd
import requests
from rapidfuzz import process

# يرجع لمجلد Team_Project
BASE_DIR = os.path.dirname(os.path.dirname(__file__))

def extract_and_tag(folder_path, source_type='local'):
    data = {}
    for file in os.listdir(folder_path):
        if file.endswith('.csv'):
            file_name = os.path.splitext(file)[0]  # بدون .csv

            if source_type == 'local':
                df = pd.read_csv(os.path.join(folder_path, file))
                print(f"📁 Loaded local: {file_name} -> {df.shape}")
                data[file_name] = df
    return data

# 🟢 دالة لتحميل كل ملفات الـ API من /api/all
def extract_api_all():
    url = 'http://localhost:5000/api/all'
    response = requests.get(url)

    data = {}
    if response.status_code == 200:
        files_data = response.json()
        for file_name, content in files_data.items():
            if isinstance(content, dict) and 'error' in content:
                print(f"❌ Error loading {file_name}: {content['error']}")
                continue
            df = pd.DataFrame(content)
            name = os.path.splitext(file_name)[0]
            data[name] = df
            print(f"🌐 Loaded from API: {name} -> {df.shape}")
    else:
        print(f"❌ Failed to fetch API data. Status code: {response.status_code}")
    
    return data

# ====== Run Extraction ======
if __name__ == '__main__':
    local_path = os.path.join(BASE_DIR, 'data/pipeline_files')

    # تحميل البيانات من المجلد المحلي
    local_data = extract_and_tag(local_path, source_type='local')

    # تحميل البيانات من API
    api_data = extract_api_all()

    # دمج الاثنين في قاموس واحد
    all_data = {**local_data, **api_data}

    print("\n✅ Total files loaded:", len(all_data))


pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)

## Transformation 
#Customers Table 
df_customers = all_data['olist_customers_dataset']

## add zero in zip_code where zipcode = 4 digitals
cleaned_customers = df_customers.copy()
cleaned_customers['customer_zip_code_prefix'] = (cleaned_customers['customer_zip_code_prefix'].astype(str).str.zfill(5))
# city_cleaning.py

def build_city_dict(cleaned_customers, city_col="customer_city", state_col="customer_state"):
    """
    Build a dictionary of unique cleaned city names grouped by state.
    """
    cleaned_customers = cleaned_customers[[state_col, city_col]].dropna()
    cleaned_customers[city_col] = cleaned_customers[city_col].str.strip().str.title()
    return (
        cleaned_customers.groupby(state_col)[city_col]
        .unique()
        .apply(lambda x: sorted(set(x)))
        .to_dict()
    )

def correct_city_name(city, state, city_dict, threshold=85):
    """
    Correct a single city name using fuzzy matching based on state.
    """
    if pd.isna(city) or pd.isna(state):
        return city
    city = str(city).strip().title()
    candidates = city_dict.get(state.upper(), [])
    if not candidates:
        return city
    match, score, _ = process.extractOne(city, candidates)
    return match if score >= threshold else city

def standardize_city_column(cleaned_customers, city_col="customer_city", state_col="customer_state"):
    """
    Apply city name correction across the entire DataFrame.
    """
    city_dict = build_city_dict(cleaned_customers, city_col, state_col)
    cleaned_customers[city_col] = cleaned_customers.apply(
        lambda row: correct_city_name(row[city_col], row[state_col], city_dict),
        axis=1
    )
    return cleaned_customers

# inside main ETL loop:
city_cols = [c for c in cleaned_customers.columns if "customer_city" in c.lower()]
state_cols = [s for s in cleaned_customers.columns if "customer_state" in s.lower()]
if city_cols and state_cols:
    city_col = city_cols[0]
    state_col = state_cols[0]
    print(f"🌍 Standardizing cities using: {city_col}, {state_col}")
    cleaned_customers = standardize_city_column(cleaned_customers, city_col=city_col, state_col=state_col)

# Drop empty rows/columns
    cleaned_customers.dropna(how="all", inplace=True)
    cleaned_customers.dropna(axis=1, how="all", inplace=True)

#Geolocation Table 
