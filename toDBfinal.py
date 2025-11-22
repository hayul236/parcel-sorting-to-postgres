import pandas as pd
import os
from glob import glob
from itertools import count
from sqlalchemy import create_engine, Table, MetaData, select, Column, String, func, update
from sqlalchemy.dialects.postgresql import insert




folder_path = "excel_data"   # folder containing EXCELs
db_url = "postgresql+psycopg2://username:password@localhost:5432/database" 
#INSERT YOUR name , pw , port and database. example  "postgresql+psycopg2://postgres:pw123!@localhost:5432/sscc_clp" 

# 0: Connect to DB
engine = create_engine(db_url)
metadata = MetaData()

# 1: Create Tables

# parcel_table
parcel_table = Table(
    'parcel_table',
    metadata,
    Column('sscc', String(50), primary_key=True),
    Column('country_code', String(10), nullable=False),
    Column('pallet_id', String(20), nullable=False),
    extend_existing=True
)


# pallet_status table
pallet_status_table = Table(
    'pallet_status', metadata,
    Column('pallet_id', String(20), primary_key=True),
    Column('console_status', String(10), default='IN CONSOLE'),
    Column('quantity', String(10), default='0'),
    extend_existing=True
)

metadata.create_all(engine)

# Load existing SSCCs to avoid duplicates
with engine.begin() as conn:
    result = conn.execute(select(parcel_table.c.sscc, parcel_table.c.pallet_id, parcel_table.c.country_code))
    existing_data = pd.DataFrame(result.fetchall(), columns=['SSCC','pallet_id','country_code'])

existing_ssccs = set(existing_data['SSCC']) if not existing_data.empty else set()

# Determine last pallet number used
if not existing_data.empty:
    pallet_numbers = [int(p.replace("PALLET","")) for p in existing_data['pallet_id'].unique()]
    last_pallet_number = max(pallet_numbers)
else:
    last_pallet_number = 0

pallet_counter = count(last_pallet_number + 1)


# Track partially filled pallets per country
country_pallets = {}
if not existing_data.empty:
    for country in existing_data['country_code'].unique():
        pallets = existing_data[existing_data['country_code'] == country]['pallet_id'].value_counts()
        country_pallets[country] = []
        for pallet_id, count_parcels in pallets.items():
            if count_parcels < 20:
                country_pallets[country].append({'pallet_id': pallet_id, 'count': count_parcels})


# 2: Read all Excel file
all_files = sorted(glob(os.path.join(folder_path, "*.xlsx")))
print("Excel files:", all_files)

df_list = []

for file in all_files:
    df = pd.read_excel(file, dtype={"SSCC / Parcel ID": str}) 
    print(df)
    df_list.append(df)

data = pd.concat(df_list, ignore_index=True)


# Change header format Excel->Python
data = data.rename(columns={
    'No.': 'no',
    'SSCC / Parcel ID': 'SSCC',
    'Country Code': 'country_code'
})

required_cols = ['no', 'SSCC', 'country_code']
if not all(col in data.columns for col in required_cols):
    raise ValueError(f"EXCEL files must have columns: {required_cols}")


# 3: Remove duplicates, run more than once it wont add the existing SSCC
data = data.drop_duplicates(subset=['SSCC'])  # within Excels
data = data[~data['SSCC'].isin(existing_ssccs)]  # skip already in DB

# Checking
print(data.columns.tolist())
print(data['SSCC'].to_list())
print(f"Total new parcels to insert: {len(data)}")


# 4: Assign pallets
all_rows = []

def generate_pallet_id():
    return f"PALLET{next(pallet_counter):05}"

for _, row in data.iterrows():
    country = row['country_code']
    
    if country not in country_pallets:
        country_pallets[country] = []
    
    # Fill existing pallet less than 20 first
    pallet_assigned = False
    for pallet in country_pallets[country]:
        if pallet['count'] < 20:
            row['pallet_id'] = pallet['pallet_id']
            pallet['count'] += 1
            pallet_assigned = True
            break
    
    # If no existing pallet with space, create new
    if not pallet_assigned:
        new_pallet_id = generate_pallet_id()
        row['pallet_id'] = new_pallet_id
        country_pallets[country].append({'pallet_id': new_pallet_id, 'count': 1})
    
    all_rows.append(row)

final_df = pd.DataFrame(all_rows)


# 5: Add into SQL DB
with engine.begin() as conn:
    for _, row in final_df.iterrows():
        update_data = insert(parcel_table).values(
            sscc=row['SSCC'],
            country_code=row['country_code'],
            pallet_id=row['pallet_id']
        ).on_conflict_do_nothing(index_elements=['sscc'])
        conn.execute(update_data)









# update pallet_status table
unique_pallet_ids = set()  # create an empty set wont duplicate pallet_id

# gather unique pallet_id
for pallets in country_pallets.values():   
    for pallet in pallets:
        unique_pallet_ids.add(pallet['pallet_id'])

print(unique_pallet_ids)

with engine.begin() as conn:
    for pid in unique_pallet_ids:
        update_data = insert(pallet_status_table).values(
            pallet_id=pid
        ).on_conflict_do_nothing(index_elements=['pallet_id'])
        conn.execute(update_data)
        
        # Count SSCCs per pallet
    counts = conn.execute(
        select(
            parcel_table.c.pallet_id,
            func.count(parcel_table.c.sscc)
        ).group_by(parcel_table.c.pallet_id)
    ).fetchall()

    # Update quantity in pallet_status table
    for pallet_id, cnt in counts:
        update_data = (
            update(pallet_status_table)
            .where(pallet_status_table.c.pallet_id == pallet_id)
            .values(quantity=str(cnt))
        )
        conn.execute(update_data)
