import streamlit as st
import pandas as pd
import numpy as np
import datetime
import io
import psycopg2
import re
from calendar import monthrange

REGION_MAP = {
    "Prambanan": "East", "3rd Party": "East", "Import": "East",
    "Ciracas": "West", "Sentul": "West"
}

PRIORITY_LIST = {
    "East_3rd Party_RTD 1": ["203835", "207670", "207666"],
    "East_3rd Party_RTD 2": ["203837", "207657", "207654"],
    
    # Update GH: Kiri ke Kanan
    "East_Prambanan_GH": [
        "202416", "202423", "202429", "202433", "208391", "208392", "208370", "208371",
        "173757", "173758", "137829", "202421", "202415", "202432", "202428", "196877",
        "196879", "196805A", "196877A", "196879A", "196878A", "1V1500", "1H1500", "3H1500",
        "3V1500", "208391A", "208392A", "208375A", "208370A", "208371A", "196881A", "196882A",
        "213930", "213929", "213942", "213941"
    ],
    
    # Update JK: Kiri ke Kanan
    "East_Prambanan_JK": [
        "196653", "196656", "196659", "196661", "206030", "208362", "208365A", "208376",
        "157937", "196655", "196663", "196648", "196658", "196660", "196653A", "196656A",
        "1V1050", "1H1050", "196659A", "196661A", "206030A", "208362A", "3H1050", "3V1050"
    ],

    "East_Prambanan_VW": ["196642", "196654", "196657", "158328", "196642A", "196654A", "196657A"],
    "East_Prambanan_AB": ["209061", "201899", "201900", "201897", "173755", "173756", "133985", "202413", "202431", "202426", "202419", "209062", "204172", "204170", "158328", "157937", "202413A", "202431A", "202426A", "202419A"],
    "East_Prambanan_XY": ["202412", "155459", "202417", "202420", "205725A", "205728A", "205732A", "205739A", "202425", "202418", "202434", "202430", "171706", "171705", "198865", "198866", "198867", "202412A", "202417A", "202420A", "130267", "163854"],
    "East_Prambanan_TU": ["196805", "196878", "196881", "196882", "208375", "208376A", "196655A", "196648A", "196658A", "196660A", "196663A"],
    "East_Prambanan_CD": ["173757", "173758", "137829", "208391A", "208392A", "208370A", "208371A", "205725A", "205728A", "205732A", "205739A", "202418", "202425", "202434", "202430", "205725", "205728", "205732", "205739", "198865", "198866"],
    "West_Ciracas_L2": ["203906", "202723", "202722", "203907", "202727", "213164", "213160", "202724", "197013", "210729A", "196673A", "196668A", "213158", "213157", "208966", "178494", "150155", "171708", "210729", "196671", "196672", "196673", "196667", "196668", "158213", "196666"],
    "West_Ciracas_L3": ["202883", "202882", "178513", "197016", "196669A", "196670A", "196664A", "196665A", "130075", "195078", "133087", "195079", "133083", "171709", "171710", "130386", "196669", "196670", "196664", "196665", "158209", "166777", "178477"],
    "West_Ciracas_L4": ["202894", "202895", "210729A", "202722", "202724", "196671A", "196666A", "210728A", "202723", "202727", "208960", "210729", "130074", "197015", "161733", "147031", "147029", "171707", "171708", "204174", "204173", "196666", "196664", "196665", "196667", "196670", "196671", "196672", "210728", "178477"],
    "West_Ciracas_BiB": ["209065", "209066", "213731", "213732", "197015", "196666A", "195080", "158494", "178159", "178160", "178171", "178158", "213733", "213734", "205308", "210728", "178172", "195077", "185871", "185869", "185875", "185876", "185874", "185878", "197013", "208970", "208969", "208972", "208971", "160828", "193899", "193907", "193906", "193903", "193902", "193897", "137933", "195078", "129860", "195076", "213930", "213929", "213942", "213941", "133985", "133984", "194382", "169569", "204333", "204330", "183085", "185786", "185787", "150162", "150163", "130386", "150155", "133085", "210728A", "196667", "196666", "208964", "196668"],
    "West_Sentul_BiB": ["209065", "209066", "213731", "213732", "197015", "196666A", "195080", "158494", "178159", "178160", "178171", "178158", "213733", "213734", "205308", "210728", "178172", "195077", "185871", "185869", "185875", "185876", "185874", "185878", "197013", "208970", "208969", "208972", "208971", "160828", "193899", "193907", "193906", "193903", "193902", "193897", "137933", "195078", "129860", "195076", "213930", "213929", "213942", "213941", "133985", "133984", "194382", "169569", "204333", "204330", "183085", "185786", "185787", "150162", "150163", "130386", "150155", "133085", "210728A", "196667", "196666", "208964", "196668"],
    "West_Sentul_CANL SGZ": ["203905", "203904", "178500", "197014", "201490", "201491", "203497", "203498", "203499", "203500", "173753", "173754", "213735", "196638", "205309", "209068", "209069", "178405", "200189", "200270", "201492", "201493", "205310", "211451", "211450", "211314", "211320", "193837", "193898", "193904", "134034", "193901", "134033", "161914", "173752", "204171", "204166", "204405", "209607", "202304", "202305", "190443", "190442", "181300", "190444", "190441", "153903", "211316", "211315", "211321", "211319", "162437", "91031", "172199", "133088", "133084", "133261", "133086", "130385", "162716", "162768", "162717", "162724", "162764", "162765", "162766", "162769", "162714", "149913", "162715", "162773", "163363", "162772", "162771", "156828", "166438"]
}

INPUT_COLUMNS = ["Material", "Material Description", "Plant", "Size", "Pcs/cb", "Machine 1"]
MONTHS_LIST = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

# --- 3. FUNGSI HELPER & DB ---

def format_material(val):
    if pd.isna(val): return ""
    return str(val).strip()

def clean_sheet_name(name):
    return re.sub(r'[\\/*?:\[\]]', '_', name)[:31]

@st.cache_data(ttl=300)
def get_db_master_full():
    try:
        conn = psycopg2.connect(
            host=st.secrets["postgres"]["host"],
            database=st.secrets["postgres"]["database"],
            user=st.secrets["postgres"]["user"],
            password=st.secrets["postgres"]["password"],
            port=st.secrets["postgres"]["port"]
        )
        query = "SELECT sku_code, size, pcs_cb, line, speed FROM fg_master_data"
        df_db = pd.read_sql(query, conn)
        conn.close()
        
        df_db['sku_code'] = df_db['sku_code'].apply(format_material)
        df_db['line'] = df_db['line'].astype(str).str.strip().str.upper()
        df_db['size'] = pd.to_numeric(df_db['size'], errors='coerce').fillna(0).round(0).astype(float)
        df_db['pcs_cb'] = pd.to_numeric(df_db['pcs_cb'], errors='coerce').fillna(0).round(0).astype(float)
        df_db['speed'] = pd.to_numeric(df_db['speed'], errors='coerce').fillna(0).astype(float)
        return df_db
    except Exception as e:
        st.error(f"Koneksi DB Gagal: {e}")
        return pd.DataFrame()

def generate_date_range(start_m, start_y, end_m, end_y):
    start_date = datetime.datetime(start_y, MONTHS_LIST.index(start_m) + 1, 1)
    end_date = datetime.datetime(end_y, MONTHS_LIST.index(end_m) + 1, 1)
    current, target_months = start_date, []
    while current <= end_date:
        target_months.append(current.strftime('%b-%y'))
        month = current.month
        year = current.year + (month // 12)
        month = (month % 12) + 1
        current = datetime.datetime(year, month, 1)
    return target_months

def validate_row_and_get_data(row, df_master):
    sku = format_material(row['Material'])
    size_src = round(float(pd.to_numeric(row['Size'], errors='coerce') or 0), 0)
    pcs_cb_src = round(float(pd.to_numeric(row['Pcs/cb'], errors='coerce') or 0), 0)
    
    m1 = str(row['Machine 1']).strip().upper()
    plant = str(row['Plant']).strip()
    region = REGION_MAP.get(plant, "Unknown")

    matches = df_master[
        (df_master['sku_code'] == sku) & 
        (np.isclose(df_master['size'], size_src, atol=0.1)) & 
        (np.isclose(df_master['pcs_cb'], pcs_cb_src, atol=0.1))
    ]
    
    if matches.empty:
        matches = df_master[(df_master['sku_code'] == sku) & (np.isclose(df_master['size'], size_src, atol=0.1))]

    assigned_line = None
    if plant == "3rd Party":
        if sku in PRIORITY_LIST.get("East_3rd Party_RTD 1", []): assigned_line = "RTD 1"
        elif sku in PRIORITY_LIST.get("East_3rd Party_RTD 2", []): assigned_line = "RTD 2"
    
    if not assigned_line and sku in PRIORITY_LIST.get("West_Ciracas_BiB", []) and "SGZ" in m1:
        assigned_line = "BiB"

    if not assigned_line:
        if not matches.empty:
            allowed_lines = matches['line'].unique().tolist()
            if region == "East" and any(l in ['0', '0.0', ''] for l in allowed_lines): assigned_line = m1
            elif m1 in allowed_lines: assigned_line = m1
            else:
                valid_lines = [l for l in allowed_lines if l not in ['0', '0.0', '']]
                assigned_line = valid_lines[0] if valid_lines else m1
        else: assigned_line = m1

    if not assigned_line: return None, 0

    speed = 0
    if not matches.empty:
        if assigned_line in ["RTD 1", "RTD 2"]:
            rtd_match = matches[matches['line'] == "A"]
            speed = rtd_match['speed'].iloc[0] if not rtd_match.empty else matches['speed'].iloc[0]
        else:
            line_match = matches[matches['line'] == assigned_line]
            speed = line_match['speed'].iloc[0] if not line_match.empty else matches['speed'].iloc[0]
    
    return assigned_line, speed

def process_data(uploaded_file, sheet_target, target_range, df_master):
    df_raw = pd.read_excel(uploaded_file, sheet_name=sheet_target, header=None)
    
    # Batas "TOTAL"
    row_header_temp = df_raw.iloc[3]
    mat_col_idx = next((i for i, val in enumerate(row_header_temp) if str(val).strip() == "Material"), None)
    if mat_col_idx is not None:
        total_mask = df_raw[mat_col_idx].astype(str).str.upper().str.contains("TOTAL", na=False)
        if total_mask.any(): df_raw = df_raw.iloc[:total_mask.idxmax()]
    
    row_unit, row_header = df_raw.iloc[2], df_raw.iloc[3]
    header_list = [str(h).strip() for h in row_header]
    selected_indices = [header_list.index(col) for col in INPUT_COLUMNS if col in header_list]
    
    prod_indices, prod_names = [], []
    for i, val in enumerate(row_header):
        lbl = val.strftime('%b-%y') if isinstance(val, (pd.Timestamp, datetime.datetime)) else str(val).strip()
        if lbl in target_range and "in cb" in str(row_unit.iloc[i]).lower():
            prod_indices.append(i); prod_names.append(lbl)

    df_data = df_raw.iloc[5:, selected_indices + prod_indices].copy()
    df_data.columns = INPUT_COLUMNS + prod_names
    df_data['Material'] = df_data['Material'].apply(format_material)
    
    # CASE INSENSITIVE: Machine 1 paksa jadi Uppercase di df_data
    df_data['Machine 1'] = df_data['Machine 1'].astype(str).str.strip().str.upper()
    
    results = df_data.apply(lambda r: validate_row_and_get_data(r, df_master), axis=1)
    df_data['Line'], df_data['Speed'] = [r[0] for r in results], [r[1] for r in results]
    df_data = df_data.dropna(subset=['Line'])

    id_vars = INPUT_COLUMNS + ["Line", "Speed"]
    df_vertical = pd.melt(df_data, id_vars=id_vars, value_vars=prod_names, var_name='Month', value_name='Qty (Ctn)')
    df_vertical['Qty (Ctn)'] = pd.to_numeric(df_vertical['Qty (Ctn)'], errors='coerce').fillna(0).round(0).astype(int)
    df_vertical = df_vertical[df_vertical['Qty (Ctn)'] > 0]
    df_vertical['Month'] = pd.Categorical(df_vertical['Month'], categories=target_range, ordered=True)

    sheet_groups = {}
    for _, row in df_vertical.iterrows():
        plant, sku, line_val = str(row['Plant']).strip(), str(row['Material']), str(row['Line']).strip()
        region = REGION_MAP.get(plant, "Unknown")
        key = f"{region}_{plant}_{line_val}"
        if sku in PRIORITY_LIST.get("West_Ciracas_BiB", []): key = f"West_{plant}_BiB"
        if key == "East_3rd Party_A": continue
        if key not in sheet_groups: sheet_groups[key] = []
        sheet_groups[key].append(row)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for key in sorted(sheet_groups.keys()):
            df_group = pd.DataFrame(sheet_groups[key])
            priority_order = PRIORITY_LIST.get(key, [])
            final_sheet_list = []
            
            for m in target_range:
                df_month = df_group[df_group['Month'] == m].copy()
                if df_month.empty: continue
                
                df_p = df_month[(df_month['Material'].isin(priority_order)) & (df_month['Speed'] > 0)].copy()
                df_t = df_month[(~df_month['Material'].isin(priority_order)) & (df_month['Speed'] > 0)].copy()
                df_missing = df_month[df_month['Speed'] == 0].copy()

                if not df_p.empty:
                    p_map = {sku: idx for idx, sku in enumerate(priority_order)}
                    df_p['_sort_idx'] = df_p['Material'].map(p_map)
                    df_p = df_p.sort_values('_sort_idx').drop(columns=['_sort_idx'])
                if not df_t.empty:
                    df_t = df_t.sort_values(by='Qty (Ctn)', ascending=False)
                
                valid_dfs = [df for df in [df_p, df_t, df_missing] if not df.empty]
                if valid_dfs: final_sheet_list.append(pd.concat(valid_dfs, ignore_index=True))
            
            if not final_sheet_list: continue
            df_f = pd.concat(final_sheet_list, ignore_index=True)
            df_f['prod hour'] = (df_f['Qty (Ctn)'] / df_f['Speed'].replace(0, np.nan)).fillna(0).round(2)
            df_f['Days'] = (df_f['prod hour'] / 24).round(2)
            
            t_starts, t_finishes, forced, over, missing = [], [], [], [], []
            current_time, last_month_str = None, None
            
            for _, row in df_f.iterrows():
                month_dt = datetime.datetime.strptime(row['Month'], "%b-%y")
                if current_time is None or row['Month'] != last_month_str:
                    current_time = month_dt.replace(day=1, hour=7, minute=0, second=0)
                
                is_forced = False
                if current_time.month != month_dt.month or current_time.year != month_dt.year:
                    last_day = monthrange(month_dt.year, month_dt.month)[1]
                    current_time = month_dt.replace(day=last_day, hour=7, minute=0, second=0)
                    is_forced = True
                
                start_t = current_time
                duration = row['prod hour']
                finish_t = start_t + datetime.timedelta(hours=float(duration))
                is_over = (finish_t.month != month_dt.month or finish_t.year != month_dt.year)
                
                t_starts.append(start_t.strftime('%d-%m-%Y %H:%M:%S'))
                t_finishes.append(finish_t.strftime('%d-%m-%Y %H:%M:%S'))
                forced.append(is_forced); over.append(is_over)
                missing.append(row['Speed'] == 0)
                current_time, last_month_str = finish_t, row['Month']
            
            df_f['Time Start'], df_f['Time Finish'], df_f['_forced'], df_f['_over'], df_f['_missing'] = t_starts, t_finishes, forced, over, missing
            
            def row_styler(row):
                if row['_missing']: return ['background-color: #00FF00'] * len(row) # HIJAU
                if row['_forced']: return ['background-color: #FF0000; color: white'] * len(row) # MERAH
                if row['_over']: return ['background-color: #FFC000'] * len(row) # EMAS
                return [''] * len(row)

            cols = ['Line', 'Month', 'Material', 'Material Description', 'Size', 'Pcs/cb', 'Speed', 'prod hour', 'Days', 'Time Start', 'Time Finish', 'Qty (Ctn)']
            styled_df = df_f.style.apply(row_styler, axis=1)
            styled_df.to_excel(writer, sheet_name=clean_sheet_name(key), index=False, columns=cols)
            
    return output.getvalue()

st.set_page_config(page_title="MPS - DPS Converter", layout="wide")
st.title("MPS to DPS Converter")

df_master = get_db_master_full()
file_upload = st.file_uploader("Upload File MPS Cycle", type="xlsx")

if file_upload:
    xl = pd.ExcelFile(file_upload)
    with st.sidebar:
        st.header("⚙️ Konfigurasi")
        sheet_target = st.selectbox("Sumber Sheet", xl.sheet_names, index=0)
        sm = st.selectbox("Mulai", MONTHS_LIST, index=0)
        sy = st.number_input("Tahun", value=2026, key='tahun_mulai') 
        em = st.selectbox("Sampai", MONTHS_LIST, index=11)
        ey = st.number_input("Tahun", value=2026, key='tahun_selesai') 

    if st.button("Mulai Proses", use_container_width=True):
        try:
            trange = generate_date_range(sm, sy, em, ey)
            res = process_data(file_upload, sheet_target, trange, df_master)
            if res:
                st.success("Berhasil! Pembersihan Data telah Dilakukan.")
                st.download_button("📥 Unduh Hasil Akhir", res, f"MPS_Final_{datetime.date.today()}.xlsx")
        except Exception as e:
            st.error(f"Error: {e}")