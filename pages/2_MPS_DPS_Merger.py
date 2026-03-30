import streamlit as st
import pandas as pd
import io

st.set_page_config(page_title="MPS & DPS Merger", layout="wide")

st.title("MPS & DPS Merger")

# Required columns to keep in the output
SELECTED_COLUMNS = [
    "Line", "Date", "SAP Article", "Description", "Pack Size", 
    "Kg_TU", "Qty (Ctn)", "Qty Bulk (kg)", "BIN", 
    "Time Start", "Time Finish", "Release Time"
]
RELEASE_TIME_COL = "Release Time"

# --- STEP 1: UPLOAD FILES ---
st.subheader("Upload Master Files (MPS & DPS)")
col_f1, col_f2 = st.columns(2)
with col_f1:
    mps_file = st.file_uploader("Upload MPS Final", type="xlsx", key="mps_global")
with col_f2:
    dps_file = st.file_uploader("Upload DPS", type="xlsx", key="dps_global")

if mps_file and dps_file:
    xl_mps = pd.ExcelFile(mps_file)
    xl_dps = pd.ExcelFile(dps_file)

    tab_east, tab_west = st.tabs(["REGION EAST", "REGION WEST"])

    # --- HELPER PROCESSING LOGIC ---
    def process_region(f_mps, f_dps, s_mps, s_dps):
        # 1. Read data
        df_mps = pd.read_excel(f_mps, sheet_name=s_mps)
        df_dps = pd.read_excel(f_dps, sheet_name=s_dps)
        
        # 2. Merge data with DPS rows first, then MPS
        df_dps['_source_priority'] = 0
        df_mps['_source_priority'] = 1
        combined = pd.concat([df_dps, df_mps], ignore_index=True)
        
        # 3. Keep only requested columns (when available)
        existing_cols = [c for c in SELECTED_COLUMNS if c in combined.columns]
        combined = combined[existing_cols]
        
        # 4. Normalize Release Time format to YYYY-MM-DD
        if RELEASE_TIME_COL in combined.columns:
            release_time_dt = pd.to_datetime(combined[RELEASE_TIME_COL], errors='coerce')
            combined[RELEASE_TIME_COL] = release_time_dt.dt.strftime('%Y-%m-%d')
            # Convert invalid/empty values to empty string instead of NaN
            combined[RELEASE_TIME_COL] = combined[RELEASE_TIME_COL].fillna('')

        # 5. Sort by Date (Month), then Line, then source priority (DPS -> MPS)
        if 'Date' in combined.columns:
            # Temporary datetime conversion for chronological sorting
            combined['Date_dt'] = pd.to_datetime(combined['Date'], errors='coerce')
            
            sort_cols = ['Date_dt']
            if 'Line' in combined.columns:
                sort_cols.append('Line')
            if '_source_priority' in combined.columns:
                sort_cols.append('_source_priority')
            
            combined = combined.sort_values(by=sort_cols, kind='mergesort')
            # Remove temporary datetime helper column
            combined = combined.drop(columns=['Date_dt'])

        # Remove internal helper column before output
        if '_source_priority' in combined.columns:
            combined = combined.drop(columns=['_source_priority'])
        
        return combined

    # --- SECTION EAST ---
    with tab_east:
        st.subheader("East Region Configuration")
        col_e1, col_e2 = st.columns(2)
        with col_e1:
            s_mps_e = st.selectbox("Select East sheet from MPS:", xl_mps.sheet_names, 
                                   index=xl_mps.sheet_names.index('All_East') if 'All_East' in xl_mps.sheet_names else 0, key="se1")
        with col_e2:
            s_dps_e = st.selectbox("Select East sheet from DPS:", xl_dps.sheet_names,
                                   index=xl_dps.sheet_names.index('All_East') if 'All_East' in xl_dps.sheet_names else 0, key="se2")
        
        if st.button("Process & Download East Only", key="btn_east"):
            try:
                final_east = process_region(mps_file, dps_file, s_mps_e, s_dps_e)
                
                output = io.BytesIO()
                # Use openpyxl engine for better xlsx compatibility
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    final_east.to_excel(writer, sheet_name='Combined_East', index=False)
                
                st.success("East data processed successfully.")
                st.download_button("Download East Excel", output.getvalue(), "East_Combined.xlsx", key="dl_e")
            except Exception as e:
                st.error(f"Error: {e}")

    # --- SECTION WEST ---
    with tab_west:
        st.subheader("West Region Configuration")
        col_w1, col_w2 = st.columns(2)
        with col_w1:
            s_mps_w = st.selectbox("Select West sheet from MPS:", xl_mps.sheet_names,
                                   index=xl_mps.sheet_names.index('All_West') if 'All_West' in xl_mps.sheet_names else 0, key="sw1")
        with col_w2:
            s_dps_w = st.selectbox("Select West sheet from DPS:", xl_dps.sheet_names,
                                   index=xl_dps.sheet_names.index('All_West') if 'All_West' in xl_dps.sheet_names else 0, key="sw2")
        
        if st.button("Process & Download West Only", key="btn_west"):
            try:
                final_west = process_region(mps_file, dps_file, s_mps_w, s_dps_w)
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    final_west.to_excel(writer, sheet_name='Combined_West', index=False)
                
                st.success("West data processed successfully.")
                st.download_button("Download West Excel", output.getvalue(), "West_Combined.xlsx", key="dl_w")
            except Exception as e:
                st.error(f"Error: {e}")

else:
    st.info("Please upload both files (MPS & DPS) to start.")