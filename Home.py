import streamlit as st

def main():
    st.set_page_config(page_title="Supply Chain Hub", layout="wide")

    st.title("🏭 Supply Chain Planning System")
    st.markdown("---")

    # Use columns for feature layout
    col1, col2 = st.columns(2)

    with col1:
        st.header("🔄 MPS to DPS Converter")
        st.info("""
        **Function:** Convert MPS Cycle files into daily DPS format.
        
        **Key Features:**
        - Automatic region mapping (East/West).
        - Flexible production period filtering.
        - Sequence determination based on SKU priority list.
        - Excel output with automatic formatting.
        """)
        if st.button("Open Converter"):
            st.switch_page("pages/1_MPS_to_DPS_Converter.py")

    with col2:
        st.header("🔗 MPS & DPS Merger")
        st.success("""
        **Function:** Merge MPS Final data with daily DPS data.
        
        **Key Features:**
        - Dynamic sheet selection from both files.
        - Separate navigation for East and West regions.
        - Automatic column cleanup (only 12 main columns).
        - Sorting by month and production line.
        """)
        if st.button("Open Merger"):
            st.switch_page("pages/2_MPS_DPS_Merger.py")

    st.markdown("---")
    st.caption("© 2026 Supply Chain Digitalization Team")

if __name__ == "__main__":
    main()