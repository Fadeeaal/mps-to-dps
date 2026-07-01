import streamlit as st

st.set_page_config(
    page_title="MPS - DPS Converter & Merger",
    page_icon="📊",
    layout="wide"
)

# =========================
# HEADER
# =========================
st.title("📊 MPS - DPS Converter & Merger")
st.caption(
    "Centralized tool for converting MPS Cycle files into DPS format "
    "and consolidating MPS Final data with daily DPS data."
)

st.markdown("---")

# =========================
# INTRO
# =========================
st.markdown(
    """
    Welcome to the **MPS - DPS Converter & Merger** 👋  

    This application is designed to support Supply Chain Planning activities through 
    **automated MPS-to-DPS conversion**, **daily DPS formatting**, and 
    **MPS-DPS data consolidation**.

    The tool helps reduce manual data preparation, improve consistency across planning files, 
    and prepare structured outputs for further analysis or reporting.

    👉 Please select a feature from the **sidebar on the left** to get started.
    """
)

st.markdown("")

# =========================
# FEATURE CARDS
# =========================
c1, c2 = st.columns(2)

with c1:
    st.subheader("🔄 MPS to DPS Converter")
    st.write(
        "Convert MPS Cycle files into daily DPS format with automatic region mapping, "
        "production period filtering, SKU priority sequencing, and formatted Excel output."
    )

with c2:
    st.subheader("🔗 MPS & DPS Merger")
    st.write(
        "Merge MPS Final data with daily DPS data using dynamic sheet selection, "
        "East and West region navigation, automatic column cleanup, and structured sorting "
        "by month and production line."
    )

st.markdown("")

# =========================
# PROCESS OVERVIEW
# =========================
st.markdown("### ⚙️ Process Overview")

p1, p2, p3 = st.columns(3)

with p1:
    st.markdown("#### 1️⃣ Upload Files")
    st.write(
        "Upload the required MPS Cycle, MPS Final, or daily DPS files based on the selected feature."
    )

with p2:
    st.markdown("#### 2️⃣ Configure Data")
    st.write(
        "Select the required sheet, region, production period, or processing mode."
    )

with p3:
    st.markdown("#### 3️⃣ Export Output")
    st.write(
        "Generate a clean and structured Excel output ready for planning, validation, or reporting."
    )

st.markdown("---")

# =========================
# FOOTER
# =========================
st.caption(
    "⚙️ Built with Streamlit | "
    "📊 Supply Chain Planning Automation Tool | "
    "🔒 Internal Supply Chain Tools - Danone Indonesia"
)