import streamlit as st
import pandas as pd
import requests
import plotly.express as px
import json
from datetime import date
from urllib.parse import urlencode

st.set_page_config(page_title="Ocean Monitoring Dashboard", layout="wide")

# --- Load JSON config ---
with open("config.json") as f:
    config = json.load(f)

API_BASE = config["api"]["base_url"]

# --- Sidebar Filters ---
st.sidebar.title("Control Panel")

# Date Range
if config["filters"].get("date_range"):
    start_date, end_date = st.sidebar.date_input(
        "Select Date Range",
        [date(2025, 1, 1), date(2025, 12, 31)]
    )

# Numeric Filters
numeric_filters = {}
for key, (min_val, max_val) in config["filters"]["numeric_filters"].items():
    numeric_filters[key] = st.sidebar.slider(
        f"{key.capitalize()} range",
        min_val,
        max_val,
        (min_val, max_val)
    )

# Pagination
limit = st.sidebar.number_input(
    "Limit per page",
    min_value=10,
    max_value=500,
    value=config["filters"]["pagination"]["limit"]
)
page = st.sidebar.number_input("Page", min_value=1, value=1)


# --- Fetch Data Function ---
@st.cache_data
def fetch_data(start_date, end_date, numeric_filters, limit, page):
    params = {
        "limit": limit,
        "skip": (page - 1) * limit
    }
    # Map filter names to API parameter names
    param_mapping = {
        "temp": "min_temp",
        "sal": "min_sal", 
        "odo": "min_odo"
    }
    
    for key, (min_val, max_val) in numeric_filters.items():
        if key in param_mapping:
            api_key = param_mapping[key]
            params[f"{api_key}"] = min_val
            params[f"{api_key.replace('min_', 'max_')}"] = max_val

    try:
        response = requests.get(API_BASE, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()["items"]  # API returns {"count": X, "items": [...]}
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Failed to fetch data: {e}")
        return pd.DataFrame()


# --- Fetch Data ---
data = fetch_data(start_date, end_date, numeric_filters, limit, page)

# --- Display Data ---
if not data.empty:
    if config["components"].get("table"):
        st.subheader("Data Table")
        st.dataframe(data)

    if config["components"].get("statistics_panel"):
        st.subheader("Statistics Summary")
        st.write(data.describe())

    charts = config["components"].get("charts", {})

    if charts.get("line_chart"):
        fig = px.line(
            data.sort_values("Time hh:mm:ss"),
            x="Time hh:mm:ss",
            y="Temperature (c)",
            title="Temperature Over Time"
        )
        st.plotly_chart(fig, use_container_width=True)

    if charts.get("histogram"):
        fig = px.histogram(data, x="Salinity (ppt)", nbins=20, title="Salinity Distribution")
        st.plotly_chart(fig, use_container_width=True)

    if charts.get("scatter_plot"):
        fig = px.scatter(
            data,
            x="Temperature (c)",
            y="ODO mg/L",
            color="Salinity (ppt)",
            title="Temperature vs ODO (colored by Salinity)"
        )
        st.plotly_chart(fig, use_container_width=True)

    if config["components"].get("map"):
        fig = px.scatter_mapbox(
            data,
            lat="Latitude",
            lon="Longitude",
            color="Temperature (c)",
            size="ODO mg/L",
            zoom=3,
            mapbox_style="open-street-map",
            title="Sample Locations"
        )
        st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("No data returned for current filter settings.")
