import streamlit as st
import pandas as pd
from pymongo import MongoClient
import pydeck as pdk
from streamlit_autorefresh import st_autorefresh
import reverse_geocoder as rg
import pycountry
from mongo_queries import (
    get_unique_years,
    get_threshold_color,
    get_heatmap_weight,
    get_total_rows,
    get_selected_year_data,
    get_all_data
)

st.set_page_config(page_title="Radiation Visualizer", layout="wide")
st_autorefresh(interval=7000, key="datarefresh")

client = MongoClient("mongodb://mongodb:27017/")
db = client["radiation_db"]
collection = db["filtered_data"]

st.markdown("<h1 style='text-align: center;'>🌍 Radiation Data Visualization</h1>", unsafe_allow_html=True)

def clean_doc(doc):
    doc = dict(doc)
    doc.pop("_id", None)
    for key in doc:
        if hasattr(doc[key], "isoformat"):
            doc[key] = doc[key].isoformat()
    return doc


if collection.find_one() is None:
    st.warning("No data found in MongoDB.")
else:
    unique_years = get_unique_years(collection)

    st.markdown("### Controls")
    col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 1])
    with col1:
        selected_year = st.selectbox("Select Year", unique_years)
    with col2:
        marker = st.slider("Marker Size", min_value=10, max_value=10000, value=10000, step=1000)
    with col3:
        threshold = st.number_input("Radiation Threshold", min_value=0.0, value=50.0)
    with col4:
        layer_type = st.selectbox("Map Layer", ("HeatmapLayer", "ScatterplotLayer", "ColumnLayer"))
    with col5:
        tooltip_field = st.selectbox("Radiation Unit", ("Counts per minute (CPM)", "Microsieverts per hour (µSv/h)"))

    threshold_color_result = get_threshold_color(collection, threshold, clean_doc)
    heatmap_weight_result = get_heatmap_weight(collection, threshold, clean_doc)

    # Always fetch only the latest 1000 data points for the selected year
    window_size = 1000
    total_rows = get_total_rows(collection, selected_year)
    offset = max(0, total_rows - window_size)
    selected_year_result = get_selected_year_data(collection, selected_year, offset, window_size, clean_doc)
    st.info(f"Showing latest {window_size} rows of {total_rows} total rows for {selected_year}")
    selected_coords = set((d['latitude'], d['longitude']) for d in selected_year_result)
    threshold_color_result_filtered = [
        d for d in threshold_color_result if (d['latitude'], d['longitude']) in selected_coords
    ]
    heatmap_weight_result_filtered = [
        d for d in heatmap_weight_result if (d['latitude'], d['longitude']) in selected_coords
    ]

    coords = [(d["latitude"], d["longitude"]) for d in selected_year_result]
    if coords:
        results = rg.search(coords, mode=1)
        countries = [r["cc"] for r in results]
    else:
        countries = []
    # Map country code to full country name
    for d, country_code in zip(selected_year_result, countries):
        try:
            country_obj = pycountry.countries.get(alpha_2=country_code)
            d["country"] = country_obj.name if country_obj else country_code
        except Exception:
            d["country"] = country_code

    df_table = pd.DataFrame(selected_year_result)
    st.markdown("### Map")
    view_state = pdk.ViewState(
        latitude=0,
        longitude=0,
        zoom=1,
        pitch=0,
    )

    if tooltip_field == "Counts per minute (CPM)":
        tooltip = {"text": "CPM: {cpm}"}
    else:
        tooltip = {"text": "µSv/h: {micro_sv_h}"}

    if layer_type == "HeatmapLayer":
        layer = pdk.Layer(
            "HeatmapLayer",
            data=heatmap_weight_result_filtered,
            get_position="[longitude, latitude]",
            aggregation='"SUM"',
            get_weight="heatmap_weight",
            pickable=True,
        )
    elif layer_type == "ScatterplotLayer":
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=threshold_color_result_filtered,
            get_position="[longitude, latitude]",
            get_color="color",
            get_radius=marker,
            pickable=True,
        )
    elif layer_type == "ColumnLayer":
        layer = pdk.Layer(
            "ColumnLayer",
            data=threshold_color_result_filtered,
            get_position="[longitude, latitude]",
            get_elevation="cpm",
            elevation_scale=10000,
            radius=marker // 10,
            get_fill_color="color",
            pickable=True,
            auto_highlight=True,
        )

    r = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip=tooltip,
        parameters={
            "getCursor": "function({isDragging, isHovering, pickedInfos}) { if (isDragging) return 'grabbing'; if (pickedInfos && pickedInfos.length > 0) return 'pointer'; return isHovering ? 'pointer' : 'grab'; }"
        },
    )
    st.pydeck_chart(r, use_container_width=True)

    st.markdown("### Analysis")
    all_data_result = get_all_data(collection, clean_doc)

    coords = [(d["latitude"], d["longitude"]) for d in all_data_result]
    if coords:
        results = rg.search(coords, mode=1)
        countries = [r["cc"] for r in results]
    else:
        countries = []
    for d, country_code in zip(all_data_result, countries):
        try:
            country_obj = pycountry.countries.get(alpha_2=country_code)
            d["country"] = country_obj.name if country_obj else country_code
        except Exception:
            d["country"] = country_code

    df_analysis = pd.DataFrame(all_data_result)

    if not df_analysis.empty:
        summary = df_analysis.groupby("country")["cpm"].agg(['count', 'sum', 'mean', 'max', 'min']).reset_index()
        summary.rename(columns={'count': 'Total Measurements', 'sum': 'Total CPM', 'mean': 'Mean CPM'}, inplace=True)
        st.dataframe(summary)
    else:
        st.info("No analysis data available.")

    st.markdown("### Radiation values by country for selected year")
    # Pagination for data table
    page_size = 20
    total_pages = (len(df_table) + page_size - 1) // page_size
    page = st.number_input(
        "Page", min_value=1, max_value=max(1, total_pages), value=1, step=1
    )
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size
    if not df_table.empty:
        st.dataframe(
            df_table[["country", "cpm", "latitude", "longitude"]].iloc[
                start_idx:end_idx
            ]
        )
        st.caption(f"Page {page} of {total_pages}")
    else:
        st.info("No data for the selected year.")
