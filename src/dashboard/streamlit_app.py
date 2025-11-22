import streamlit as st
import pandas as pd

st.set_page_config(page_title="Data Jobs", layout="wide")
st.title("Job Market – Analysis")

@st.cache_data
def load():
    return pd.read_csv("data_raw/adzuna_jobs.csv")

df = load()
st.metric("Total jobs", len(df))
st.subheader("Top Companies")
st.bar_chart(df['company'].value_counts().head(10))
st.subheader("Top Locations")
st.bar_chart(df['location'].value_counts().head(10))
st.subheader("Sample Jobs")
st.dataframe(df.head(50))
