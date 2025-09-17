import streamlit as st
from configuracao import obter_api_base

st.set_page_config(page_title="KPIs Hospitalar", page_icon="🩺", layout="wide")
st.title("Dashboard Analitico - KPIs ")

st.markdown(
    f"""
    Bem-vindo! Use o menu lateral **Páginas** para navegar.

    **API Base**: `{obter_api_base()}`
    """
)