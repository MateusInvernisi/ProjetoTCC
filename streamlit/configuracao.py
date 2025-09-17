import os
import streamlit as st

def obter_api_base() -> str:
    # prioridade para secrets em produção
    try:
        api_base = st.secrets["API_BASE"]  # definido em .streamlit/secrets.toml
    except Exception:
        api_base = os.getenv("API_BASE", "http://127.0.0.1:8000")
    return api_base