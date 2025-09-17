import requests
import streamlit as st
from configuracao import obter_api_base

@st.cache_data(ttl=60, show_spinner=False)
def obter_kpi_gestao(setor: str, inicio: str, fim: str) -> dict:
    url = f"{obter_api_base()}/kpi/gestao"
    params = {"setor": setor, "inicio": inicio, "fim": fim}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

@st.cache_data(ttl=60, show_spinner=False)
def obter_kpi_paciente(id_internacao: str) -> dict:
    url = f"{obter_api_base()}/kpi/paciente/{id_internacao}"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()