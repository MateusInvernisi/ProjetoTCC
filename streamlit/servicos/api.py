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

@st.cache_data(ttl=300)
def carregar_setores():
    r = requests.get(f"{obter_api_base()}/setores", timeout=10)
    r.raise_for_status()
    data = r.json()
    itens = data.get("setores", [])
    opcoes_rotulo = [it.get("nome") or it["id_setor"] for it in itens]
    mapa_rotulo_para_id = {(it.get("nome") or it["id_setor"]): it["id_setor"] for it in itens}
    return opcoes_rotulo, mapa_rotulo_para_id
