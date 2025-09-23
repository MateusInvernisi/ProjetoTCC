# -*- coding: utf-8 -*-
import requests
from requests.exceptions import HTTPError
from configuracao import obter_api_base

API_BASE = obter_api_base().rstrip("/")

def _get(path: str, params: dict | None = None, timeout: int = 30) -> dict:
    url = f"{API_BASE}{'' if path.startswith('/') else '/'}{path}"
    r = requests.get(url, params=params or {}, timeout=timeout)
    r.raise_for_status()
    return r.json()

# -----------------------------
# KPI Gestão
# -----------------------------
def obter_kpi_gestao(setor: str, inicio: str, fim: str, persistir: bool = True) -> dict:
    return _get(
        "/kpi/gestao",
        {"setor": setor, "inicio": inicio, "fim": fim, "persistir": str(persistir).lower()},
        timeout=60,
    )

def carregar_setores() -> list[dict]:
    """
    Retorna lista de setores como [{id_setor, nome}] (se backend já devolver)
    ou adapta caso venha como lista de strings.
    """
    data = _get("/kpi/setores", timeout=15)
    setores = data.get("setores", [])
    if setores and isinstance(setores[0], str):
        return [{"id_setor": s, "nome": s.replace("-", " ").title()} for s in setores]
    return setores

# -----------------------------
# KPI Paciente
# -----------------------------
def obter_kpi_paciente(id_internacao: str) -> dict:
    return _get(f"/kpi/paciente/{id_internacao}", timeout=60)

def pacientes_internados_no_setor(setor: str) -> list[dict]:
    """
    Espera endpoint GET /kpi/pacientes/internados?setor=ID_SETOR
    Resposta: {"pacientes":[{"id_paciente": "...", "id_internacao": "...", "admissao_ts":"...", "nome":"..."?}, ...]}
    """
    data = _get("/kpi/pacientes/internados", {"setor": setor}, timeout=20)
    return data.get("pacientes", [])
