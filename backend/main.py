from datetime import datetime, timezone, date, timedelta
from typing import List, Set, Dict, Any, Tuple
import unicodedata
import traceback

from fastapi import FastAPI, Query, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pymongo.database import Database
from dotenv import load_dotenv

# Imports compatíveis com execução como pacote ou script
try:
    from .db_connection import get_db
    from .indicadores.gestao import build_kpi_cti_gestao
    from .indicadores.paciente import build_kpi_paciente
except Exception:
    from db_connection import get_db
    from indicadores.gestao import build_kpi_cti_gestao
    from indicadores.paciente import build_kpi_paciente

load_dotenv()

app = FastAPI(
    title="KPIs CTI - API",
    version="1.2.3",
    description="Backend para KPIs da CTI (gestão e paciente).",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# ===================== Helpers de datas =====================
def _parse_date_yyyy_mm_dd(s: str) -> date:
    try:
        return date.fromisoformat(s)
    except Exception:
        raise HTTPException(400, f"Data inválida: {s}. Use YYYY-MM-DD.")

def _dt_utc(d: date, end_exclusive: bool = False) -> datetime:
    base = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    return base + (timedelta(days=1) if end_exclusive else timedelta())

# ===================== Normalização de setor =====================
_HYPHENS = {"\u2010", "\u2011", "\u2012", "\u2013", "\u2014", "\u2212"}

def _norm_basic(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKC", s).replace("\u00A0", " ")
    for h in _HYPHENS:
        s = s.replace(h, "-")
    return " ".join(s.strip().split())

def _norm_code(s: str) -> str:
    s = _norm_basic(s).replace(" ", "-")
    return s.upper()

def _norm_name(s: str) -> str:
    return _norm_basic(s)

def _setores_list(db: Database) -> List[Dict[str, str]]:
    nomes_cols = set(db.list_collection_names())
    for col in ("cti.setores", "setores"):
        if col in nomes_cols:
            docs = list(db[col].find({}, {"_id": 0, "id_setor": 1, "nome": 1}))
            out, seen = [], set()
            for d in docs:
                raw = _norm_code(d.get("id_setor") or "")
                if not raw or raw in seen:
                    continue
                nome = _norm_name(d.get("nome") or raw.replace("-", " ").title())
                out.append({"id_setor": raw, "nome": nome})
                seen.add(raw)
            if out:
                return out
    candidatos: Set[str] = set()
    for col in (
        "cti.estadas_setor", "estadas_setor",
        "cti.paciente_dia_setor", "paciente_dia_setor",
        "cti.kpi_cti_gestao", "kpi_cti_gestao",
    ):
        if col in nomes_cols:
            try:
                for c in db[col].distinct("id_setor"):
                    c = _norm_code(c or "")
                    if c:
                        candidatos.add(c)
            except Exception:
                pass
    return [{"id_setor": s, "nome": s.replace("-", " ").title()} for s in sorted(candidatos)]

def _build_resolvers(db: Database) -> Tuple[Dict[str, str], Dict[str, str]]:
    from unicodedata import normalize
    def strip_accents(x: str) -> str:
        x = normalize("NFKD", x)
        return "".join(ch for ch in x if unicodedata.category(ch) != "Mn")
    setores = _setores_list(db)
    by_code, by_name = {}, {}
    for s in setores:
        code = _norm_code(s["id_setor"])
        name = _norm_name(s.get("nome") or code.replace("-", " ").title())
        by_code[code] = code
        by_code[code.replace("-", " ")] = code
        by_code[code.replace("-", "")] = code
        by_name[strip_accents(name).lower()] = code
    return by_code, by_name

def _canonicalize_setor(db: Database, setor_input: str) -> str:
    s_in_raw = setor_input or ""
    s_in = _norm_code(s_in_raw)
    by_code, by_name = _build_resolvers(db)
    if s_in in by_code:
        return by_code[s_in]
    for alt in (s_in.replace("-", " "), s_in.replace("-", "")):
        if alt in by_code:
            return by_code[alt]
    from unicodedata import normalize
    def strip_accents(x: str) -> str:
        x = normalize("NFKD", x)
        return "".join(ch for ch in x if unicodedata.category(ch) != "Mn")
    name_key = strip_accents(_norm_name(s_in_raw)).lower()
    if name_key in by_name:
        return by_name[name_key]
    sugestoes = sorted(set(by_code.values()))[:12]
    raise HTTPException(404, detail={"erro": f"Setor '{setor_input}' não encontrado.", "tente_um_dos": sugestoes})

# ===================== Util helpers =====================
def _resolve_col(db: Database, candidates: List[str]) -> str:
    names = set(db.list_collection_names())
    for c in candidates:
        if c in names:
            return c
    return candidates[0]

# ===================== Endpoints =====================
@app.get("/health")
def health():
    try:
        db = get_db()
        _ = db.list_collection_names()
        return {"status": "ok", "db": "up", "version": app.version}
    except Exception as e:
        return {"status": "error", "db": "down", "detail": str(e), "version": app.version}

@app.get("/kpi/setores", tags=["referenciais"])
def listar_setores(db: Database = Depends(get_db)) -> Dict[str, List[Dict[str, str]]]:
    try:
        return {"setores": _setores_list(db)}
    except Exception as e:
        raise HTTPException(500, f"Erro ao listar setores: {e}")

@app.get("/kpi/gestao", tags=["gestao"])
def kpi_gestao(
    setor: str = Query(..., description="ID do setor (ex.: CTI-ADULTO) ou o nome (ex.: CTI Adulto)"),
    inicio: str = Query(..., regex=r"^\d{4}-\d{2}-\d{2}$"),
    fim: str = Query(..., regex=r"^\d{4}-\d{2}-\d{2}$"),
    persistir: bool = Query(True, description="Se true, salva/atualiza em cti.kpi_cti_gestao"),
    db: Database = Depends(get_db),
):
    ini_d = _parse_date_yyyy_mm_dd(inicio)
    fim_d = _parse_date_yyyy_mm_dd(fim)
    if ini_d > fim_d:
        raise HTTPException(400, "Parâmetro inválido: 'inicio' > 'fim'.")
    ini_dt = _dt_utc(ini_d, end_exclusive=False)
    fim_dt = _dt_utc(fim_d, end_exclusive=True)
    setor_canonico = _canonicalize_setor(db, setor)

    try:
        doc = build_kpi_cti_gestao(setor_canonico, ini_dt, fim_dt)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, f"Falha ao calcular KPIs para setor '{setor_canonico}' entre {inicio} e {fim}: {e}")

    if persistir:
        COL_KPI = _resolve_col(db, ["cti.kpi_cti_gestao", "kpi_cti_gestao"])
        filtro = {"id_setor": doc["id_setor"], "periodo.inicio": doc["periodo"]["inicio"], "periodo.fim": doc["periodo"]["fim"]}
        meta = {"versao": app.version, "gerado_em": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), "status_calculo": "ok"}
        payload = {**doc, **meta}
        db[COL_KPI].update_one(filtro, {"$set": payload}, upsert=True)

    return doc

@app.get("/kpi/paciente/{id_internacao}", tags=["paciente"])
def kpi_paciente(id_internacao: str):
    data = build_kpi_paciente(id_internacao)
    if "error" in data:
        raise HTTPException(404, data["error"])
    return data

# ---------- Pacientes internados no setor (estada aberta + sem alta) ----------
@app.get("/kpi/pacientes/internados", tags=["paciente"])
def pacientes_internados_no_setor(
    setor: str = Query(..., description="ID do setor, ex.: CTI-ADULTO"),
    limit: int = Query(50, ge=1, le=500),
    db: Database = Depends(get_db),
):
    setor_canonico = _canonicalize_setor(db, setor)

    col_estadas = _resolve_col(db, ["cti.estadas_setor", "estadas_setor"])
    col_intern  = _resolve_col(db, ["cti.internacoes", "internacoes"])

    pipeline = [
        {"$match": {
            "id_setor": setor_canonico,
            "$or": [
                {"fim": {"$exists": False}},
                {"fim": None},
                {"fim": ""},
            ],
        }},
        {"$lookup": {
            "from": col_intern,
            "localField": "id_internacao",
            "foreignField": "id_internacao",
            "as": "int"
        }},
        {"$unwind": "$int"},
        {"$match": {
            "$or": [
                {"int.alta_ts": {"$exists": False}},
                {"int.alta_ts": None},
                {"int.alta_ts": ""},
            ]
        }},
        {"$group": {
            "_id": "$id_internacao",
            "id_paciente": {"$first": "$int.id_paciente"},
            "admissao_ts": {"$first": "$int.admissao_ts"},
        }},
        {"$project": {
            "_id": 0,
            "id_internacao": "$_id",
            "id_paciente": 1,
            "admissao_ts": 1,
        }},
        {"$sort": {"admissao_ts": 1}},
        {"$limit": limit},
    ]

    pacientes: List[Dict[str, Any]] = list(db[col_estadas].aggregate(pipeline))
    # enriquecer com nome (se existir)
    nomes_disponiveis = set(db.list_collection_names())
    try:
        ids_pac = [p["id_paciente"] for p in pacientes if p.get("id_paciente")]
        if ids_pac and ("cti.pacientes" in nomes_disponiveis or "pacientes" in nomes_disponiveis):
            col_pac = "cti.pacientes" if "cti.pacientes" in nomes_disponiveis else "pacientes"
            nomes_map = {
                d["id_paciente"]: (d.get("nome") or d.get("nome_completo") or "")
                for d in db[col_pac].find(
                    {"id_paciente": {"$in": ids_pac}}, {"_id": 0, "id_paciente": 1, "nome": 1, "nome_completo": 1}
                )
            }
            for p in pacientes:
                p["nome"] = nomes_map.get(p.get("id_paciente"), "")
    except Exception:
        pass

    return {"pacientes": pacientes}

# ---------- DEBUG: ver coleções e contagens de cada fase ----------
@app.get("/debug/internados_check", tags=["debug"])
def debug_internados_check(
    setor: str = Query(..., description="ID do setor, ex.: CTI-ADULTO"),
    limit: int = Query(10, ge=1, le=200),
    db: Database = Depends(get_db),
):
    setor_canonico = _canonicalize_setor(db, setor)
    col_estadas = _resolve_col(db, ["cti.estadas_setor", "estadas_setor"])
    col_intern  = _resolve_col(db, ["cti.internacoes", "internacoes"])

    q_estadas_abertas = {
        "id_setor": setor_canonico,
        "$or": [
            {"fim": {"$exists": False}},
            {"fim": None},
            {"fim": ""},
        ],
    }
    count_abertas = db[col_estadas].count_documents(q_estadas_abertas)
    amostra_estadas = list(db[col_estadas].find(q_estadas_abertas, {"_id": 0, "id_internacao": 1, "fim": 1}).limit(limit))

    pipeline = [
        {"$match": q_estadas_abertas},
        {"$lookup": {
            "from": col_intern,
            "localField": "id_internacao",
            "foreignField": "id_internacao",
            "as": "int"
        }},
        {"$unwind": "$int"},
        {"$match": {
            "$or": [
                {"int.alta_ts": {"$exists": False}},
                {"int.alta_ts": None},
                {"int.alta_ts": ""},
            ]
        }},
        {"$project": {
            "_id": 0,
            "id_internacao": 1,
            "id_paciente": "$int.id_paciente",
            "admissao_ts": "$int.admissao_ts",
            "alta_ts": "$int.alta_ts",
        }},
        {"$limit": limit},
    ]
    amostra_join = list(db[col_estadas].aggregate(pipeline))
    count_final = len(list(db[col_estadas].aggregate(pipeline[:-1])))  # sem o limit

    return {
        "setor_input": setor,
        "setor_canonico": setor_canonico,
        "col_estadas": col_estadas,
        "col_intern": col_intern,
        "count_estadas_abertas": count_abertas,
        "amostra_estadas_abertas": amostra_estadas,
        "count_final": count_final,
        "amostra_join": amostra_join,
    }

# ===================== Uvicorn local =====================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend.main:app", host="127.0.0.1", port=8000, reload=True)
