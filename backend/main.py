from datetime import datetime, timezone
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from db_connection import get_db
from indicadores.gestao import build_kpi_cti_gestao
from indicadores.paciente import build_kpi_paciente
from typing import Dict


load_dotenv()

#####################################################
# Seção:        Configuração do aplicativo FastAPI
# Descrição:    Define metadados da API e habilita CORS amplo
#               para permitir chamadas do dashboard/cliente.
#####################################################
app = FastAPI(
    title="KPIs CTI - API (mínima)",
    version="0.1.0",
    description="API mínima com /health para validar conexão ao MongoDB Atlas."
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

#####################################################
# Função:      health
# Rota:        GET /health
# Saída:       JSON com status da aplicação e do banco.
#####################################################
@app.get("/health")
def health():
    try:
        db = get_db()
        _ = db.list_collection_names()
        return {"status": "ok", "db": "up"}
    except Exception as e:
        return {"status": "error", "db": "down", "detail": str(e)}

#####################################################
# Função:      parse_date
# Entrada:     date_str (str) no formato YYYY-MM-DD.
# Saída:       datetime em UTC (timezone.utc).
#####################################################
def parse_date(date_str: str) -> datetime:
    try:
        return datetime.fromisoformat(date_str + "T00:00:00+00:00").astimezone(timezone.utc)
    except Exception:
        raise HTTPException(400, f"Data inválida: {date_str}. Use YYYY-MM-DD.")

#####################################################
# Função:      kpi_gestao
# Rota:        GET /kpi/gestao
# Parâmetros:  setor (str) - ID do setor (ex.: CTI-ADULTO)
#              inicio (str) - data inicial (YYYY-MM-DD)
#              fim (str)    - data final   (YYYY-MM-DD)
# Saída:       JSON com KPIs de gestão do setor no período.
#####################################################
@app.get("/kpi/gestao")
def kpi_gestao(
    setor: str = Query(..., description="ID do setor, ex: CTI-ADULTO"),
    inicio: str = Query(..., regex=r"\d{4}-\d{2}-\d{2}"),
    fim: str = Query(..., regex=r"\d{4}-\d{2}-\d{2}")
):
    ini = parse_date(inicio)
    end = parse_date(fim)
    return build_kpi_cti_gestao(setor, ini, end)

#####################################################
# Função:      kpi_paciente
# Rota:        GET /kpi/paciente/{id_internacao}
# Parâmetros:  id_internacao (str) - identificador da internação.
# Saída:       Constrói JSON com KPIs individuais chamando
#              build_kpi_paciente(id_internacao).
#####################################################
@app.get("/kpi/paciente/{id_internacao}")
def kpi_paciente(id_internacao: str):
    data = build_kpi_paciente(id_internacao)
    if "error" in data:
        raise HTTPException(404, data["error"])
    return data

#####################################################
# Função:      kpi_paciente
# Rota:        GET /kpi/paciente/{id_internacao}
#####################################################
@app.get("/kpi/paciente/{id_internacao}")
def kpi_paciente(id_internacao: str):
    data = build_kpi_paciente(id_internacao)
    if "error" in data:
        raise HTTPException(404, data["error"])
    return data

#####################################################
# Função:      listar_pacientes_internados_no_setor
# Rota:        GET /pacientes/internados?setor=...
#####################################################
@app.get("/pacientes/internados", tags=["pacientes"])
def listar_pacientes_internados_no_setor(setor: str) -> Dict:
    try:
        db = get_db()
        estadas_coll = db["cti.estadas_setor"]
        internacoes_coll = db["cti.internacoes"]

        filtro_estadas = {
            "id_setor": setor,
            "$or": [{"fim": None}, {"fim": {"$exists": False}}]
        }
        estadas = list(estadas_coll.find(
            filtro_estadas,
            {"_id": 0, "id_internacao": 1, "inicio": 1}
        ))
        ids_internacao = [e["id_internacao"] for e in estadas]

        if not ids_internacao:
            return {"setor": setor, "pacientes": []}

        cur = internacoes_coll.find(
            {"id_internacao": {"$in": ids_internacao}, "alta_ts": None},
            {"_id": 0, "id_internacao": 1, "id_paciente": 1, "admissao_ts": 1}
        )

        uniq = {}
        for doc in cur:
            uniq[doc["id_paciente"]] = {
                "id_paciente": doc["id_paciente"],
                "id_internacao": doc["id_internacao"],
                "admissao_ts": doc.get("admissao_ts")
            }

        pacientes = sorted(
            uniq.values(),
            key=lambda x: x.get("admissao_ts") or ""
        )

        return {"setor": setor, "pacientes": pacientes}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar pacientes internados: {e}")


#####################################################
# Função:      listar_setores
# Rota:        GET /setores
#####################################################
@app.get("/setores", tags=["referenciais"])
def listar_setores():
    try:
        db = get_db()  # <-- importante
        coll_setores = db["cti.setores"]
        cur = coll_setores.find({}, {"_id": 0, "id_setor": 1, "nome": 1}).sort("id_setor", 1)
        setores = [
            {"id_setor": d["id_setor"], "nome": d.get("nome", d["id_setor"])}
            for d in cur
        ]

        if not setores:
            distintos = sorted(db["cti.estadas_setor"].distinct("id_setor"))
            setores = [{"id_setor": s, "nome": s} for s in distintos]

        return {"setores": setores}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao listar setores: {e}")
