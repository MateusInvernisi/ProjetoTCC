from datetime import datetime, timezone
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from db_connection import get_db
from indicadores.gestao import build_kpi_cti_gestao
from indicadores.paciente import build_kpi_paciente


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
