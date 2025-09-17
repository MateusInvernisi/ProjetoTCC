from datetime import datetime, timezone
from typing import Dict, Any, List, DefaultDict
from collections import defaultdict

from db_connection import get_db
from utils.time import diferenca_dias, diferenca_horas, data_iso_utc

#####################################################
# Entrada:     nome do exame (variações/minúsculas) valor (numérico ou str numérica)
# Saída:       str - flag (↑, ↓, acidose, alcalose) ou ""
# Descrição:   Regras simples para destacar resultados fora de
#              referência. Ajustar limiares conforme protocolo local.
#####################################################
def _lab_flag(exame: str, valor) -> str:
    if valor is None:
        return ""
    ex = (exame or "").lower()
    try:
        v = float(valor)
    except Exception:
        return ""

    if ex == "creatinina": return "↑" if v > 1.2 else ""
    if ex == "ureia":      return "↑" if v > 50 else ""
    if ex == "hb":         return "↓" if v < 12 else ""
    if ex == "leucocitos": return "↑" if v > 11000 else ("↓" if v < 4000 else "")
    if ex == "plaquetas":  return "↓" if v < 150000 else ""
    if ex == "ph":         return "acidose (↓)" if v < 7.35 else ("alcalose (↑)" if v > 7.45 else "")
    if ex == "pco2":       return "↑" if v > 45 else ("↓" if v < 35 else "")
    if ex == "po2":        return "↓" if v < 60 else ""
    if ex == "hco3":       return "↓" if v < 22 else ("↑" if v > 26 else "")
    if ex == "lactato":    return "↑" if v > 2.0 else ""
    if ex == "glicemia":   return "↓" if v < 70 else ("↑" if v > 180 else "")
    return ""

#####################################################
# Função:      build_kpi_paciente
# Entrada:     identificador único da internação, id_setor (str|None)
# Saída:       Dict[str, Any] - documento JSON com KPIs individuais
# Descrição:   KPIs por internação: tempo total, ventilação (tempo
#              total e tempo até 1ª intubação, incluindo períodos
#              em aberto), reintubação 48h, dispositivos, antibióticos
#              e laboratoriais (últimos via aggregate + séries).
#####################################################
def build_kpi_paciente(id_internacao: str, id_setor: str | None = None) -> Dict[str, Any]:
    # -----------------------
    # Contexto da internação
    # -----------------------
    db = get_db()
    it = db.internacoes.find_one({"id_internacao": id_internacao})
    if not it:
        return {"error": "internação não encontrada"}

    adm_ts = it["admissao_ts"]
    alta_ts = it.get("alta_ts")
    now_utc = datetime.now(timezone.utc)

    if alta_ts:
        tempo_total_internacao_d = diferenca_dias(adm_ts, alta_ts)
        status = it.get("desfecho", "alta")
    else:
        tempo_total_internacao_d = diferenca_dias(adm_ts, now_utc)
        status = "internado"

    # -----------------------
    # Ventilação mecânica
    # - Inclui períodos abertos (fim ausente -> now_utc)
    # - Tempo até 1ª intubação
    # - Flag de reintubação em 48h
    # -----------------------
    vent = db.ventilacao.find_one({"id_internacao": id_internacao}) or {}
    periodos: List[Dict[str, Any]] = vent.get("periodos", []) or []

    total_secs = 0.0
    periodos_serializados = []
    for p in periodos:
        ini = p.get("inicio")
        fim = p.get("fim")
        end_ts = fim or now_utc  # inclui período aberto
        # acumula apenas quando há início válido
        if ini:
            total_secs += (end_ts - ini).total_seconds()
        periodos_serializados.append({
            "tipo": "ventilacao",
            "inicio": data_iso_utc(ini) if ini else None,
            "fim": data_iso_utc(fim) if fim else None,
            "fonte_fim": p.get("fonte_fim", "")
        })

    tempo_total_vent_d = total_secs / 86400.0 if periodos else 0.0
    t_first_int_h = (
        diferenca_horas(adm_ts, min(vent["intubacoes_ts"]))
        if vent.get("intubacoes_ts") else None
    )

    # Reintubação 48h (extubação -> próxima intubação)
    reint_flag = False
    ext = sorted(vent.get("extubacoes_ts", []))
    intu = sorted(vent.get("intubacoes_ts", []))
    for e in ext:
        prox = next((i for i in intu if i > e), None)
        if prox and diferenca_horas(e, prox) <= 48.0:
            reint_flag = True
            break

    # -----------------------
    # Dispositivos em uso
    # - Mapeamento robusto: CVC, Foley, Art Line, Outros
    # -----------------------
    disp_cur = db.dispositivo_uso.find({"id_internacao": id_internacao})
    disp = {"cvc": [], "foley": [], "art_line": [], "outros": []}
    for d in disp_cur:
        tipo = (d.get("tipo") or "").lower()
        if   tipo == "cvc":   key = "cvc"
        elif tipo == "foley": key = "foley"
        elif tipo in ("art", "art_line", "arterial", "art."): key = "art_line"
        else: key = "outros"
        disp[key].append({
            "inicio": data_iso_utc(d["inicio"]) if d.get("inicio") else None,
            "fim": data_iso_utc(d["fim"]) if d.get("fim") else None,
            "fonte_fim": d.get("fonte_fim", ""),
            "tipo_raw": d.get("tipo", "")
        })

    # -----------------------
    # Antibióticos
    # - DOT e linhas do tempo
    # -----------------------
    ab_docs = list(db.antibioticos_uso.find({"id_internacao": id_internacao}))
    ab_ids = [x["_id"] for x in ab_docs]

    # Mapa de períodos por antibiótico (linhas do tempo)
    per_map: Dict[str, List[Dict[str, str]]] = {}
    if ab_ids:
        for p in db.antibiotico_periodos.find({"id_ab_uso": {"$in": ab_ids}}):
            per_map.setdefault(str(p["id_ab_uso"]), []).append({
                "inicio": p["inicio"].date().isoformat(),
                "fim": p["fim"].date().isoformat()
            })

    dot_por_ab = [{"antibiotico": d["antibiotico"], "dot_dias": d.get("dot_dias", 0)} for d in ab_docs]
    linhas = [{"antibiotico": d["antibiotico"], "periodos": per_map.get(str(d["_id"]), [])} for d in ab_docs]

    # -----------------------
    # Laboratoriais
    # - Últimos via aggregate único (por exame)
    # - Séries via consulta única e bucket por exame
    # -----------------------
    exames_chave = ["creatinina", "ureia", "hb", "leucocitos", "plaquetas",
                    "ph", "pco2", "po2", "hco3", "lactato", "glicemia"]

    # Aggregate para últimos (um doc por exame)
    ultimos_docs = list(db.labs.aggregate([
        {"$match": {"id_internacao": id_internacao, "exame": {"$in": exames_chave}}},
        {"$sort": {"exame": 1, "ts": -1}},
        {"$group": {
            "_id": "$exame",
            "ts": {"$first": "$ts"},
            "valor": {"$first": "$valor"},
            "unidade": {"$first": "$unidade"}
        }},
        {"$project": {"_id": 0, "exame": "$_id", "ts": 1, "valor": 1, "unidade": 1}}
    ]))

    ultimos: Dict[str, Any] = {}
    for doc in ultimos_docs:
        ex = (doc.get("exame") or "").lower()
        ultimos[ex] = {
            "valor": doc.get("valor"),
            "unidade": doc.get("unidade", ""),
            "flag": _lab_flag(ex, doc.get("valor")),
            "data": data_iso_utc(doc["ts"]) if doc.get("ts") else None
        }

    # Séries: uma única consulta, ordenada por ts crescente
    series_bucket: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
    cur = db.labs.find({"id_internacao": id_internacao, "exame": {"$in": exames_chave}}).sort("ts", 1)
    for x in cur:
        ex = (x.get("exame") or "").lower()
        series_bucket[ex].append({"ts": data_iso_utc(x["ts"]), "valor": x.get("valor")})

    series = {
        "creatinina": series_bucket.get("creatinina", []),
        "ureia": series_bucket.get("ureia", []),
        "hemograma": {
            "hb": series_bucket.get("hb", []),
            "htc": series_bucket.get("htc", []),
            "leucocitos": series_bucket.get("leucocitos", []),
        },
        "plaquetas": series_bucket.get("plaquetas", []),
        "gasometria": {
            "ph": series_bucket.get("ph", []),
            "pco2": series_bucket.get("pco2", []),
            "po2": series_bucket.get("po2", []),
            "hco3": series_bucket.get("hco3", []),
            "lactato": series_bucket.get("lactato", []),
        },
        "glicemia": series_bucket.get("glicemia", []),
    }

    # -----------------------
    # Sinalização de passagem no setor opcional
    # -----------------------
    if id_setor:
        est_setor = db.estadas_setor.find_one({"id_internacao": id_internacao, "id_setor": id_setor})
        id_setor_out = id_setor if est_setor else ""
    else:
        est_setor = None
        id_setor_out = ""

    # -----------------------
    # Documento de saída
    # -----------------------
    return {
        "id_internacao": id_internacao,
        "id_setor": id_setor_out,
        "id_paciente": it["id_paciente"],
        "admissao_ts": data_iso_utc(it["admissao_ts"]),
        "alta_ts": data_iso_utc(it["alta_ts"]) if it.get("alta_ts") else None,
        "status": status,
        "tempo_total_internacao_d": round(tempo_total_internacao_d, 2),
        "ventilacao": {
            "tempo_total_d": round(tempo_total_vent_d, 2),
            "tempo_ate_primeira_intubacao_h": round(t_first_int_h, 2) if t_first_int_h is not None else None,
            "periodos": periodos_serializados,
            "extubacoes": [{"ts": data_iso_utc(t)} for t in vent.get("extubacoes_ts", [])],
            "reintubacao_48h_flag": reint_flag
        },
        "dispositivos": disp,
        "antibioticos": {
            "dot_por_antibiotico": dot_por_ab,
            "linhas_do_tempo": linhas
        },
        "labs": {"ultimos": ultimos, "series": series}
    }
