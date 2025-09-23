from datetime import datetime, timezone
from statistics import median
from typing import Dict, Any, List, Tuple, Set, Iterable

from ..db_connection import get_db
from ..utils.time import p90, diferenca_dias, diferenca_horas, data_iso_utc


# ========== Utilidades ==========

def _stats(arr: List[float]) -> Tuple[float, float, float]:
    if not arr:
        return (0.0, 0.0, 0.0)
    return (
        round(sum(arr) / len(arr), 2),
        round(median(arr), 2),
        round(p90(arr), 2),
    )

def _ratio(num: float, den: float, ndigits: int = 4) -> float:
    return round((num / den) if den else 0.0, ndigits)

def _normaliza_destino(s: Dict[str, Any]) -> str:
    dest = (s.get("destino_alta") or "").lower().strip()
    if dest:
        return dest
    dest = (s.get("desfecho") or "alta").lower().strip()
    if dest == "alta":
        return "enfermaria"
    if dest == "obito":
        return "obito"
    if dest == "transferencia":
        return "outro_hospital"
    return dest or "enfermaria"

def _coerce_dt(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.astimezone(timezone.utc)
    try:
        if isinstance(v, str):
            v = v.replace("Z", "+00:00")
        return datetime.fromisoformat(v).astimezone(timezone.utc)
    except Exception:
        return None

def _overlap_seconds(a_start: datetime, a_end: datetime | None,
                     b_start: datetime, b_end: datetime) -> float:
    """
    Segundos de sobreposição entre [a_start, a_end] (a_end pode ser None = aberto)
    e [b_start, b_end). Retorna 0 se não houver sobreposição.
    """
    a0 = _coerce_dt(a_start)
    a1 = _coerce_dt(a_end) if a_end else None
    b0 = _coerce_dt(b_start)
    b1 = _coerce_dt(b_end)
    if not a0 or not b0 or not b1:
        return 0.0
    if a1 is None:
        # janela aberta: usa agora como limite superior
        a1 = datetime.now(timezone.utc)
    ini = max(a0, b0)
    fim = min(a1, b1)
    secs = (fim - ini).total_seconds()
    return secs if secs > 0 else 0.0


# ========== Resolvedor de coleções (cta/sem prefixo) ==========

def _resolve_col(db, candidates: List[str]) -> str:
    """
    Retorna a primeira coleção existente dentre as candidatas.
    Se nenhuma existir, devolve a primeira (será criada sob demanda).
    """
    names = set(db.list_collection_names())
    for c in candidates:
        if c in names:
            return c
    return candidates[0]


# ========== Núcleo (coorte por presença no setor) ==========

def build_kpi_cti_gestao(id_setor: str, inicio_inclusivo: datetime, fim_exclusivo: datetime) -> Dict[str, Any]:
    """
    Calcula os KPIs de gestão para UM setor no intervalo [inicio_inclusivo, fim_exclusivo).

    COORTE (PRESENÇA):
      Internações com pelo menos uma estada em {id_setor} que sobrepôs o período:
      estada.inicio < fim_exclusivo  AND  (estada.fim || now) >= inicio_inclusivo

    MÉTRICAS:
      - LOS (no período, por internação): soma dos trechos de estadas no setor que caem no período.
      - Paciente-dia e proporções de dispositivos: já por data (cti.paciente_dia_setor) dentro do período.
      - Ventilação: soma do tempo ventilado que cai no período (apenas coorte).
      - Antibióticos: DOT por antibiótico somando somente dias que sobrepõem o período (apenas coorte).
      - Mortalidade, destino da alta, readmissão 48h: permanecem baseadas em ALTAS no período (boa prática).
    """
    db = get_db()

    # Coleções (auto-resolve nomes com ou sem prefixo)
    COL_INT    = _resolve_col(db, ["cti.internacoes", "internacoes"])
    COL_EST    = _resolve_col(db, ["cti.estadas_setor", "estadas_setor"])
    COL_PD     = _resolve_col(db, ["cti.paciente_dia_setor", "paciente_dia_setor"])
    COL_VENT   = _resolve_col(db, ["cti.ventilacao", "ventilacao"])
    COL_AB_USO = _resolve_col(db, ["cti.antibioticos_uso", "antibioticos_uso"])
    COL_AB_PER = _resolve_col(db, ["cti.antibiotico_periodos", "antibiotico_periodos"])

    now_utc = datetime.now(timezone.utc)

    # ----------------------
    # 0) Coorte por presença
    # ----------------------
    # Busca estadas do setor que sobrepõem o período
    estadas = list(db[COL_EST].find({
        "id_setor": id_setor,
        "inicio": {"$lt": fim_exclusivo},
        "$or": [
            {"fim": {"$gte": inicio_inclusivo}},
            {"fim": None},
            {"fim": {"$exists": False}},
        ],
    }, {"_id": 0, "id_internacao": 1, "inicio": 1, "fim": 1}))

    # Conjunto de internações presentes no período
    coorte_ids: Set[str] = set([e["id_internacao"] for e in estadas if e.get("id_internacao")])

    # Mapa: id_internacao -> segundos dentro do período (somando estadas sobrepostas)
    secs_no_setor: Dict[str, float] = {}
    for e in estadas:
        iid = e.get("id_internacao")
        if not iid:
            continue
        sec = _overlap_seconds(e.get("inicio"), e.get("fim"), inicio_inclusivo, fim_exclusivo)
        if sec > 0:
            secs_no_setor[iid] = secs_no_setor.get(iid, 0.0) + sec

    # Lista LOS em dias no período (por internação)
    los_periodo_d: List[float] = [s / 86400.0 for s in secs_no_setor.values()]
    los_media, los_mediana, los_p90 = _stats(los_periodo_d)
    pacientes_no_periodo = len(secs_no_setor)

    # ----------------------
    # 1) Altas no período (para mortalidade, readmissão, destino)
    # ----------------------
    saidas = list(db[COL_INT].aggregate([
        {"$match": {"alta_ts": {"$gte": inicio_inclusivo, "$lt": fim_exclusivo}}},
        {"$lookup": {
            "from": COL_EST,
            "let": {"internacao_id": "$id_internacao"},
            "pipeline": [
                {"$match": {"$expr": {"$and": [
                    {"$eq": ["$id_internacao", "$$internacao_id"]},
                    {"$eq": ["$id_setor", id_setor]}
                ]}}}
            ],
            "as": "passou_setor"
        }},
        {"$match": {"passou_setor": {"$ne": []}}},
        {"$project": {
            "_id": 0,
            "id_internacao": 1,
            "id_paciente": 1,
            "admissao_ts": 1,
            "alta_ts": 1,
            "desfecho": 1,
            "destino_alta": 1
        }}
    ]))

    # Ajusta datas
    for s in saidas:
        s["admissao_ts"] = _coerce_dt(s.get("admissao_ts"))
        s["alta_ts"] = _coerce_dt(s.get("alta_ts"))

    quantidade_saidas = len(saidas)
    # LOS por alta (clássico) ainda pode ser interessante pro painel; calculamos também:
    los_alta_list = [
        diferenca_dias(s["admissao_ts"], s["alta_ts"])
        for s in saidas if s.get("admissao_ts") and s.get("alta_ts")
    ]
    los_alta_media, los_alta_mediana, los_alta_p90 = _stats(los_alta_list)

    obitos = sum(1 for s in saidas if (s.get("desfecho") or "").lower() == "obito")
    mortalidade_taxa = _ratio(obitos, quantidade_saidas, 4)

    # Readmissão 48h (entre altas do período e próxima internação no MESMO setor)
    readm_result = list(db[COL_INT].aggregate([
        {"$match": {"alta_ts": {"$gte": inicio_inclusivo, "$lt": fim_exclusivo}, "desfecho": "alta"}},
        {"$lookup": {
            "from": COL_EST,
            "let": {"internacao_id": "$id_internacao"},
            "pipeline": [
                {"$match": {"$expr": {"$and": [
                    {"$eq": ["$id_internacao", "$$internacao_id"]},
                    {"$eq": ["$id_setor", id_setor]}
                ]}}}
            ],
            "as": "passou_setor"
        }},
        {"$match": {"passou_setor": {"$ne": []}}},
        {"$lookup": {
            "from": COL_INT,
            "let": {"idpac": "$id_paciente", "alta": "$alta_ts"},
            "pipeline": [
                {"$match": {"$expr": {"$and": [
                    {"$eq": ["$id_paciente", "$$idpac"]},
                    {"$gt": ["$admissao_ts", "$$alta"]}
                ]}}},
                {"$sort": {"admissao_ts": 1}},
                {"$limit": 1}
            ],
            "as": "prox_int"
        }},
        {"$unwind": {"path": "$prox_int", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": COL_EST,
            "let": {"next_id": "$prox_int.id_internacao"},
            "pipeline": [
                {"$match": {"$expr": {"$and": [
                    {"$eq": ["$id_internacao", "$$next_id"]},
                    {"$eq": ["$id_setor", id_setor]}
                ]}}}
            ],
            "as": "prox_passou_setor"
        }},
        {"$addFields": {
            "horas": {
                "$cond": [
                    {"$and": [
                        {"$ne": ["$prox_int", None]},
                        {"$gt": [{"$size": "$prox_passou_setor"}, 0]}
                    ]},
                    {"$divide": [{"$subtract": ["$prox_int.admissao_ts", "$alta_ts"]}, 1000 * 3600]},
                    None
                ]
            }
        }},
        {"$group": {
            "_id": None,
            "altas_utilizadas": {"$sum": 1},
            "readmissoes": {"$sum": {"$cond": [
                {"$and": [{"$ne": ["$horas", None]}, {"$lte": ["$horas", 48]}]},
                1, 0
            ]}}
        }}
    ]))
    if readm_result:
        altas_utilizadas = int(readm_result[0].get("altas_utilizadas", 0) or 0)
        readmissoes = int(readm_result[0].get("readmissoes", 0) or 0)
    else:
        altas_utilizadas = 0
        readmissoes = 0
    readmissao_taxa = _ratio(readmissoes, altas_utilizadas, 4)

    # Destino da alta (das altas do período)
    destino_counts: Dict[str, int] = {}
    for s in saidas:
        dest = _normaliza_destino(s)
        destino_counts[dest] = destino_counts.get(dest, 0) + 1
    total_dest = sum(destino_counts.values()) or 1
    destino_alta = [
        {"destino": dest, "quantidade": qtd, "percentual": _ratio(qtd, total_dest, 4)}
        for dest, qtd in destino_counts.items()
    ]

    # ----------------------
    # 2) Ventilação: tempo ventilado NO PERÍODO (apenas coorte)
    # ----------------------
    vent_tot_d: List[float] = []
    qtd_ventilados = 0
    qtd_intubados = 0
    reint = 0
    total_ext = 0
    tt_int_h: List[float] = []

    if coorte_ids:
        vents = list(db[COL_VENT].find(
            {"id_internacao": {"$in": list(coorte_ids)}},
            {"_id": 0, "id_internacao": 1, "intubacoes_ts": 1, "extubacoes_ts": 1, "periodos": 1}
        ))
        # (opcional) admissão para tempo até 1ª intubação
        it_map = {x["id_internacao"]: x for x in db[COL_INT].find(
            {"id_internacao": {"$in": list(coorte_ids)}},
            {"_id": 0, "id_internacao": 1, "admissao_ts": 1}
        )}

        for v in vents:
            intu = sorted([_coerce_dt(x) for x in (v.get("intubacoes_ts") or []) if _coerce_dt(x)])
            ext  = sorted([_coerce_dt(x) for x in (v.get("extubacoes_ts") or []) if _coerce_dt(x)])
            per  = v.get("periodos", []) or []

            # tempo até 1ª intubação (global, a partir da admissão)
            if intu:
                adm = _coerce_dt((it_map.get(v["id_internacao"]) or {}).get("admissao_ts"))
                if adm:
                    qtd_intubados += 1
                    tt_int_h.append(diferenca_horas(adm, intu[0]))

            # tempo total ventilado no período (sobreposição)
            total_secs = 0.0
            for p in per:
                ini_p = _coerce_dt(p.get("inicio"))
                fim_p = _coerce_dt(p.get("fim")) if p.get("fim") else now_utc
                total_secs += _overlap_seconds(ini_p, fim_p, inicio_inclusivo, fim_exclusivo)
            if total_secs > 0:
                qtd_ventilados += 1
                vent_tot_d.append(total_secs / 86400.0)

            # reintubação 48h (global)
            total_ext += len(ext)
            for e in ext:
                prox = next((i for i in intu if i and e and i > e), None)
                if prox and diferenca_horas(e, prox) <= 48.0:
                    reint += 1

    mean_h, med_h, p90_h = _stats(tt_int_h)
    mean_v, med_v, p90_v = _stats(vent_tot_d)
    reint_taxa = _ratio(reint, total_ext, 4)

    # ----------------------
    # 3) Paciente-dia & dispositivos (já por data no período)
    # ----------------------
    pd_result = list(db[COL_PD].aggregate([
        {"$match": {"id_setor": id_setor, "data": {"$gte": inicio_inclusivo, "$lt": fim_exclusivo}}},
        {"$group": {
            "_id": None,
            "patient_days": {"$sum": 1},
            "cvc_days": {"$sum": {"$cond": [{"$eq": ["$cvc", True]}, 1, 0]}},
            "foley_days": {"$sum": {"$cond": [{"$eq": ["$foley", True]}, 1, 0]}},
            "art_days": {"$sum": {"$cond": [{"$eq": ["$art_line", True]}, 1, 0]}},
            "all_ids": {"$addToSet": "$id_internacao"},
            "vent_ids_raw": {"$addToSet": {"$cond": [{"$eq": ["$ventilado", True]}, "$id_internacao", None]}},
            "cvc_ids_raw": {"$addToSet": {"$cond": [{"$eq": ["$cvc", True]}, "$id_internacao", None]}},
            "foley_ids_raw": {"$addToSet": {"$cond": [{"$eq": ["$foley", True]}, "$id_internacao", None]}},
            "art_ids_raw": {"$addToSet": {"$cond": [{"$eq": ["$art_line", True]}, "$id_internacao", None]}}
        }},
        {"$project": {
            "_id": 0,
            "patient_days": 1,
            "cvc_days": 1,
            "foley_days": 1,
            "art_days": 1,
            "all_ids": 1,
            "vent_ids": {"$setDifference": ["$vent_ids_raw", [None]]},
            "cvc_ids": {"$setDifference": ["$cvc_ids_raw", [None]]},
            "foley_ids": {"$setDifference": ["$foley_ids_raw", [None]]},
            "art_ids": {"$setDifference": ["$art_ids_raw", [None]]}
        }}
    ]))

    if pd_result:
        pd = pd_result[0]
        patient_days = int(pd.get("patient_days", 0) or 0)
        cvc_days = int(pd.get("cvc_days", 0) or 0)
        foley_days = int(pd.get("foley_days", 0) or 0)
        art_days = int(pd.get("art_days", 0) or 0)

        total_pac_list = pd.get("all_ids", []) or []
        vent_ids = pd.get("vent_ids", []) or []
        cvc_ids = pd.get("cvc_ids", []) or []
        foley_ids = pd.get("foley_ids", []) or []
        art_ids = pd.get("art_ids", []) or []

        total_pac = len(total_pac_list)
        prop_vent = _ratio(len(vent_ids), total_pac, 4)
        pc_cvc = _ratio(len(cvc_ids), total_pac, 4)
        pc_foley = _ratio(len(foley_ids), total_pac, 4)
        pc_art = _ratio(len(art_ids), total_pac, 4)
    else:
        patient_days = cvc_days = foley_days = art_days = 0
        total_pac_list = vent_ids = cvc_ids = foley_ids = art_ids = []
        total_pac = 0
        prop_vent = pc_cvc = pc_foley = pc_art = 0.0

    # ----------------------
    # 4) Antibióticos (DOT no período, apenas coorte)
    # ----------------------
    dot_por_antibiotico: List[Dict[str, Any]] = []
    ranking: List[Dict[str, Any]] = []

    if coorte_ids:
        ab_usos = list(db[COL_AB_USO].find(
            {"id_internacao": {"$in": list(coorte_ids)}},
            {"_id": 1, "id_internacao": 1, "antibiotico": 1}
        ))

        by_ab_id = {str(x["_id"]): x for x in ab_usos}
        ab_ids = list(by_ab_id.keys())

        if ab_ids:
            per_cur = db[COL_AB_PER].find(
                {"id_ab_uso": {"$in": [by_ab_id[k]["_id"] for k in ab_ids]}},
                {"_id": 0, "id_ab_uso": 1, "inicio": 1, "fim": 1}
            )
            # acumula DOT no período e pacientes expostos por antibiótico
            dot_map: Dict[str, float] = {}
            exp_map: Dict[str, Set[str]] = {}

            for p in per_cur:
                ab_id = str(p["id_ab_uso"])
                base = by_ab_id.get(ab_id)
                if not base:
                    continue
                ab_name = base.get("antibiotico") or "desconhecido"
                iid = base.get("id_internacao")

                secs = _overlap_seconds(p.get("inicio"), p.get("fim"), inicio_inclusivo, fim_exclusivo)
                if secs <= 0:
                    continue
                dias = secs / 86400.0

                dot_map[ab_name] = dot_map.get(ab_name, 0.0) + dias
                if ab_name not in exp_map:
                    exp_map[ab_name] = set()
                if iid:
                    exp_map[ab_name].add(iid)

            # monta listas ordenadas
            items = [{"antibiotico": k, "dot_total_d": round(v, 2), "pacientes_expostos": len(exp_map.get(k, set()))}
                     for k, v in dot_map.items()]
            items.sort(key=lambda x: x["dot_total_d"], reverse=True)
            ranking = items
            dot_por_antibiotico = [{"antibiotico": x["antibiotico"], "dot_total_d": x["dot_total_d"]} for x in items]

    # ----------------------
    # 5) Documento final
    # ----------------------
    return {
        "periodo": {"inicio": data_iso_utc(inicio_inclusivo), "fim": data_iso_utc(fim_exclusivo)},
        "id_setor": id_setor,

        "coorte": {
            "criterio": "presenca_no_setor",
            "pacientes_no_periodo": pacientes_no_periodo
        },

        "geral": {
            # LOS com base na PRESENÇA no período (por internação)
            "los_periodo_setor": {
                "media_d": los_media,
                "mediana_d": los_mediana,
                "p90_d": los_p90,
                "pacientes_no_periodo": pacientes_no_periodo
            },
            # LOS por alta (clássico) também incluso para referência
            "los_por_alta_no_periodo": {
                "media_d": los_alta_media,
                "mediana_d": los_alta_mediana,
                "p90_d": los_alta_p90,
                "quantidade_saidas": quantidade_saidas
            },
            "mortalidade": {
                "obitos": obitos,
                "saidas": quantidade_saidas,
                "taxa": mortalidade_taxa
            },
            "readmissao_48h": {
                "readmissoes": readmissoes,
                "altas": altas_utilizadas,
                "taxa": readmissao_taxa
            },
            "reintubacao_48h": {
                "reintubacoes": reint,
                "extubacoes": total_ext,
                "taxa": _ratio(reint, total_ext, 4)
            },
            "destino_alta": destino_alta
        },

        "antibioticos": {
            "ranking": ranking,
            "dot_por_antibiotico": dot_por_antibiotico
        },

        "dispositivos": {
            "tempo_ate_intubacao_h": {
                "media_h": mean_h,
                "mediana_h": med_h,
                "p90_h": p90_h,
                "quantidade_intubados": qtd_intubados
            },
            "tempo_ventilacao_no_periodo": {
                "media_d": mean_v,
                "mediana_d": med_v,
                "p90_d": p90_v,
                "quantidade_ventilados": qtd_ventilados
            },
            "proporcao_pacientes_ventilados": {
                "pacientes_ventilados": len(vent_ids) if pd_result else 0,
                "total_pacientes": total_pac if pd_result else 0,
                "percentual": prop_vent if pd_result else 0.0
            },
            "utilizacao_cvc": {
                "cvc_days": cvc_days,
                "patient_days": patient_days,
                "percentual": _ratio(cvc_days, patient_days, 4)
            },
            "pacientes_com_cvc": {
                "pacientes": len(cvc_ids) if pd_result else 0,
                "total_pacientes": total_pac if pd_result else 0,
                "percentual": pc_cvc if pd_result else 0.0
            },
            "utilizacao_foley": {
                "foley_days": foley_days,
                "patient_days": patient_days,
                "percentual": _ratio(foley_days, patient_days, 4)
            },
            "pacientes_com_foley": {
                "pacientes": len(foley_ids) if pd_result else 0,
                "total_pacientes": total_pac if pd_result else 0,
                "percentual": pc_foley if pd_result else 0.0
            },
            "utilizacao_art_line": {
                "art_line_days": art_days,
                "patient_days": patient_days,
                "percentual": _ratio(art_days, patient_days, 4)
            },
            "pacientes_com_art_line": {
                "pacientes": len(art_ids) if pd_result else 0,
                "total_pacientes": total_pac if pd_result else 0,
                "percentual": pc_art if pd_result else 0.0
            }
        }
    }
