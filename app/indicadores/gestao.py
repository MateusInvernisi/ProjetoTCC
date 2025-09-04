from datetime import datetime
from statistics import median
from typing import Dict, Any, List, Tuple
from app.db_connection import get_db
from app.utils.time import p90, diferenca_dias, diferenca_horas, data_iso_utc


#####################################################
# Função:      _stats
# Entrada:     arr (List[float]) - amostra numérica.
# Saída:       Tuple[float, float, float] - (média, mediana, p90),
#              arredondados com 2 casas; retorna (0,0,0) se vazio.
# Descrição:   Agrega estatísticas simples para listas de
#              durações/tempos em horas/dias.
#####################################################
def _stats(arr: List[float]) -> Tuple[float, float, float]:
    if not arr:
        return (0.0, 0.0, 0.0)
    return (
        round(sum(arr) / len(arr), 2),
        round(median(arr), 2),
        round(p90(arr), 2),
    )


#####################################################
# Função:      _ratio
# Descrição:   Evita divisão por zero. Retorna 0.0 se den == 0.
#####################################################
def _ratio(num: float, den: float, ndigits: int = 4) -> float:
    return round((num / den) if den else 0.0, ndigits)


#####################################################
# Função:      _normaliza_destino
# Entrada:     registro de saída de internação.
# Descrição:   Normaliza rótulos a partir de destino_alta ou
#              desfecho, com regras locais simples.
#####################################################
def _normaliza_destino(s: Dict[str, Any]) -> str:
    dest = (s.get("destino_alta") or "").lower().strip()
    if dest:
        return dest
    dest = s.get("desfecho", "alta").lower().strip()
    if dest == "alta":
        return "enfermaria"
    if dest == "obito":
        return "obito"
    if dest == "transferencia":
        return "outro_hospital"
    return dest or "enfermaria"


#####################################################
# Função:      build_kpi_cti_gestao
# Entrada:     id_setor (str) - ex.: "CTI-ADULTO"
#              inicio (datetime) - limite inferior (incl.)
#              fim (datetime)    - limite superior (excl.)
# Descrição:   Calcula KPIs de gestão do setor a partir de:
#              - Internações com ALTA no período
#              - Que passaram pelo setor informado
#####################################################
def build_kpi_cti_gestao(id_setor: str, inicio: datetime, fim: datetime) -> Dict[str, Any]:
    db = get_db()

    # ---------------------------------------------------------
    # 1) Saídas no período + passou pelo setor (pipeline otimizado)
    #    - $lookup com pipeline filtra o setor já no join
    #    - $match subsequente garante passagem no setor
    # ---------------------------------------------------------
    saidas = list(db.internacoes.aggregate([
        {"$match": {"alta_ts": {"$gte": inicio, "$lt": fim}}},
        {"$lookup": {
            "from": "estadas_setor",
            "let": {"internacao_id": "$id_internacao"},
            "pipeline": [
                {"$match": {
                    "$expr": {
                        "$and": [
                            {"$eq": ["$id_internacao", "$$internacao_id"]},
                            {"$eq": ["$id_setor", id_setor]}
                        ]
                    }
                }},
                {"$limit": 1}
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

    # -----------------------
    # LOS (Length of Stay)
    # -----------------------
    quantidade_saidas = len(saidas)
    los_list = [diferenca_dias(s["admissao_ts"], s["alta_ts"]) for s in saidas]
    los_media, los_mediana, los_p90 = _stats(los_list)

    # -----------------------
    # Mortalidade no período
    # -----------------------
    obitos = sum(1 for s in saidas if s.get("desfecho") == "obito")
    mortalidade_taxa = _ratio(obitos, quantidade_saidas, 4)

    # ---------------------------------------------------------
    # 2) Readmissão 48h (alta -> próxima admissão <= 48h) no MESMO setor
    #    - Pipeline único (sem N+1)
    #    - Considera só saídas por "alta" e que passaram no setor
    # ---------------------------------------------------------
    readm_result = list(db.internacoes.aggregate([
        {"$match": {"alta_ts": {"$gte": inicio, "$lt": fim}, "desfecho": "alta"}},
        {"$lookup": {
            "from": "estadas_setor",
            "let": {"internacao_id": "$id_internacao"},
            "pipeline": [
                {"$match": {
                    "$expr": {
                        "$and": [
                            {"$eq": ["$id_internacao", "$$internacao_id"]},
                            {"$eq": ["$id_setor", id_setor]}
                        ]
                    }
                }},
                {"$limit": 1}
            ],
            "as": "passou_setor"
        }},
        {"$match": {"passou_setor": {"$ne": []}}},
        {"$lookup": {
            "from": "internacoes",
            "let": {"idpac": "$id_paciente", "alta": "$alta_ts"},
            "pipeline": [
                {"$match": {
                    "$expr": {
                        "$and": [
                            {"$eq": ["$id_paciente", "$$idpac"]},
                            {"$gt": ["$admissao_ts", "$$alta"]}
                        ]
                    }
                }},
                {"$sort": {"admissao_ts": 1}},
                {"$limit": 1}
            ],
            "as": "prox_int"
        }},
        {"$unwind": {"path": "$prox_int", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "estadas_setor",
            "let": {"next_id": "$prox_int.id_internacao"},
            "pipeline": [
                {"$match": {
                    "$expr": {
                        "$and": [
                            {"$eq": ["$id_internacao", "$$next_id"]},
                            {"$eq": ["$id_setor", id_setor]}
                        ]
                    }
                }},
                {"$limit": 1}
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
                    {"$divide": [
                        {"$subtract": ["$prox_int.admissao_ts", "$alta_ts"]},
                        1000 * 3600  # ms -> horas
                    ]},
                    None
                ]
            }
        }},
        {"$group": {
            "_id": None,
            "altas_utilizadas": {"$sum": 1},
            "readmissoes": {
                "$sum": {
                    "$cond": [
                        {"$and": [{"$ne": ["$horas", None]}, {"$lte": ["$horas", 48]}]},
                        1,
                        0
                    ]
                }
            }
        }}
    ]))

    if readm_result:
        altas_utilizadas = int(readm_result[0]["altas_utilizadas"])
        readmissoes = int(readm_result[0]["readmissoes"])
    else:
        altas_utilizadas = 0
        readmissoes = 0
    readmissao_taxa = _ratio(readmissoes, altas_utilizadas, 4)

    # ---------------------------------------------------------
    # 3) Ventilação: tempo até 1ª intubação (h) e tempo total ventilação (d)
    #    - Consulta única por lista de id_internacao (evita N+1)
    #    - Reintubação 48h calculada em memória sobre a mesma consulta
    # ---------------------------------------------------------
    ids_int = [s["id_internacao"] for s in saidas]
    tt_int_h: List[float] = []
    vent_tot_d: List[float] = []
    qtd_intubados = 0
    qtd_ventilados = 0
    reint = 0
    total_ext = 0

    if ids_int:
        vents = list(db.ventilacao.find(
            {"id_internacao": {"$in": ids_int}},
            {"_id": 0, "id_internacao": 1, "intubacoes_ts": 1, "extubacoes_ts": 1, "periodos": 1}
        ))

        # Indexar admissões por id_internacao para cálculo de T até 1ª intubação
        adm_por_int = {s["id_internacao"]: s["admissao_ts"] for s in saidas}

        for v in vents:
            intu = sorted(v.get("intubacoes_ts", []))
            ext = sorted(v.get("extubacoes_ts", []))
            per = v.get("periodos", []) or []

            # T até 1ª intubação (horas)
            if intu:
                qtd_intubados += 1
                adm = adm_por_int.get(v["id_internacao"])
                if adm is not None:
                    tt_int_h.append(diferenca_horas(adm, intu[0]))

            # Tempo total ventilação (dias)
            total_secs = 0.0
            for p in per:
                if p.get("inicio") and p.get("fim"):
                    total_secs += (p["fim"] - p["inicio"]).total_seconds()
            if total_secs > 0:
                qtd_ventilados += 1
                vent_tot_d.append(total_secs / 86400.0)

            # Reintubação 48h
            total_ext += len(ext)
            for e in ext:
                prox = next((i for i in intu if i > e), None)
                if prox and diferenca_horas(e, prox) <= 48.0:
                    reint += 1

    mean_h, med_h, p90_h = _stats(tt_int_h)
    mean_v, med_v, p90_v = _stats(vent_tot_d)
    reint_taxa = _ratio(reint, total_ext, 4)

    # ---------------------------------------------------------
    # 4) Paciente-dia e utilizações (CVC, foley, art line, ventilados)
    #    - Agregação única com $group
    #    - Reduz vários distinct()/count_documents() a 1 pipeline
    # ---------------------------------------------------------
    pd_result = list(db.paciente_dia_setor.aggregate([
        {"$match": {"id_setor": id_setor, "data": {"$gte": inicio, "$lt": fim}}},
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
        patient_days = int(pd["patient_days"])
        cvc_days = int(pd["cvc_days"])
        foley_days = int(pd["foley_days"])
        art_days = int(pd["art_days"])

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

    # ---------------------------------------------------------
    # 5) Antibióticos (ranking por DOT e exposição de pacientes)
    #    - Mantido "ranking"
    #    - "dot_por_antibiotico" derivado do ranking (sem custo extra)
    # ---------------------------------------------------------
    ab_agg = list(db.antibioticos_uso.aggregate([
        {"$lookup": {
            "from": "antibiotico_periodos",
            "localField": "_id",
            "foreignField": "id_ab_uso",
            "as": "per"
        }},
        {"$match": {"per.fim": {"$gte": inicio}, "per.inicio": {"$lt": fim}}},
        {"$group": {
            "_id": "$antibiotico",
            "dot_total_d": {"$sum": "$dot_dias"},
            "pacientes_expostos": {"$addToSet": "$id_internacao"}
        }},
        {"$project": {
            "_id": 0,
            "antibiotico": "$_id",
            "dot_total_d": 1,
            "pacientes_expostos": {"$size": "$pacientes_expostos"}
        }},
        {"$sort": {"dot_total_d": -1}}
    ]))
    dot_por_antibiotico = [
        {"antibiotico": x["antibiotico"], "dot_total_d": x["dot_total_d"]}
        for x in ab_agg
    ]

    # ---------------------------------------------------------
    # 6) Destino da alta (normalização + percentuais)
    # ---------------------------------------------------------
    destino_counts: Dict[str, int] = {}
    for s in saidas:
        dest = _normaliza_destino(s)
        destino_counts[dest] = destino_counts.get(dest, 0) + 1

    total_dest = sum(destino_counts.values()) or 1
    destino_alta = [
        {
            "destino": dest,
            "quantidade": qtd,
            "percentual": _ratio(qtd, total_dest, 4)
        }
        for dest, qtd in destino_counts.items()
    ]

    # ---------------------------------------------------------
    # 7) Documento de saída
    # ---------------------------------------------------------
    return {
        "periodo": {"inicio": data_iso_utc(inicio), "fim": data_iso_utc(fim)},
        "id_setor": id_setor,
        "geral": {
            "los": {
                "media_d": los_media,
                "mediana_d": los_mediana,
                "p90_d": los_p90,
                "quantidade_saidas": quantidade_saidas
            },
            "mortalidade": {
                "obitos": obitos,
                "saidas": quantidade_saidas,
                "taxa": _ratio(obitos, quantidade_saidas, 4)
            },
            "readmissao_48h": {
                "readmissoes": readmissoes,
                "altas": altas_utilizadas,
                "taxa": readmissao_taxa
            },
            "reintubacao_48h": {
                "reintubacoes": reint,
                "extubacoes": total_ext,
                "taxa": reint_taxa
            },
            "destino_alta": destino_alta
        },
        "antibioticos": {
            "ranking": ab_agg,
            "dot_por_antibiotico": dot_por_antibiotico
        },
        "dispositivos": {
            "tempo_ate_intubacao_h": {
                "media_h": mean_h,
                "mediana_h": med_h,
                "p90_h": p90_h,
                "quantidade_intubados": qtd_intubados
            },
            "tempo_ventilacao": {
                "media_d": mean_v,
                "mediana_d": med_v,
                "p90_d": p90_v,
                "quantidade_ventilados": qtd_ventilados
            },
            "proporcao_pacientes_ventilados": {
                "pacientes_ventilados": len(vent_ids),
                "total_pacientes": total_pac,
                "percentual": prop_vent
            },
            "utilizacao_cvc": {
                "cvc_days": cvc_days,
                "patient_days": patient_days,
                "percentual": _ratio(cvc_days, patient_days, 4)
            },
            "pacientes_com_cvc": {
                "pacientes": len(cvc_ids),
                "total_pacientes": total_pac,
                "percentual": pc_cvc
            },
            "utilizacao_foley": {
                "foley_days": foley_days,
                "patient_days": patient_days,
                "percentual": _ratio(foley_days, patient_days, 4)
            },
            "pacientes_com_foley": {
                "pacientes": len(foley_ids),
                "total_pacientes": total_pac,
                "percentual": pc_foley
            },
            "utilizacao_art_line": {
                "art_line_days": art_days,
                "patient_days": patient_days,
                "percentual": _ratio(art_days, patient_days, 4)
            },
            "pacientes_com_art_line": {
                "pacientes": len(art_ids),
                "total_pacientes": total_pac,
                "percentual": pc_art
            }
        }
    }
