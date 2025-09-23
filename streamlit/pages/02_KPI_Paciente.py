# -*- coding: utf-8 -*-
import sys
from pathlib import Path
import pandas as pd
import streamlit as st
from requests.exceptions import HTTPError

# ============================================================
# Shim: garante imports absolutos a partir de 'streamlit/'
# ============================================================
_here = Path(__file__).resolve()
_streamlit_dir = _here.parent.parent  # .../streamlit
if str(_streamlit_dir) not in sys.path:
    sys.path.append(str(_streamlit_dir))

# ===== IMPORTS da sua árvore =====
from servicos.api import (
    obter_kpi_paciente,
    carregar_setores,
    pacientes_internados_no_setor,
)
from componentes.caixas import caixa_kpi
from componentes.graficos import grafico_barras, grafico_linha_do_tempo, grafico_series
from util.formatacao import dias_para_dias_horas, formatar_data_iso_br

st.subheader("KPI Paciente (visão individual)")

# ------------------------------------------------
# 1) Seletor de SETOR (igual ao KPI Gestão)
# ------------------------------------------------
try:
    itens = carregar_setores()  # [{id_setor, nome}] OU ["CTI-ADULTO", ...]
except Exception as e:
    st.error(f"Falha ao carregar setores: {e}")
    st.stop()

if not itens:
    st.warning("Nenhum setor encontrado no endpoint /kpi/setores.")
    st.stop()

if isinstance(itens[0], dict):
    labels = [(it.get("nome") or it["id_setor"]) for it in itens]
    mapa = {(it.get("nome") or it["id_setor"]): it["id_setor"] for it in itens}
else:
    labels = [str(x) for x in itens]
    mapa = {lbl: lbl for lbl in labels}

rotulo_escolhido = st.selectbox("Setor", labels, key="pac_setor")
setor = mapa[rotulo_escolhido]

st.markdown("")

# ------------------------------------------------
# 2) Seletor de PACIENTES internados no setor
#    (estada aberta + sem alta) — sem campo manual
# ------------------------------------------------
pacientes = []
try:
    pacientes = pacientes_internados_no_setor(setor)
except HTTPError as e:
    st.error(f"Erro ao listar pacientes internados ({e.response.status_code}): {getattr(e.response, 'text', '')}")
    st.stop()
except Exception as e:
    st.error(f"Falha ao listar pacientes internados: {e}")
    st.stop()

if not pacientes:
    st.warning("Nenhum paciente internado encontrado neste setor neste momento.")
    st.stop()

# Monta rótulo amigável e mapa -> id_internacao
opcoes = []
mapa_opc = {}
for p in pacientes:
    nome = p.get("nome") or ""
    idp = p.get("id_paciente", "")
    idi = p.get("id_internacao", "")
    adm = p.get("admissao_ts")
    label = f"{(nome + ' · ') if nome else ''}{idp} · {idi}"
    if adm:
        label += f" · Adm: {formatar_data_iso_br(adm)}"
    opcoes.append(label)
    mapa_opc[label] = idi

escolha = st.selectbox("Paciente internado no setor", opcoes, index=0, key="paciente_internado")
id_internacao = mapa_opc[escolha]

st.markdown("---")

executar = st.button("Atualizar KPI Paciente", type="primary")

# ------------------------------------------------
# 3) Buscar KPI do paciente e exibir
# ------------------------------------------------
if executar:
    try:
        dados = obter_kpi_paciente(id_internacao)

        # Cabeçalho
        c1, c2, c3 = st.columns(3)
        caixa_kpi(c1, "Internação", [
            ("ID Internação", dados.get("id_internacao")),
            ("ID Paciente", dados.get("id_paciente")),
            ("Setor", dados.get("id_setor") or setor),
            ("Admissão", formatar_data_iso_br(dados.get("admissao_ts"))),
            ("Alta", formatar_data_iso_br(dados.get("alta_ts"))),
            ("Status", dados.get("status")),
        ])
        caixa_kpi(c2, "Tempo total", [
            ("Internação", dias_para_dias_horas(dados.get("tempo_total_internacao_d", 0))),
        ])

        vent = dados.get("ventilacao", {}) or {}
        caixa_kpi(c3, "Ventilação", [
            ("Tempo total (dias)", dias_para_dias_horas(vent.get("tempo_total_d", 0))),
            ("T até 1ª intubação (h)", vent.get("tempo_ate_primeira_intubacao_h")),
            ("Reintubação ≤ 48h", "Sim" if vent.get("reintubacao_48h_flag") else "Não"),
        ])

        st.markdown("---")

        # Linha do tempo de ventilação
        periodos = [{"label": "Ventilação", "inicio": p.get("inicio"), "fim": p.get("fim")} for p in vent.get("periodos", [])]
        fig_vent = grafico_linha_do_tempo(periodos, titulo="Períodos de ventilação")
        if fig_vent:
            st.plotly_chart(fig_vent, use_container_width=True)

        # Antibióticos
        ab = dados.get("antibioticos", {}) or {}
        dot = ab.get("dot_por_antibiotico", [])
        linhas = ab.get("linhas_do_tempo", [])

        c_ab1, c_ab2 = st.columns(2)
        with c_ab1:
            if dot:
                df_dot = pd.DataFrame(dot)
                fig_dot = grafico_barras(df_dot, eixo_x="antibiotico", eixo_y="dot_dias", titulo="DOT por antibiótico (paciente)")
                if fig_dot:
                    st.plotly_chart(fig_dot, use_container_width=True)
        with c_ab2:
            if linhas:
                linhas_tl = []
                for item in linhas:
                    nome = item.get("antibiotico")
                    for pr in item.get("periodos", []):
                        linhas_tl.append({"label": nome, "inicio": pr.get("inicio"), "fim": pr.get("fim")})
                fig_ab_tl = grafico_linha_do_tempo(linhas_tl, titulo="Linhas do tempo de antibióticos")
                if fig_ab_tl:
                    st.plotly_chart(fig_ab_tl, use_container_width=True)

        st.markdown("---")

        # Labs
        labs = dados.get("labs", {}) or {}
        ultimos = labs.get("ultimos", {})
        series = labs.get("series", {})

        c_l1, c_l2 = st.columns([1.2, 1])
        with c_l1:
            fig_series = grafico_series(series, "Tendência de exames laboratoriais")
            if fig_series:
                st.plotly_chart(fig_series, use_container_width=True)
        with c_l2:
            if ultimos:
                linhas_u = []
                for ex, d in ultimos.items():
                    linhas_u.append({
                        "exame": ex,
                        "valor": d.get("valor"),
                        "unidade": d.get("unidade", ""),
                        "flag": d.get("flag", ""),
                        "data": formatar_data_iso_br(d.get("data"))
                    })
                st.markdown("#### Últimos valores (com flags)")
                st.dataframe(pd.DataFrame(linhas_u).sort_values("exame"))

        # Dispositivos
        st.markdown("---")
        disp = dados.get("dispositivos", {}) or {}
        linhas_disp = []
        for nome_disp, arr in disp.items():
            for p in arr:
                linhas_disp.append({
                    "dispositivo": nome_disp,
                    "inicio": p.get("inicio"),
                    "fim": p.get("fim"),
                    "fonte_fim": p.get("fonte_fim", "")
                })
        df_disp = pd.DataFrame(linhas_disp)
        if not df_disp.empty:
            st.markdown("#### Dispositivos (períodos)")
            st.dataframe(df_disp)

    except HTTPError as e:
        st.error(f"Erro HTTP: {e} – {getattr(e.response, 'text', '')}")
    except Exception as e:
        st.error(f"Falha ao carregar KPI Paciente: {e}")
