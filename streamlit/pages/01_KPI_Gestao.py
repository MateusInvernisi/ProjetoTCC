import pandas as pd
import streamlit as st
from datetime import date

from servicos.api import obter_kpi_gestao
from componentes.caixas import caixa_kpi
from componentes.graficos import (
    grafico_barras,
    grafico_donut_percentual,
)
from util.dicionarios import pegar_seguro
from util.formatacao import formatar_data_iso_br

st.subheader("KPI Gestão (Setor CTI)")

# Filtros (sidebar)
with st.sidebar:
    st.markdown("### Filtros Gerais")
    setor = st.text_input("Setor", value="CTI-CENTRAL")
    inicio = st.date_input("Início", value=date(2025, 8, 1), format="DD-MM-YYYY")
    fim = st.date_input("Fim", value=date.today(), format="DD-MM-YYYY")
    inicio_str, fim_str = inicio.strftime("%Y-%m-%d"), fim.strftime("%Y-%m-%d")

col_a, col_b = st.columns(2)
with col_a:
    st.text(f"Setor:  {setor}")
    st.text(f"Início: {inicio.strftime('%d/%m/%Y')}")
    st.text(f"Fim:    {fim.strftime('%d/%m/%Y')}")

executar = st.button("Atualizar KPI Gestão", type="primary")

if executar:
    try:
        kpi = obter_kpi_gestao(setor, inicio_str, fim_str)

        # ============ MÉTRICAS GERAIS ============
        c1, c2, c3, c4 = st.columns(4)
        los = pegar_seguro(kpi, ["geral", "los"], {}) or {}
        mort = pegar_seguro(kpi, ["geral", "mortalidade"], {}) or {}
        readm = pegar_seguro(kpi, ["geral", "readmissao_48h"], {}) or {}
        reint = pegar_seguro(kpi, ["geral", "reintubacao_48h"], {}) or {}

        caixa_kpi(c1, "LOS (dias)", [
            ("Média", los.get("media_d", 0)),
            ("Mediana", los.get("mediana_d", 0)),
            ("P90", los.get("p90_d", 0)),
            ("Saídas", los.get("quantidade_saidas", 0)),
        ])
        caixa_kpi(c2, "Mortalidade", [
            ("Óbitos", mort.get("obitos", 0)),
            ("Saídas", mort.get("saidas", 0)),
            ("Taxa", f"{mort.get('taxa', 0) * 100:.1f}%"),
        ])
        caixa_kpi(c3, "Readmissão 48h", [
            ("Readmissões", readm.get("readmissoes", 0)),
            ("Altas", readm.get("altas", 0)),
            ("Taxa", f"{readm.get('taxa', 0) * 100:.1f}%"),
        ])
        caixa_kpi(c4, "Reintubação 48h", [
            ("Reintubações", reint.get("reintubacoes", 0)),
            ("Extubações", reint.get("extubacoes", 0)),
            ("Taxa", f"{reint.get('taxa', 0) * 100:.1f}%"),
        ])

        st.markdown("---")

        # ============ DESTINO DA ALTA ============
        destino = pegar_seguro(kpi, ["geral", "destino_alta"], []) or []
        col_g1, col_g2 = st.columns([2, 1])
        with col_g1:
            if destino:
                df_dest = pd.DataFrame(destino)
                fig_pie = grafico_barras(df_dest, eixo_x="destino", eixo_y="quantidade", titulo="Destino da alta")
                # Observação: se preferir pizza, use plotly.express.pie diretamente
                if fig_pie:
                    st.plotly_chart(fig_pie, use_container_width=True)
        with col_g2:
            if destino:
                st.dataframe(pd.DataFrame(destino))

        st.markdown("---")

        # ============ ANTIBIÓTICOS ============
        ab_rank = pegar_seguro(kpi, ["antibioticos", "ranking"], []) or []
        if ab_rank:
            df_ab = pd.DataFrame(ab_rank)
            fig_ab = grafico_barras(df_ab, eixo_x="antibiotico", eixo_y="dot_total_d", titulo="DOT total por antibiótico")
            if fig_ab:
                st.plotly_chart(fig_ab, use_container_width=True)
            st.dataframe(df_ab)

        st.markdown("---")

        # ============ DISPOSITIVOS ============
        disp = pegar_seguro(kpi, ["dispositivos"], {}) or {}

        # 1) Tempo até intubação / tempo de ventilação
        tti = pegar_seguro(disp, ["tempo_ate_intubacao_h"], {}) or {}
        tvm = pegar_seguro(disp, ["tempo_ventilacao"], {}) or {}

        d1, d2 = st.columns(2)
        caixa_kpi(d1, "Tempo até primeira intubação (h)", [
            ("Média", tti.get("media_h", 0)),
            ("Mediana", tti.get("mediana_h", 0)),
            ("P90", tti.get("p90_h", 0)),
            ("Intubados", tti.get("quantidade_intubados", 0)),
        ])
        caixa_kpi(d2, "Tempo de ventilação (dias)", [
            ("Média", tvm.get("media_d", 0)),
            ("Mediana", tvm.get("mediana_d", 0)),
            ("P90", tvm.get("p90_d", 0)),
            ("Ventilados", tvm.get("quantidade_ventilados", 0)),
        ])

        st.markdown("")

        # 2) Proporção de pacientes ventilados
        prop = pegar_seguro(disp, ["proporcao_pacientes_ventilados"], {}) or {}
        percentual_vent = float(prop.get("percentual", 0))
        g1, g2, g3 = st.columns([1.3, 1.2, 1])
        with g1:
            st.markdown("#### % Pacientes ventilados")
            fig_donut = grafico_donut_percentual("Ventilados", percentual_vent, "Não ventilados")
            st.plotly_chart(fig_donut, use_container_width=True)
        with g2:
            caixa_kpi(st, "Pacientes (ventilação)", [
                ("Ventilados", prop.get("pacientes_ventilados", 0)),
                ("Total", prop.get("total_pacientes", 0)),
                ("Percentual", f"{percentual_vent*100:.1f}%"),
            ])
        with g3:
            st.markdown("#### Período")
            caixa_kpi(st, "", [
                ("Início", formatar_data_iso_br(pegar_seguro(kpi, ["periodo", "inicio"], ""))),
                ("Fim", formatar_data_iso_br(pegar_seguro(kpi, ["periodo", "fim"], ""))),
            ])

        st.markdown("---")

        # 3) Utilização por device (CVC/Foley/Art-line)
        util_linhas = []
        for chave, rotulo, dias_key in [
            ("utilizacao_cvc", "CVC-days / patient-days", "cvc_days"),
            ("utilizacao_foley", "Foley-days / patient-days", "foley_days"),
            ("utilizacao_art_line", "Art-line-days / patient-days", "art_line_days"),
        ]:
            u = pegar_seguro(disp, [chave], {}) or {}
            util_linhas.append({
                "indicador": rotulo,
                "utilizacao": u.get("percentual", 0.0),
                "device_days": u.get(dias_key, 0),
                "patient_days": u.get("patient_days", 0),
            })
        df_util = pd.DataFrame(util_linhas)
        if not df_util.empty:
            fig_util = grafico_barras(
                df_util, eixo_x="indicador", eixo_y="utilizacao",
                titulo="Utilização de dispositivos (device-days / patient-days)", orientacao="h"
            )
            if fig_util:
                fig_util.update_layout(xaxis_tickformat=".0%")
                st.plotly_chart(fig_util, use_container_width=True)
            st.dataframe(df_util)

        st.markdown("")

        # 4) % de pacientes com device
        pc_linhas = []
        for chave, rotulo in [
            ("pacientes_com_cvc", "% pacientes com CVC"),
            ("pacientes_com_foley", "% pacientes com Foley"),
            ("pacientes_com_art_line", "% pacientes com art-line"),
        ]:
            p = pegar_seguro(disp, [chave], {}) or {}
            pc_linhas.append({
                "indicador": rotulo,
                "percentual": p.get("percentual", 0.0),
                "pacientes": p.get("pacientes", 0),
                "total_pacientes": p.get("total_pacientes", 0)
            })
        df_pc = pd.DataFrame(pc_linhas)
        if not df_pc.empty:
            fig_pc = grafico_barras(df_pc, eixo_x="indicador", eixo_y="percentual", titulo="% de pacientes por dispositivo")
            if fig_pc:
                fig_pc.update_layout(yaxis_tickformat=".0%")
                st.plotly_chart(fig_pc, use_container_width=True)
            st.dataframe(df_pc)

    except requests.HTTPError as e:  # type: ignore[name-defined]
        st.error(f"Erro HTTP: {e} – {getattr(e.response, 'text', '')}")
    except Exception as e:
        st.error(f"Falha ao carregar KPI Gestão: {e}")