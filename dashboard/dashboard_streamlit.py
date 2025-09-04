import os
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from datetime import date, datetime

# ---------------------------
# Config
# ---------------------------
API_BASE = os.getenv("API_BASE", "http://127.0.0.1:8000")

st.set_page_config(page_title="KPIs CTI", page_icon="🩺", layout="wide")
st.title("KPIs - Dashboard")

# ---------------------------
# Helpers
# ---------------------------

def dias_para_dias_horas(valor_dias: float) -> str:
    dias = int(valor_dias)  # parte inteira
    horas = int(round((valor_dias - dias) * 24))  # parte decimal convertida em horas
    return f"{dias} dias e {horas} horas"

def get_kpi_gestao(setor: str, inicio: str, fim: str) -> dict:
    url = f"{API_BASE}/kpi/gestao"
    params = {"setor": setor, "inicio": inicio, "fim": fim}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def format_date(dt_str):
    if not dt_str:
        return "-"
    try:
        # converte string ISO em datetime e formata no padrão brasileiro
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt.strftime("%d/%m/%Y %H:%M")
    except Exception:
        return dt_str

def get_kpi_paciente(id_internacao: str) -> dict:
    url = f"{API_BASE}/kpi/paciente/{id_internacao}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()

def kpi_box(col, title, lines):
    with col:
        st.markdown(f"#### {title}")
        for label, value in lines:
            st.markdown(f"**{label}**: {value}")

def donut_percent(title: str, value: float, remainder_label="Outros"):
    # value entre 0 e 1
    value = 0 if value is None else float(value)
    df = pd.DataFrame({
        "nome": [title, remainder_label],
        "valor": [value, max(0.0, 1.0 - value)]
    })
    fig = px.pie(df, names="nome", values="valor", hole=0.6)
    fig.update_traces(textinfo="percent+label")
    return fig

def safe_get(d: dict, path: list, default=None):
    cur = d
    for p in path:
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return default
    return cur

def plot_bar(df: pd.DataFrame, x: str, y: str, title: str, orientation="v"):
    if df.empty:
        return None
    if orientation == "h":
        fig = px.bar(df, x=y, y=x, title=title, orientation="h")
    else:
        fig = px.bar(df, x=x, y=y, title=title)
    return fig

def plot_timeline_periods(periods: list, start_key="inicio", end_key="fim", name_key="label", title="Linha do tempo"):
    if not periods:
        return None
    rows = []
    for p in periods:
        ini = p.get(start_key)
        fim = p.get(end_key) or ini  # se fim None, usa ini só pra render
        label = p.get(name_key, "período")
        rows.append({"Item": label, "Início": ini, "Fim": fim})
    df = pd.DataFrame(rows)
    if df.empty:
        return None
    fig = px.timeline(df, x_start="Início", x_end="Fim", y="Item", title=title)
    fig.update_yaxes(autorange="reversed")
    return fig

def melt_series_dict(series: dict, title: str):
    """
    Converte dict de séries em dataframe longo para plot.
    Aceita formatos:
      - {"creatinina": [{"data": "...", "valor": ...}, ...]}
      - {"hemograma": {"hb":[...], "leucocitos":[...]}}
    """
    rows = []
    for k, v in series.items():
        if isinstance(v, list):
            for p in v:
                rows.append({"data": p.get("data"), "exame": k, "valor": p.get("valor")})
        elif isinstance(v, dict):
            for subk, arr in v.items():
                for p in arr:
                    rows.append({"data": p.get("data"), "exame": f"{k}:{subk}", "valor": p.get("valor")})
    df = pd.DataFrame(rows)
    if not df.empty and "data" in df.columns:
        try:
            df["data"] = pd.to_datetime(df["data"])
        except Exception:
            pass
    if df.empty:
        return None
    fig = px.line(df, x="data", y="valor", color="exame", markers=True, title=title)
    return fig

# ---------------------------
# Layout
# ---------------------------
tabs = st.tabs(["KPI Gestão", "KPI Paciente"])

# Filtros comuns (sidebar)
with st.sidebar:
    st.markdown("### Filtros Gerais")
    setor = st.text_input("Setor", value="CTI-ADULTO")
    inicio = st.date_input("Início", value=date(2025, 8, 1), format="YYYY-MM-DD")
    fim = st.date_input("Fim", value=date(2025, 9, 1), format="YYYY-MM-DD")
    inicio_str, fim_str = inicio.strftime("%Y-%m-%d"), fim.strftime("%Y-%m-%d")

# ---------------------------
# Aba KPI Gestão (Tela 1)
# ---------------------------
with tabs[0]:
    st.subheader("KPI Gestão (Setor CTI)")
    c1, c2 = st.columns(2)
    with c1:
        st.text(f"Setor:  {setor}")
        st.text(f"Início: {inicio.strftime('%d/%m/%Y')}")
        st.text(f"Fim:    {fim.strftime('%d/%m/%Y')}")
    run_gestao = st.button("Atualizar KPI Gestão", type="primary")

    if run_gestao:
        try:
            kpi = get_kpi_gestao(setor, inicio_str, fim_str)

            # ============ MÉTRICAS GERAIS ============
            c1, c2, c3, c4 = st.columns(4)
            los = safe_get(kpi, ["geral", "los"], {})
            mort = safe_get(kpi, ["geral", "mortalidade"], {})
            readm = safe_get(kpi, ["geral", "readmissao_48h"], {})
            reint = safe_get(kpi, ["geral", "reintubacao_48h"], {})

            kpi_box(c1, "LOS (dias)", [
                ("Média", los.get("media_d", 0)),
                ("Mediana", los.get("mediana_d", 0)),
                ("P90", los.get("p90_d", 0)),
                ("Saídas", los.get("quantidade_saidas", 0)),
            ])
            kpi_box(c2, "Mortalidade", [
                ("Óbitos", mort.get("obitos", 0)),
                ("Saídas", mort.get("saidas", 0)),
                ("Taxa", f"{mort.get('taxa', 0) * 100:.1f}%"),

            ])
            kpi_box(c3, "Readmissão 48h", [
                ("Readmissões", readm.get("readmissoes", 0)),
                ("Altas", readm.get("altas", 0)),
                ("Taxa", f"{readm.get('taxa', 0) * 100:.1f}%"),
            ])
            kpi_box(c4, "Reintubação 48h", [
                ("Reintubações", reint.get("reintubacoes", 0)),
                ("Extubações", reint.get("extubacoes", 0)),
                ("Taxa", f"{reint.get('taxa', 0) * 100:.1f}%"),
            ])

            st.markdown("---")

            # ============ DESTINO DA ALTA ============
            destino = safe_get(kpi, ["geral", "destino_alta"], []) or []
            c1, c2 = st.columns([2, 1])
            with c1:
                if destino:
                    df_dest = pd.DataFrame(destino)
                    fig_pie = px.pie(df_dest, names="destino", values="quantidade", title="Destino da alta")
                    st.plotly_chart(fig_pie, use_container_width=True)
            with c2:
                if destino:
                    st.dataframe(pd.DataFrame(destino))

            st.markdown("---")

            # ============ ANTIBIÓTICOS ============
            ab_rank = safe_get(kpi, ["antibioticos", "ranking"], []) or []
            ab_dot = safe_get(kpi, ["antibioticos", "dot_por_antibiotico"], []) or []

            c1, c2 = st.columns(2)
            with c1:
                if ab_rank:
                    df_ab = pd.DataFrame(ab_rank)
                    fig_ab = plot_bar(df_ab, x="antibiotico", y="dot_total_d", title="DOT total por antibiótico")
                    if fig_ab: st.plotly_chart(fig_ab, use_container_width=True)
            with c2:
                if ab_rank:
                    st.dataframe(pd.DataFrame(ab_rank))

            st.markdown("---")

            # ============ DISPOSITIVOS ============
            disp = safe_get(kpi, ["dispositivos"], {}) or {}
            # 1) Tempo até intubação / tempo de ventilação
            tti = safe_get(disp, ["tempo_ate_intubacao_h"], {}) or {}
            tvm = safe_get(disp, ["tempo_ventilacao"], {}) or {}
            c1, c2 = st.columns(2)
            kpi_box(c1, "Tempo até primeira intubação (h)", [
                ("Média", tti.get("media_h", 0)),
                ("Mediana", tti.get("mediana_h", 0)),
                ("P90", tti.get("p90_h", 0)),
                ("Intubados", tti.get("quantidade_intubados", 0)),
            ])
            kpi_box(c2, "Tempo de ventilação (dias)", [
                ("Média", tvm.get("media_d", 0)),
                ("Mediana", tvm.get("mediana_d", 0)),
                ("P90", tvm.get("p90_d", 0)),
                ("Ventilados", tvm.get("quantidade_ventilados", 0)),
            ])

            st.markdown("")

            # 2) Proporção de pacientes ventilados
            prop = safe_get(disp, ["proporcao_pacientes_ventilados"], {}) or {}
            pv = prop.get("percentual", 0)
            c1, c2, c3 = st.columns([1.3, 1.2, 1])
            with c1:
                st.markdown("#### % Pacientes ventilados")
                fig_donut = donut_percent("Ventilados", pv, "Não ventilados")
                st.plotly_chart(fig_donut, use_container_width=True)
            with c2:
                kpi_box(st, "Pacientes (ventilação)", [
                    ("Ventilados", prop.get("pacientes_ventilados", 0)),
                    ("Total", prop.get("total_pacientes", 0)),
                    ("Percentual", pv),
                ])
            with c3:
                st.markdown("#### Período")
                kpi_box(st, "", [
                    ("Início", format_date(safe_get(kpi, ["periodo", "inicio"], ""))),
                    ("Fim", format_date(safe_get(kpi, ["periodo", "fim"], ""))),
                ])

            st.markdown("---")

            # 3) Utilização por device (CVC/Foley/Art-line)
            util_rows = []
            for key, label, days_key in [
                ("utilizacao_cvc", "CVC-days / patient-days", "cvc_days"),
                ("utilizacao_foley", "Foley-days / patient-days", "foley_days"),
                ("utilizacao_art_line", "Art-line-days / patient-days", "art_line_days"),
            ]:
                u = safe_get(disp, [key], {}) or {}
                util_rows.append({
                    "indicador": label,
                    "utilizacao": u.get("percentual", 0.0),
                    "device_days": u.get(days_key, 0),
                    "patient_days": u.get("patient_days", 0),
                })
            df_util = pd.DataFrame(util_rows)
            if not df_util.empty:
                # barras horizontais com percentual
                fig_util = plot_bar(df_util, x="indicador", y="utilizacao",
                                    title="Utilização de dispositivos (device-days / patient-days)", orientation="h")
                if fig_util:
                    fig_util.update_layout(xaxis_tickformat=".0%")
                    st.plotly_chart(fig_util, use_container_width=True)
                st.dataframe(df_util)

            st.markdown("")

            # 4) % de pacientes com device
            pc_rows = []
            for key, label in [
                ("pacientes_com_cvc", "% pacientes com CVC"),
                ("pacientes_com_foley", "% pacientes com Foley"),
                ("pacientes_com_art_line", "% pacientes com art-line"),
            ]:
                p = safe_get(disp, [key], {}) or {}
                pc_rows.append({
                    "indicador": label,
                    "percentual": p.get("percentual", 0.0),
                    "pacientes": p.get("pacientes", 0),
                    "total_pacientes": p.get("total_pacientes", 0)
                })
            df_pc = pd.DataFrame(pc_rows)
            if not df_pc.empty:
                fig_pc = plot_bar(df_pc, x="indicador", y="percentual",
                                  title="% de pacientes por dispositivo", orientation="v")
                if fig_pc:
                    fig_pc.update_layout(yaxis_tickformat=".0%")
                    st.plotly_chart(fig_pc, use_container_width=True)
                st.dataframe(df_pc)

        except requests.HTTPError as e:
            st.error(f"Erro HTTP: {e} – {getattr(e.response, 'text', '')}")
        except Exception as e:
            st.error(f"Falha ao carregar KPI Gestão: {e}")

# ---------------------------
# Aba KPI Paciente (Tela 2)
# ---------------------------
with tabs[1]:
    st.subheader("KPI Paciente (visão individual)")
    id_internacao = st.text_input("ID da internação", value="E-000123")
    run_paciente = st.button("Atualizar KPI Paciente")

    if run_paciente and id_internacao.strip():
        try:
            data = get_kpi_paciente(id_internacao.strip())

            # ======= Cards principais =======
            c1, c2, c3 = st.columns(3)
            kpi_box(c1, "Internação", [
                ("ID", data.get("id_internacao")),
                ("Status", data.get("status")),
                ("Admissão", format_date(data.get("admissao_ts"))),
                ("Alta", format_date(data.get("alta_ts"))),
            ])
            kpi_box(c2, "Tempo total", [
                ("Internação", dias_para_dias_horas(data.get("tempo_total_internacao_d", 0))),
            ])

            vent = data.get("ventilacao", {}) or {}
            kpi_box(c3, "Ventilação", [
                ("Tempo total (dias)", dias_para_dias_horas(vent.get("tempo_total_d", 0))),
                ("T até 1ª intubação (h)", vent.get("tempo_ate_primeira_intubacao_h")),
                ("Reintubação ≤ 48h", "Sim" if vent.get("reintubacao_48h_flag") else "Não"),
            ])

            st.markdown("---")

            # ======= Linha do tempo de ventilação =======
            periods = [
                {"label": "Ventilação", "inicio": p.get("inicio"), "fim": p.get("fim")}
                for p in vent.get("periodos", [])
            ]
            fig_vent = plot_timeline_periods(periods, title="Períodos de ventilação")
            if fig_vent:
                st.plotly_chart(fig_vent, use_container_width=True)

            # ======= Antibióticos do paciente =======
            ab = data.get("antibioticos", {}) or {}
            dot = ab.get("dot_por_antibiotico", [])
            linhas = ab.get("linhas_do_tempo", [])

            c1, c2 = st.columns(2)
            with c1:
                if dot:
                    df_dot = pd.DataFrame(dot)
                    fig_dot = plot_bar(df_dot, x="antibiotico", y="dot_dias", title="DOT por antibiótico (paciente)")
                    if fig_dot:
                        st.plotly_chart(fig_dot, use_container_width=True)
            with c2:
                if linhas:
                    # transformar em timeline
                    rows = []
                    for item in linhas:
                        ab_name = item.get("antibiotico")
                        for pr in item.get("periodos", []):
                            rows.append({"label": ab_name, "inicio": pr.get("inicio"), "fim": pr.get("fim")})
                    fig_ab_tl = plot_timeline_periods(rows, title="Linhas do tempo de antibióticos")
                    if fig_ab_tl:
                        st.plotly_chart(fig_ab_tl, use_container_width=True)

            st.markdown("---")

            # ======= Labs =======
            labs = data.get("labs", {}) or {}
            ultimos = labs.get("ultimos", {})
            series = labs.get("series", {})

            c1, c2 = st.columns([1.2, 1])
            with c1:
                fig_series = melt_series_dict(series, "Tendência de exames laboratoriais")
                if fig_series:
                    st.plotly_chart(fig_series, use_container_width=True)
            with c2:
                if ultimos:
                    # tabela “últimos” com flags
                    rows = []
                    for ex, d in ultimos.items():
                        rows.append({
                            "exame": ex,
                            "valor": d.get("valor"),
                            "unidade": d.get("unidade", ""),
                            "flag": d.get("flag", ""),
                            "data": format_date(d.get("data"))
                        })
                    st.markdown("#### Últimos valores (com flags)")
                    st.dataframe(pd.DataFrame(rows).sort_values("exame"))

            # ======= Dispositivos do paciente =======
            st.markdown("---")
            disp = data.get("dispositivos", {}) or {}
            rows = []
            for k, arr in disp.items():
                for p in arr:
                    rows.append({"dispositivo": k, "inicio": p.get("inicio"), "fim": p.get("fim"), "fonte_fim": p.get("fonte_fim", "")})
            df_disp = pd.DataFrame(rows)
            if not df_disp.empty:
                st.markdown("#### Dispositivos (períodos)")
                st.dataframe(df_disp)

        except requests.HTTPError as e:
            st.error(f"Erro HTTP: {e} – {getattr(e.response, 'text', '')}")
        except Exception as e:
            st.error(f"Falha ao carregar KPI Paciente: {e}")
