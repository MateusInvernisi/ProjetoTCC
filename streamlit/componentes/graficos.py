import pandas as pd
import plotly.express as px


def grafico_donut_percentual(titulo: str, valor: float, rotulo_resto: str = "Outros"):
    valor = 0.0 if valor is None else float(valor)
    df = pd.DataFrame({
        "nome": [titulo, rotulo_resto],
        "valor": [valor, max(0.0, 1.0 - valor)]
    })
    fig = px.pie(df, names="nome", values="valor", hole=0.6)
    fig.update_traces(textinfo="percent+label")
    return fig


def grafico_barras(df: pd.DataFrame, eixo_x: str, eixo_y: str, titulo: str, orientacao: str = "v"):
    if df is None or df.empty:
        return None
    if orientacao == "h":
        fig = px.bar(df, x=eixo_y, y=eixo_x, title=titulo, orientation="h")
    else:
        fig = px.bar(df, x=eixo_x, y=eixo_y, title=titulo)
    return fig


def grafico_linha_do_tempo(periodos: list, chave_inicio: str = "inicio", chave_fim: str = "fim", chave_nome: str = "label", titulo: str = "Linha do tempo"):
    if not periodos:
        return None
    linhas = []
    for p in periodos:
        ini = p.get(chave_inicio)
        fim = p.get(chave_fim) or ini
        nome = p.get(chave_nome, "período")
        linhas.append({"Item": nome, "Início": ini, "Fim": fim})
    df = pd.DataFrame(linhas)
    if df.empty:
        return None
    fig = px.timeline(df, x_start="Início", x_end="Fim", y="Item", title=titulo)
    fig.update_yaxes(autorange="reversed")
    return fig


def grafico_series(dados_series: dict, titulo: str):
    """
    Aceita formatos:
      - {"creatinina": [{"data": "...", "valor": ...}, ...]}
      - {"hemograma": {"hb":[...], "leucocitos":[...]}}
    """
    linhas = []
    for k, v in dados_series.items():
        if isinstance(v, list):
            for p in v:
                linhas.append({"data": p.get("data"), "exame": k, "valor": p.get("valor")})
        elif isinstance(v, dict):
            for subk, arr in v.items():
                for p in arr:
                    linhas.append({"data": p.get("data"), "exame": f"{k}:{subk}", "valor": p.get("valor")})
    df = pd.DataFrame(linhas)
    if not df.empty and "data" in df.columns:
        try:
            df["data"] = pd.to_datetime(df["data"])
        except Exception:
            pass
    if df.empty:
        return None
    fig = px.line(df, x="data", y="valor", color="exame", markers=True, title=titulo)
    return fig