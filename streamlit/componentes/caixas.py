import streamlit as st
from typing import Iterable, Tuple

def caixa_kpi(container: st.delta_generator.DeltaGenerator, titulo: str, linhas: Iterable[Tuple[str, object]]):
    container.markdown(f"#### {titulo}")
    for rotulo, valor in linhas:
        container.markdown(f"**{rotulo}**: {valor}")