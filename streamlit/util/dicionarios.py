from typing import Any, Iterable

def pegar_seguro(d: dict, caminho: Iterable, padrao=None) -> Any:
    atual = d
    for p in caminho:
        if isinstance(atual, dict) and p in atual:
            atual = atual[p]
        else:
            return padrao
    return atual