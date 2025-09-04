from datetime import datetime, timezone
from math import floor, ceil
from typing import Iterable

#####################################################
# Entrada:     valores (iterável numérico) - amostra de valores.
# Saída:       float - percentil 90 (com interpolação linear).
# Descrição:   Calcula o P90 
#####################################################
def p90(valores: Iterable[float]) -> float:
    valores = list(valores)
    if not valores:
        return 0.0
    valores = sorted(valores)
    k = 0.9 * (len(valores) - 1)
    f, c = floor(k), ceil(k)
    if f == c:
        return float(valores[int(k)])
    return float(valores[f] + (k - f) * (valores[c] - valores[f]))

#####################################################
# Entrada:     a (datetime), b (datetime)
# Descrição:   Retorna a diferença em dias entre b e a.
#####################################################
def diferenca_dias(a: datetime, b: datetime) -> float:
    return (b - a).total_seconds() / 86400.0

#####################################################
# Entrada:     a (datetime), b (datetime)
# Descrição:   Retorna a diferença em horas entre b e a.
#####################################################
def diferenca_horas(a: datetime, b: datetime) -> float:
    return (b - a).total_seconds() / 3600.0

#####################################################
# Entrada:     dt (datetime) - com ou sem timezone.
# Descrição:   Converte para UTC e serializa em ISO, trocando
#              '+00:00' por 'Z' para padronização.
#####################################################
def data_iso_utc(dt: datetime) -> str:
    return (
        dt.astimezone(timezone.utc)
          .replace(tzinfo=timezone.utc)
          .isoformat()
          .replace("+00:00", "Z")
    )
