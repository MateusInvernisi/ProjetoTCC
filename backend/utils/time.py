from datetime import datetime, timezone
from math import floor, ceil
from typing import Iterable

def p90(valores: Iterable[float]) -> float:
    valores = list(valores)
    if not valores:
        return 0.0
    valores.sort()
    k = 0.9 * (len(valores) - 1)
    f, c = floor(k), ceil(k)
    if f == c:
        return float(valores[int(k)])
    return float(valores[f] + (k - f) * (valores[c] - valores[f]))

def diferenca_dias(a: datetime, b: datetime) -> float:
    return (b - a).total_seconds() / 86400.0

def diferenca_horas(a: datetime, b: datetime) -> float:
    return (b - a).total_seconds() / 3600.0

def data_iso_utc(dt: datetime) -> str:
    return (
        dt.astimezone(timezone.utc)
          .replace(tzinfo=timezone.utc)
          .isoformat()
          .replace("+00:00", "Z")
    )
