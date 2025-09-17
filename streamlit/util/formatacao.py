from datetime import datetime

def dias_para_dias_horas(valor_dias: float) -> str:
    if valor_dias is None:
        return "-"
    dias = int(valor_dias)
    horas = int(round((valor_dias - dias) * 24))
    return f"{dias} dias e {horas} horas"


def formatar_data_iso_br(dt_str: str) -> str:
    if not dt_str:
        return "-"
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt.strftime("%d/%m/%Y %H:%M")
    except Exception:
        return dt_str