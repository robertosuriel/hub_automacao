"""Conectores de inversor. Interface comum: daily_energy(plant_id, start, end) -> dict[date, kWh]."""
from typing import Callable, Dict, Optional

from .fronius import Fronius
from .manual import SerieManual
from .solis import Solis
from .sungrow import Sungrow


def montar_conectores(segredo: Callable[[str], str]):
    """Cria os conectores cujas credenciais existem em st.secrets/env. `segredo(nome)` devolve '' se faltar."""
    c = {}
    if segredo("SUNGROW_APPKEY") and segredo("SUNGROW_SECRET_KEY"):
        c["iSolarCloud"] = Sungrow(segredo("SUNGROW_APPKEY"), segredo("SUNGROW_SECRET_KEY"),
                                   segredo("SUNGROW_LOGIN_EMAIL"), segredo("SUNGROW_LOGIN_PASS"),
                                   segredo("SUNGROW_BASE_URL") or "https://gateway.isolarcloud.com.hk")
    if segredo("SOLIS_KEY_ID") and segredo("SOLIS_KEY_SECRET"):
        c["SolisCloud"] = Solis(segredo("SOLIS_KEY_ID"), segredo("SOLIS_KEY_SECRET"),
                                segredo("SOLIS_BASE_URL") or "https://www.soliscloud.com:13333")
    if segredo("FRONIUS_ACCESS_KEY_ID") and segredo("FRONIUS_ACCESS_KEY_VALUE"):
        c["Fronius"] = Fronius(segredo("FRONIUS_ACCESS_KEY_ID"), segredo("FRONIUS_ACCESS_KEY_VALUE"),
                                segredo("FRONIUS_BASE_URL") or "https://api.solarweb.com/swqapi")
    return c


def coletar(usinas, conectores, ini_fim: Dict[str, tuple], manual: Optional[SerieManual] = None):
    """Devolve ({usina: {date: kWh}}, avisos). Usa a API; cai no CSV manual se não houver conector ou der erro."""
    series, avisos = {}, []
    for u in usinas:
        if u.nome not in ini_fim:
            continue
        ini, fim = ini_fim[u.nome]
        serie, erro = None, None
        con = conectores.get(u.portal)
        if con:
            try:
                serie = con.daily_energy(u.id_portal, ini, fim)
            except Exception as e:  # um portal fora do ar não pode derrubar o relatório
                erro = f"{type(e).__name__}: {str(e)[:150]}"
        if not serie and manual and manual.tem(u.nome):
            serie = manual.serie(u.nome)
        if not serie:
            avisos.append(f"{u.nome}: sem dados do inversor ({erro or 'portal ' + u.portal + ' sem credencial/API'})")
        series[u.nome] = serie or {}
    return series, avisos
