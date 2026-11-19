"""Conectores de inversor. Interface comum: daily_energy(plant_id, start, end) -> dict[date, kWh]."""
from typing import Callable, Dict, Optional

from .fronius import Fronius
from .manual import SerieManual
from .solis import Solis
from .solis_web import SolisWeb
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
    elif segredo("SOLIS_LOGIN_USER") and segredo("SOLIS_LOGIN_PASSWORD"):
        # ponte via navegador enquanto a API oficial não é liberada
        c["SolisCloud"] = SolisWeb(segredo("SOLIS_LOGIN_USER"), segredo("SOLIS_LOGIN_PASSWORD"))
    if segredo("FRONIUS_ACCESS_KEY_ID") and segredo("FRONIUS_ACCESS_KEY_VALUE"):
        c["Fronius"] = Fronius(segredo("FRONIUS_ACCESS_KEY_ID"), segredo("FRONIUS_ACCESS_KEY_VALUE"),
                                segredo("FRONIUS_BASE_URL") or "https://api.solarweb.com/swqapi")
    return c


def _coletar_uma(u, con, ini_fim, manual):
    ini, fim = ini_fim[u.nome]
    serie, erro = None, None
    if con:
        try:
            serie = con.daily_energy(u.id_portal, ini, fim)
        except Exception as e:  # um portal fora do ar não pode derrubar o relatório
            erro = f"{type(e).__name__}: {str(e)[:150]}"
    if not serie and manual and manual.tem(u.nome):
        serie = manual.serie(u.nome)
    aviso = None
    if not serie:
        aviso = f"{u.nome}: sem dados do inversor ({erro or 'portal ' + u.portal + ' sem credencial/API'})"
    return u.nome, serie or {}, aviso


def coletar(usinas, conectores, ini_fim: Dict[str, tuple], manual: Optional[SerieManual] = None, workers=8):
    """Devolve ({usina: {date: kWh}}, avisos). Usa a API; cai no CSV manual se não houver conector ou der erro.
    Conectores com `paralelo = True` (Sungrow, Fronius) rodam várias usinas ao mesmo tempo."""
    from concurrent.futures import ThreadPoolExecutor
    alvo = [u for u in usinas if u.nome in ini_fim]
    resultados = []
    for portal in dict.fromkeys(u.portal for u in alvo):
        grupo = [u for u in alvo if u.portal == portal]
        con = conectores.get(portal)
        if con and getattr(con, "paralelo", False) and len(grupo) > 1:
            if hasattr(con, "preparar"):
                try:
                    con.preparar()  # login e lista de usinas uma vez só, antes das threads
                except Exception:
                    pass
            with ThreadPoolExecutor(max_workers=workers) as ex:
                resultados += list(ex.map(lambda u: _coletar_uma(u, con, ini_fim, manual), grupo))
        else:
            resultados += [_coletar_uma(u, con, ini_fim, manual) for u in grupo]
    series = {nome: serie for nome, serie, _ in resultados}
    avisos = [a for _, _, a in resultados if a]
    for con in conectores.values():  # fecha o navegador do SolisWeb, se houver
        if hasattr(con, "close"):
            con.close()
    return series, avisos
