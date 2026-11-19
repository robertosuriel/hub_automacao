r"""Teste manual do login/coleta do SolisCloud por Selenium.

Uso (na raiz do projeto):  venv\Scripts\python.exe -m auditoria_geracao.testar_solis_web
Lê SOLIS_LOGIN_USER / SOLIS_LOGIN_PASSWORD de .streamlit/secrets.toml. Para ver o navegador, rode com --visivel.
Esperado: Itaguaçu 18, 24/09-23/10/2026 = ~20.316 kWh, dias sem dado 28/09 e 20/10.
"""
import sys
import tomllib
from datetime import date

from .inversores.solis_web import SolisWeb

sec = tomllib.load(open(".streamlit/secrets.toml", "rb"))
faltam = [k for k in ("SOLIS_LOGIN_USER", "SOLIS_LOGIN_PASSWORD") if not sec.get(k)]
if faltam:
    sys.exit(
        "Faltam no .streamlit/secrets.toml: " + ", ".join(faltam) + ".\n"
        "Adicione-as ANTES da linha [google_credentials] (chaves de topo precisam vir antes das tabelas), por exemplo:\n"
        '  SOLIS_LOGIN_USER = "seu-login-do-soliscloud"\n'
        '  SOLIS_LOGIN_PASSWORD = "sua-senha"'
    )
con = SolisWeb(sec["SOLIS_LOGIN_USER"], sec["SOLIS_LOGIN_PASSWORD"], headless="--visivel" not in sys.argv)
try:
    s = con.daily_energy("1298491919450704520", date(2026, 9, 24), date(2026, 10, 23))
    faltando = [d for d in (date(2026, 9, 24) + __import__("datetime").timedelta(i) for i in range(30)) if not s.get(d)]
    print(f"{len(s)} dias | total {sum(s.values()):,.1f} kWh | sem dado: {[d.strftime('%d/%m') for d in faltando]}")
finally:
    con.close()
