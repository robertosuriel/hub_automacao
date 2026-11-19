import numpy as np
import pandas as pd
from datetime import date, timedelta
from dateutil.easter import easter
from gspread.utils import rowcol_to_a1

COL_SOL = "data_vencimento_sol"
# Posição (0-based) onde a coluna data_vencimento_sol deve ficar: logo após "vencimento" (coluna E).
_IDX_COL_SOL = 5


def _letra(n):
    return rowcol_to_a1(1, n).rstrip("0123456789")


def obter_layout(aba):
    """Descobre pelo cabeçalho se a aba já tem a coluna data_vencimento_sol (e devolve as colunas deslocadas)."""
    cabecalho = [str(c).strip().lower() for c in aba.row_values(1)]
    tem_sol = len(cabecalho) > _IDX_COL_SOL and cabecalho[_IDX_COL_SOL] == COL_SOL
    if not tem_sol and COL_SOL in cabecalho:
        print(f"  ⚠️ [{aba.title}] '{COL_SOL}' está na coluna {_letra(cabecalho.index(COL_SOL) + 1)}, "
              f"mas o esperado é F (logo após 'vencimento'); a coluna será ignorada.")
    d = 1 if tem_sol else 0
    return {
        "tem_sol": tem_sol,
        "ultima_exportada": _letra(7 + d),
        "col_link": _letra(10 + d),
        "col_link_pago": _letra(11 + d),
        "idx_link": 10 + d,
        "flags": [_letra(13 + d + i) for i in range(6)],
        "fim_flags": _letra(18 + d),
        "deslocamento_flags": 10 + d,
    }


def _feriados(ano):
    pascoa = easter(ano)
    datas = [
        date(ano, 1, 1), date(ano, 4, 21), date(ano, 5, 1), date(ano, 9, 7), date(ano, 10, 12),
        date(ano, 11, 2), date(ano, 11, 15), date(ano, 11, 20), date(ano, 12, 25),
        date(ano, 7, 2),
        pascoa - timedelta(days=48), pascoa - timedelta(days=47),
        pascoa - timedelta(days=2),
        pascoa + timedelta(days=60),
    ]
    return [np.datetime64(d) for d in datas]


def dia_util_anterior(data):
    """Último dia útil estritamente anterior à data (seg-sex, sem feriados nacionais e da Bahia)."""
    if pd.isna(data):
        return pd.NaT
    d = pd.Timestamp(data).date()
    feriados = _feriados(d.year - 1) + _feriados(d.year) + _feriados(d.year + 1)
    return pd.Timestamp(np.busday_offset(np.datetime64(d), -1, roll="forward", holidays=feriados))
