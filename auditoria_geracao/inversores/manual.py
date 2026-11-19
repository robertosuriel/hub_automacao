"""Série diária informada à mão (CSV: usina,data,kwh) — contingência enquanto as chaves de API não existem."""
import io

import pandas as pd


class SerieManual:
    def __init__(self, csv_bytes: bytes):
        df = pd.read_csv(io.BytesIO(csv_bytes), sep=None, engine="python", decimal=",")
        df.columns = [c.strip().lower() for c in df.columns]
        df["data"] = pd.to_datetime(df["data"], dayfirst=True).dt.date
        self._s = {}
        for u, g in df.groupby("usina"):
            self._s[str(u).strip()] = {d: float(k) for d, k in zip(g["data"], g["kwh"])}

    def tem(self, nome):
        return nome in self._s

    def serie(self, nome):
        return self._s.get(nome, {})
