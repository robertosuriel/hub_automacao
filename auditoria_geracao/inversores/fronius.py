"""Fronius Solar.web Query API (SWQAPI), validado: Riachão 07/10–05/11/2026 = 12.855,8 kWh (igual ao portal).

GET /pvsystems/{id}/aggdata/years/{ano}/months/{mes}/days  ->  data.channels[].values{"dia": Wh}
Canal usado: EnergyProductionTotal. Headers: AccessKeyId / AccessKeyValue.
"""
from datetime import date

import requests


class Fronius:
    paralelo = True
    def __init__(self, key_id, key_value, base_url="https://api.solarweb.com/swqapi"):
        self.h = {"accept": "application/json", "AccessKeyId": key_id, "AccessKeyValue": key_value}
        self.base = base_url.rstrip("/")

    def daily_energy(self, plant_id, start: date, end: date):
        out, y, m = {}, start.year, start.month
        while (y, m) <= (end.year, end.month):
            r = requests.get(f"{self.base}/pvsystems/{plant_id}/aggdata/years/{y}/months/{m}/days",
                             headers=self.h, timeout=30)
            r.raise_for_status()
            for ch in r.json().get("data", {}).get("channels", []):
                if ch.get("channelName") != "EnergyProductionTotal":
                    continue
                for dia, wh in ch.get("values", {}).items():
                    d = date(y, m, int(dia))
                    if start <= d <= end and wh is not None:
                        out[d] = float(wh) / 1000.0  # Wh -> kWh
            y, m = (y + 1, 1) if m == 12 else (y, m + 1)
        return out
