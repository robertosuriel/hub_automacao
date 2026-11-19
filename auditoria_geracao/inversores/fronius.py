"""Fronius Solar.web Query API (SWQAPI). Chaves AccessKeyId/AccessKeyValue geradas no Solar.web."""
from datetime import date

import requests


class Fronius:
    def __init__(self, key_id, key_value, base_url="https://api.solarweb.com/swqapi"):
        self.h = {"AccessKeyId": key_id, "AccessKeyValue": key_value, "Accept": "application/json"}
        self.base = base_url.rstrip("/")

    def daily_energy(self, plant_id, start: date, end: date):
        # TODO confirmar na doc da SWQAPI: nome do canal e janela máxima do período "days".
        r = requests.get(f"{self.base}/pvsystems/{plant_id}/aggdata", headers=self.h, timeout=30, params={
            "from": start.isoformat(), "to": end.isoformat(), "period": "days", "channel": "EnergyProductionTotal"})
        r.raise_for_status()
        out = {}
        for item in r.json().get("data", []):
            d = date.fromisoformat(str(item.get("logDateTime", ""))[:10])
            for ch in item.get("channels", []):
                if ch.get("channelName") == "EnergyProductionTotal" and ch.get("value") is not None:
                    out[d] = float(ch["value"]) / 1000.0  # Wh -> kWh
        return out
