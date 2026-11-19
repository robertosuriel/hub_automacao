"""SolisCloud Platform API v2.0 (HMAC-SHA1).

Assinatura e parâmetros conferidos contra o cliente open source hultenvp/soliscloud_api. A API exige relógio
local dentro de 15 min do servidor (senão HTTP 408). Formato da resposta (data[].dateStr / energy) a confirmar
quando as chaves existirem."""
import base64
import hashlib
import hmac
import json
import time
from datetime import date, datetime, timezone

import requests


class Solis:
    def __init__(self, key_id, key_secret, base_url="https://www.soliscloud.com:13333"):
        self.key_id, self.key_secret, self.base = key_id, key_secret, base_url.rstrip("/")

    def _post(self, caminho, corpo):
        body = json.dumps(corpo, separators=(",", ":"))
        md5 = base64.b64encode(hashlib.md5(body.encode()).digest()).decode()
        data = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
        ctype = "application/json"
        assinar = f"POST\n{md5}\n{ctype}\n{data}\n{caminho}"
        sign = base64.b64encode(hmac.new(self.key_secret.encode(), assinar.encode(), hashlib.sha1).digest()).decode()
        r = requests.post(self.base + caminho, data=body.encode(), timeout=30, headers={
            "Content-MD5": md5, "Content-Type": ctype, "Date": data, "Authorization": f"API {self.key_id}:{sign}"})
        r.raise_for_status()
        j = r.json()
        if str(j.get("code")) not in ("0", "200") and not j.get("success"):
            raise RuntimeError(f"Solis: {j.get('msg')}")
        return j

    def daily_energy(self, plant_id, start: date, end: date):
        out, y, m = {}, start.year, start.month
        while (y, m) <= (end.year, end.month):
            j = self._post("/v1/api/stationMonth", {"id": int(plant_id), "money": "BRL",
                                                    "month": f"{y}-{m:02d}"})
            time.sleep(0.6)  # a API aceita no máx. 2 requisições por segundo
            for it in j.get("data") or []:
                d = date.fromisoformat(it["dateStr"])
                if start <= d <= end and it.get("energy") is not None:
                    out[d] = float(it["energy"])
            y, m = (y + 1, 1) if m == 12 else (y, m + 1)
        return out
