"""iSolarCloud (Sungrow) OpenAPI. Login com appkey + x-access-key + usuário, depois token."""
from datetime import date

import requests


class Sungrow:
    # TODO confirmar na doc do portal de desenvolvedor da Sungrow o endpoint/ponto de energia diária da usina.
    ENDPOINT_DIARIO = "/openapi/getPowerStationStatistic"

    def __init__(self, appkey, access_key, user, password, base_url="https://gateway.isolarcloud.com.hk"):
        self.appkey, self.access_key, self.user, self.password = appkey, access_key, user, password
        self.base = base_url.rstrip("/")
        self.token = None
        self._ps_ids = None

    def _post(self, caminho, corpo):
        corpo = dict(corpo, appkey=self.appkey, lang="_en_US")
        if self.token:
            corpo["token"] = self.token
        r = requests.post(self.base + caminho, json=corpo, timeout=30,
                          headers={"x-access-key": self.access_key, "Content-Type": "application/json"})
        r.raise_for_status()
        j = r.json()
        if str(j.get("result_code")) != "1":
            raise RuntimeError(f"iSolarCloud {caminho}: {j.get('result_msg')}")
        return j["result_data"]

    def _login(self):
        if not self.token:
            self.token = self._post("/openapi/login", {"user_account": self.user, "user_password": self.password})["token"]

    def _ps_id(self, nome):
        self._login()
        if self._ps_ids is None:
            lista = self._post("/openapi/getPowerStationList", {"curPage": 1, "size": 200}).get("pageList", [])
            self._ps_ids = {p["ps_name"].strip(): str(p["ps_id"]) for p in lista}
        if nome not in self._ps_ids:
            raise KeyError(f"usina {nome} não existe na conta iSolarCloud")
        return self._ps_ids[nome]

    def daily_energy(self, plant_id, start: date, end: date):
        ps_id = self._ps_id(plant_id)
        r = self._post(self.ENDPOINT_DIARIO, {"ps_id": ps_id, "start_time": start.strftime("%Y%m%d"),
                                              "end_time": end.strftime("%Y%m%d"), "data_type": "day"})
        out = {}
        for it in r.get("list", []):  # formato a confirmar: {"time": "YYYYMMDD", "energy": kWh}
            d = date(int(it["time"][:4]), int(it["time"][4:6]), int(it["time"][6:8]))
            if it.get("energy") is not None:
                out[d] = float(it["energy"])
        return out
