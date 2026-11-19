"""iSolarCloud (Sungrow) OpenAPI, no formato que já funciona nos ETLs da pasta dadosUsinas.

- Login: POST /openapi/login com `appkey` no corpo e o header `x-access-key` = chave secreta da aplicação.
- Usinas: getPowerStationList (nome -> ps_id); inversores: getDeviceList (device_type_list=[1]) -> ps_key.
- Energia diária: getDevicePointMinuteDataList, ponto p1 = geração ACUMULADA DO DIA em Wh por inversor
  (zera à meia-noite). Energia do dia da usina = soma, entre inversores, do maior p1 do dia.
  A API só aceita janelas de até 3h por chamada, então consultamos o último bloco do dia (21h–23h59).
"""
import time
from datetime import date, datetime, timedelta

import requests


class Sungrow:
    BLOCOS_FINAIS = [("210000", "235959"), ("180000", "205959")]  # o 2º só se o 1º vier vazio

    def __init__(self, appkey, secret_key, email, senha, base_url="https://gateway.isolarcloud.com.hk"):
        self.appkey, self.secret, self.email, self.senha = appkey, secret_key, email, senha
        self.base = base_url.rstrip("/")
        self.token = None
        self._usinas = None   # nome -> ps_id
        self._ps_keys = {}    # ps_id -> [ps_key]

    @property
    def _headers(self):
        return {"Content-Type": "application/json;charset=UTF-8", "sys_code": "200",
                "x-access-key": self.secret, "x-client-tz": "GMT-3", "x-sign-code": "0"}

    def _post(self, caminho, corpo, relogar=True):
        if caminho != "/openapi/login" and not self.token:
            self._login()
        base = {"appkey": self.appkey, "lang": "_en_US", "sys_code": 207}
        if self.token:
            base["token"] = self.token
        r = self._http(caminho, {**base, **corpo})
        j = r.json()
        if j.get("result_code") == "E904" and relogar:  # token expirado
            self.token = None
            self._login()
            return self._post(caminho, corpo, relogar=False)
        if j.get("result_code") != "1":
            raise RuntimeError(f"iSolarCloud {caminho}: {j.get('result_code')} {j.get('result_msg')}")
        return j["result_data"]

    def _http(self, caminho, corpo, tentativas=3):
        """POST com nova tentativa em timeout/queda de conexão (o gateway da Sungrow oscila)."""
        for i in range(tentativas):
            try:
                r = requests.post(self.base + caminho, json=corpo, headers=self._headers, timeout=45)
                r.raise_for_status()
                return r
            except (requests.Timeout, requests.ConnectionError):
                if i == tentativas - 1:
                    raise
                time.sleep(3 * (i + 1))

    def _login(self):
        j = self._http("/openapi/login", {
            "user_account": self.email, "user_password": self.senha, "appkey": self.appkey,
            "lang": "_en_US", "sys_code": "207"}).json()
        if j.get("result_code") != "1":
            raise RuntimeError(f"iSolarCloud login: {j.get('result_msg')}")
        self.token = j["result_data"]["token"]

    def _ps_id(self, nome):
        if self._usinas is None:
            self._usinas, pagina = {}, 1
            while True:
                d = self._post("/openapi/getPowerStationList", {"curPage": pagina, "size": 100})
                lote = d.get("pageList", [])
                self._usinas.update({p["ps_name"].strip(): str(p["ps_id"]) for p in lote})
                if not lote or len(self._usinas) >= int(d.get("rowCount", 0)):
                    break
                pagina += 1
        if nome not in self._usinas:
            raise KeyError(f"usina {nome} não existe na conta iSolarCloud")
        return self._usinas[nome]

    def _chaves_inversores(self, ps_id):
        if ps_id not in self._ps_keys:
            d = self._post("/openapi/getDeviceList", {"curPage": 1, "size": 50, "ps_id": ps_id, "device_type_list": [1]})
            self._ps_keys[ps_id] = [x["ps_key"] for x in d.get("pageList", [])]
        return self._ps_keys[ps_id]

    def _energia_do_dia(self, chaves, dia: date):
        """kWh do dia (soma dos inversores) ou None se o portal não devolveu dados."""
        for ini, fim in self.BLOCOS_FINAIS:
            d = self._post("/openapi/getDevicePointMinuteDataList", {
                "start_time_stamp": dia.strftime("%Y%m%d") + ini, "end_time_stamp": dia.strftime("%Y%m%d") + fim,
                "ps_key_list": chaves, "points": "p1", "minute_interval": 60, "is_get_data_acquisition_time": "1"})
            total, achou = 0.0, False
            for registros in (d or {}).values():
                vals = [float(x["p1"]) for x in registros if x.get("p1") not in (None, "")]
                if vals:
                    total += max(vals)
                    achou = True
            if achou:
                return total / 1000.0
        return None

    def daily_energy(self, plant_id, start: date, end: date):
        chaves = self._chaves_inversores(self._ps_id(plant_id))
        out, dia = {}, start
        while dia <= end:
            kwh = self._energia_do_dia(chaves, dia)
            if kwh is not None:
                out[dia] = kwh
            dia += timedelta(1)
            time.sleep(0.3)
        return out
