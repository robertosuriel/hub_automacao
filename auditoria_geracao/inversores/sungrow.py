"""iSolarCloud (Sungrow) OpenAPI, no formato que já funciona nos ETLs da pasta dadosUsinas.

- Login: POST /openapi/login com `appkey` no corpo e o header `x-access-key` = chave secreta da aplicação.
- Usinas: getPowerStationList (nome -> ps_id); inversores: getDeviceList (device_type_list=[1]) -> ps_key.
- Energia diária: getDevicePointsDayMonthYearDataList (query_type=1 dia, data_type=2 máximo do dia, ponto p1 = geração
  acumulada do dia em Wh por inversor), UMA chamada por usina e período; soma entre os inversores. Validado em
  19/11/2026: Itaguaçu 1, 25/09-26/10 = 16.852,2 kWh (igual ao portal); 19 usinas em ~24 s.
"""
import time
from datetime import date, datetime, timedelta

import requests


class Sungrow:
    paralelo = True  # várias usinas ao mesmo tempo (a lista de usinas e o login são feitos em preparar())
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

    def _carregar_usinas(self):
        if self._usinas is None:
            usinas, pagina = {}, 1
            while True:
                d = self._post("/openapi/getPowerStationList", {"curPage": pagina, "size": 100})
                lote = d.get("pageList", [])
                usinas.update({p["ps_name"].strip(): str(p["ps_id"]) for p in lote})
                if not lote or len(usinas) >= int(d.get("rowCount", 0)):
                    break
                pagina += 1
            self._usinas = usinas

    def preparar(self):
        """Login + lista de usinas, feitos uma vez antes de abrir as threads."""
        self._login()
        self._carregar_usinas()

    def _ps_id(self, nome):
        self._carregar_usinas()
        if nome not in self._usinas:
            raise KeyError(f"usina {nome} não existe na conta iSolarCloud")
        return self._usinas[nome]

    def _chaves_inversores(self, ps_id):
        if ps_id not in self._ps_keys:
            d = self._post("/openapi/getDeviceList", {"curPage": 1, "size": 50, "ps_id": ps_id, "device_type_list": [1]})
            self._ps_keys[ps_id] = [x["ps_key"] for x in d.get("pageList", [])]
        return self._ps_keys[ps_id]

    def _serie_periodo(self, chaves, ini: date, fim: date):
        """Série diária (kWh) do período em UMA chamada: getDevicePointsDayMonthYearDataList com query_type=1 (dia),
        data_type=2 (máximo do dia) e ponto p1 (geração acumulada do dia, Wh, por inversor). Soma entre inversores."""
        d = self._post("/openapi/getDevicePointsDayMonthYearDataList", {
            "ps_key_list": chaves, "points": "p1", "data_point": "p1", "data_type": "2", "query_type": "1",
            "start_time": ini.strftime("%Y%m%d"), "end_time": fim.strftime("%Y%m%d"), "order": "0"})
        out = {}
        for pontos in (d or {}).values():
            for it in pontos.get("p1", []):
                wh = it.get("2")
                if wh in (None, ""):
                    continue
                dia = datetime.strptime(it["time_stamp"], "%Y%m%d").date()
                out[dia] = out.get(dia, 0.0) + float(wh) / 1000.0
        return out

    def daily_energy(self, plant_id, start: date, end: date):
        chaves = self._chaves_inversores(self._ps_id(plant_id))
        out, ini = {}, start
        while ini <= end:  # em blocos de 31 dias por precaução com o limite da API
            fim = min(end, ini + timedelta(days=30))
            out.update(self._serie_periodo(chaves, ini, fim))
            ini = fim + timedelta(days=1)
        return {d: v for d, v in out.items() if v > 0}
