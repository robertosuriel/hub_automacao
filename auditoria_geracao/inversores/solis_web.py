"""SolisCloud via navegador (Selenium) — ponte enquanto a API oficial não é liberada (ticket Solis 1132264).

Como funciona (validado à mão no Chrome em 19/11/2026):
- A página da usina (/overview/plantStation/details/overview/<id>) tem o cartão "Dados operacionais" com os botões
  Dia | Mês | Ano. No modo Mês o app chama POST /api/chart/station/month (assinada pelo próprio app) e a resposta
  traz data[].dateStr + data[].energy (kWh). Um interceptador de XHR instalado em toda página nova guarda essas
  respostas em window.__solis; o robô só navega e lê. Somando 24/09-23/10 da Itaguaçu 18 deu 20.316,5 kWh (handoff: 20.316).
- Seletores: botões ".header-r .el-button" (texto Mês/Month); setas ".stepped-date-picker-container .left-icon/.right-icon";
  o input do seletor mostra "MM/AAAA".

NÃO validado: o login automático. A tela tem usuário/senha + aceite dos termos; se aparecer quebra-cabeça ou
código de verificação ao entrar, o robô para com erro explicando (e salva print em erro_solis_login.png).
"""
import json
import re
import time
from datetime import date

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

URL_LOGIN = "https://www.soliscloud.com/login"
URL_USINA = "https://www.soliscloud.com/overview/plantStation/details/overview/{id}"

_HOOK = """
window.__solis = window.__solis || [];
(function () {
  if (window.__solisHook) return; window.__solisHook = true;
  const o = XMLHttpRequest.prototype.open, s = XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.open = function (m, u) { this.__u = u; return o.apply(this, arguments); };
  XMLHttpRequest.prototype.send = function (b) {
    this.addEventListener('load', () => {
      if ((this.__u || '').includes('chart/station/month')) window.__solis.push({body: b, resp: this.responseText});
    });
    return s.apply(this, arguments);
  };
})();
"""


class SolisWeb:
    def __init__(self, usuario, senha, headless=True):
        self.usuario, self.senha, self.headless = usuario, senha, headless
        self.d = None
        self._cache = {}  # (id, ano, mes) -> {date: kWh}

    # ---------- navegador ----------
    def _driver(self):
        if self.d:
            return self.d
        o = Options()
        if self.headless:
            o.add_argument("--headless=new")
        for a in ("--window-size=1920,1080", "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu",
                  "--log-level=3", "--lang=pt-BR"):
            o.add_argument(a)
        self.d = webdriver.Chrome(options=o)
        self.d.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": _HOOK})
        self._login()
        return self.d

    def close(self):
        if self.d:
            try:
                self.d.quit()
            finally:
                self.d = None

    def _esperar(self, cond, timeout=30, passo=0.5):
        fim = time.time() + timeout
        while time.time() < fim:
            r = cond()
            if r:
                return r
            time.sleep(passo)
        return None

    def _erro_login(self, motivo):
        try:
            self.d.save_screenshot("erro_solis_login.png")
        except Exception:
            pass
        texto = ""
        try:
            texto = self.d.find_element(By.TAG_NAME, "body").text.replace("\n", " | ")[:300]
        except Exception:
            pass
        raise RuntimeError(f"SolisCloud login: {motivo}. Tela: {texto}")

    # ---------- login ----------
    def _login(self):
        d = self.d
        d.get(URL_LOGIN)
        senha = self._esperar(lambda: next((e for e in d.find_elements(By.CSS_SELECTOR, "input[type=password]")
                                            if e.is_displayed()), None), 25)
        if not senha:
            self._erro_login("campo de senha não apareceu")
        usuario = next(e for e in d.find_elements(By.CSS_SELECTOR, "input[type=text]") if e.is_displayed())
        usuario.clear()
        usuario.send_keys(self.usuario)
        senha.clear()
        senha.send_keys(self.senha)
        # aceite dos termos (checkbox do Element Plus): clica no rótulo
        for cb in d.find_elements(By.CSS_SELECTOR, ".el-checkbox, input[type=checkbox]"):
            if cb.is_displayed():
                d.execute_script("arguments[0].click();", cb)
                break
        botao = next((b for b in d.find_elements(By.CSS_SELECTOR, "button")
                      if b.is_displayed() and b.text.strip().lower() in ("entrar", "login", "sign in", "log in")), None)
        if not botao:
            self._erro_login("botão Entrar não encontrado")
        d.execute_script("arguments[0].click();", botao)
        if not self._esperar(lambda: "/login" not in d.current_url, 40):
            self._erro_login("continuou na tela de login (senha errada, quebra-cabeça ou código de verificação?)")

    # ---------- dados ----------
    def _clicar_texto(self, container_css, textos):
        for b in self.d.find_elements(By.CSS_SELECTOR, container_css):
            if b.is_displayed() and b.text.strip() in textos:
                self.d.execute_script("arguments[0].click();", b)
                return True
        return False

    def _mes_mostrado(self):
        try:
            v = self.d.find_element(By.CSS_SELECTOR, ".stepped-date-picker-container input").get_attribute("value")
            m = re.match(r"(\d{2})/(\d{4})$", v or "")
            return (int(m.group(2)), int(m.group(1))) if m else None
        except Exception:
            return None

    def _capturado(self, ano, mes):
        alvo = f"{ano}-{mes:02d}"
        for x in reversed(self.d.execute_script("return window.__solis || [];")):
            if json.loads(x["body"]).get("month") == alvo:
                return json.loads(x["resp"])
        return None

    def _meses(self, id_usina, meses):
        """Abre a usina no modo Mês e navega pelas setas até capturar cada mês pedido."""
        d = self._driver()
        d.get(URL_USINA.format(id=id_usina))
        if not self._esperar(lambda: d.find_elements(By.CSS_SELECTOR, ".header-r .el-button"), 40):
            raise RuntimeError(f"SolisCloud: página da usina {id_usina} não carregou")
        if not self._clicar_texto(".header-r .el-button", ("Mês", "Month")):
            raise RuntimeError("SolisCloud: botão Mês não encontrado (o portal mudou?)")
        atual = self._esperar(self._mes_mostrado, 15)
        if not atual:
            raise RuntimeError("SolisCloud: seletor de mês não apareceu")
        for (ano, mes) in sorted(meses, reverse=True):
            while self._mes_mostrado() != (ano, mes):
                m = self._mes_mostrado()
                lado = ".left-icon" if (ano, mes) < m else ".right-icon"
                alvo = d.find_element(By.CSS_SELECTOR, f".stepped-date-picker-container {lado}")
                d.execute_script("arguments[0].click();", alvo)
                time.sleep(0.8)
            resp = self._esperar(lambda: self._capturado(ano, mes), 20)
            if resp is None:
                raise RuntimeError(f"SolisCloud: sem resposta de {ano}-{mes:02d} para a usina {id_usina}")
            serie = {}
            for it in resp.get("data") or []:
                if it.get("energy") is not None:
                    serie[date.fromisoformat(it["dateStr"])] = float(it["energy"])
            self._cache[(id_usina, ano, mes)] = serie

    def daily_energy(self, plant_id, start: date, end: date):
        pedir, y, m = [], start.year, start.month
        while (y, m) <= (end.year, end.month):
            if (plant_id, y, m) not in self._cache:
                pedir.append((y, m))
            y, m = (y + 1, 1) if m == 12 else (y, m + 1)
        if pedir:
            self._meses(plant_id, pedir)
        out = {}
        for (pid, y, m), serie in self._cache.items():
            if pid == plant_id:
                out.update({d: v for d, v in serie.items() if start <= d <= end})
        return out
