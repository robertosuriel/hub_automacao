"""Regras de cálculo da auditoria (seção 4 do HANDOFF) e montagem das linhas do relatório."""
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Dict, List, Optional

from .cadastro import Usina

VERMELHO, AMARELO, VERDE, CINZA, NEUTRO = "vermelho", "amarelo", "verde", "cinza", "neutro"


@dataclass
class Linha:
    usina: Usina
    mes_ref: str = ""
    leit_ant: Optional[date] = None
    leit_atu: Optional[date] = None
    dias: Optional[int] = None
    prox: Optional[date] = None
    injetado_fatura: Optional[float] = None
    inversor: Optional[float] = None
    target: Optional[float] = None
    target_max: Optional[float] = None       # só Riachão (faixa)
    relatorio: Optional[float] = None
    obs: List[str] = field(default_factory=list)
    dias_sem_dado: List[date] = field(default_factory=list)

    # --- derivados ---
    @property
    def coelba(self):
        """Injetado usado na comparação: fatura; se faltar, o do relatório."""
        return self.injetado_fatura if self.injetado_fatura is not None else self.relatorio

    @property
    def origem_coelba(self):
        return "fatura" if self.injetado_fatura is not None else ("relatório" if self.relatorio is not None else "")

    @property
    def razao(self):
        if self.coelba is None or not self.inversor:
            return None
        return self.coelba / self.inversor

    @staticmethod
    def _delta(v, t):
        return v / t - 1 if v is not None and t else None

    @property
    def inv_x_target(self):
        return self._delta(self.inversor, self.target)

    @property
    def coelba_x_target(self):
        return self._delta(self.coelba, self.target)

    @property
    def cor(self):
        return cor_da_razao(self.razao)


def cor_da_razao(r):
    """Sempre pelo Coelba ÷ Inversor."""
    if r is None:
        return CINZA
    if r < 0.90:
        return VERMELHO
    if r > 1.10:
        return AMARELO
    if 0.95 <= r <= 1.05:
        return VERDE
    return NEUTRO


def periodo(leit_ant: date, leit_atu: date):
    """Do dia seguinte à leitura anterior até o dia da leitura atual, inclusive."""
    return leit_ant + timedelta(1), leit_atu


def soma_inversor(serie: Dict[date, float], ini: date, fim: date):
    """(total, dias_sem_dado). Dia ausente ou zerado conta como sem dado. total=None se a série é vazia."""
    if not serie:
        return None, []
    total, faltando, t = 0.0, [], ini
    while t <= fim:
        v = serie.get(t)
        if not v:
            faltando.append(t)
        else:
            total += v
        t += timedelta(1)
    return total, faltando


def _fmt_dias(ds):
    return ", ".join(d.strftime("%d/%m") for d in ds)


def observacoes(l: Linha) -> str:
    obs = list(l.obs)
    if l.inversor is None:
        obs.append("Sem dado no portal")
    elif l.dias_sem_dado:
        obs.append(f"Portal sem dado/zerado em {len(l.dias_sem_dado)} dia(s): {_fmt_dias(l.dias_sem_dado)}")
    if l.injetado_fatura is None and l.relatorio is not None:
        obs.append("Fatura sem injetado: usado o relatório da Coelba")
    elif l.coelba is None:
        obs.append("Não veio na fatura")
    r = l.razao
    if r is not None:
        if r < 0.90:
            obs.append(f"Coelba registrou {r:.0%} do inversor: possível perda de crédito, conferir medidor")
        elif r > 1.10:
            obs.append(f"Coelba {r - 1:.0%} acima do inversor: provável falha do portal (dias sem dado)")
    return "; ".join(obs)


def montar_linha(usina: Usina, fatura, serie, targets, relatorio_inj=None, avisos_fatura=()) -> Linha:
    """fatura: dict de faturas.obter_fatura (ou None); serie: {date: kWh}; targets: auditoria_geracao.targets.Targets."""
    l = Linha(usina=usina, obs=list(avisos_fatura))
    l.relatorio = relatorio_inj
    if not fatura:
        l.obs.append("Fatura não encontrada")
        return l
    l.mes_ref, l.leit_ant, l.leit_atu = fatura["mesReferencia"], fatura["leit_ant"], fatura["leit_atu"]
    l.dias, l.prox, l.injetado_fatura = fatura["dias"], fatura["prox"], fatura["injetado"]
    if not l.leit_ant or l.leit_ant.year < 2000:  # primeiro ciclo: leitura anterior zerada
        l.obs.append("Primeiro ciclo (leitura anterior zerada)")
        return l
    ini, fim = periodo(l.leit_ant, l.leit_atu)
    l.inversor, l.dias_sem_dado = soma_inversor(serie, ini, fim)
    if usina.aba_target and targets:
        l.target = targets.soma(usina.aba_target, l.leit_ant, l.leit_atu)
        if usina.aba_target_max:
            l.target_max = targets.soma(usina.aba_target_max, l.leit_ant, l.leit_atu)
        if l.target is None:
            l.obs.append(f"Target indisponível: aba {usina.aba_target} ausente ou com dias faltando na planilha")
    return l
