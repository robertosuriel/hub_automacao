"""Targets diários (planilha do Gersio): colunas C=ano, D=mês, E..AI = dias 1..31, a partir da linha 5."""
from datetime import date, timedelta
from openpyxl import load_workbook


class Targets:
    def __init__(self, arquivo, ano):
        self.wb = load_workbook(arquivo, data_only=True, read_only=True)
        self.ano = ano
        self._cache = {}

    def diario(self, aba):
        if aba in self._cache:
            return self._cache[aba]
        d = {}
        if aba not in self.wb.sheetnames:
            self._cache[aba] = d
            return d
        for r in self.wb[aba].iter_rows(min_row=5, values_only=True):
            if len(r) < 35 or r[2] != self.ano or not isinstance(r[3], int):
                continue
            for dia in range(1, 32):
                v = r[3 + dia]
                if isinstance(v, (int, float)):
                    try:
                        d[date(self.ano, r[3], dia)] = float(v)
                    except ValueError:
                        pass
        self._cache[aba] = d
        return d

    def soma(self, aba, leit_ant, leit_atu):
        """Soma do dia seguinte à leitura anterior até o dia da leitura atual, inclusive. None se faltar dia."""
        d = self.diario(aba)
        total, t = 0.0, leit_ant + timedelta(1)
        while t <= leit_atu:
            if t not in d:
                return None
            total += d[t]
            t += timedelta(1)
        return total
