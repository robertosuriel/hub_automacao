"""Extrai dados de auditoria de uma fatura Coelba/Neoenergia (texto de `pdftotext -layout`)."""
import re, sys, json

def br(s):
    """'45.650,00' -> 45650.0 ; '6984,12' -> 6984.12 ; '173818.92' (formato do bloco SCEE/MMGD) -> 173818.92"""
    if s is None:
        return None
    s = s.strip()
    if ',' in s:
        return float(s.replace('.', '').replace(',', '.'))
    return float(s)

def extrai(txt):
    T = re.sub(r'[ \t]+', ' ', txt)
    d = {}
    m = re.search(r'CLASSIFICAÇÃO: (\S+)', T); d['classificacao'] = m.group(1) if m else None
    m = re.search(r'TIPO DE FORNECIMENTO: .*?- (\w+)', T); d['fornecimento'] = m.group(1) if m else None
    m = re.search(r'(\d\.\d{3}\.\d{3}\.\d{3}-\d{2})', T); d['uc'] = m.group(1) if m else None
    if not d['uc']:
        m = re.search(r'CÓDIGO DA INSTALAÇÃO\s+(\d+)', T); d['uc'] = ('Instalação ' + m.group(1)) if m else None
    m = re.search(r'DATA DE EMISSÃO: (\d{2}/\d{2}/\d{4})', T); d['emissao'] = m.group(1) if m else None
    m = re.search(r'\n ?(\d{2}/\d{4}) ([\d.,]+) (\d{2}/\d{2}/\d{4})\n', T)
    if m:
        d['ref'], d['vencimento'] = m.group(1), m.group(3)
    m = re.search(r'LEITURA ANTERIOR (\d{2}/\d{2}/\d{4}) LEITURA ATUAL (\d{2}/\d{2}/\d{4}) N° DE DIAS (\d+) PRÓXIMA LEITURA (\d{2}/\d{2}/\d{4})', T)
    if m:
        d['leit_ant_data'], d['leit_atu_data'], d['dias'], d['prox_leitura'] = m.group(1), m.group(2), int(m.group(3)), m.group(4)
    m = re.search(r'(\d{6,}) Energia Ativa Único ([\d.,]+) ([\d.,]+) ([\d.,]+) ([\d.,]+)', T)
    if m:
        d['medidor'] = m.group(1)
        d['leit_ant_kwh'], d['leit_atu_kwh'], d['constante'], d['consumo_coluna_kwh'] = [br(x) for x in m.groups()[1:]]
    m = re.search(r'Consumo-TUSD kWh ([\d.,]+)', T); d['consumo_faturado_kwh'] = br(m.group(1)) if m else None
    m = re.search(r'função\(ões\) CAT de - ([\d.,]+) kWh', T); d['compensado_kwh'] = br(m.group(1)) if m else None
    m = re.search(r'Excedente ([\d.,]+) kWh e creditos utilizados ([\d.,]+?)\.? ', T)
    if m:
        d['excedente_kwh'], d['creditos_utilizados_kwh'] = br(m.group(1)), br(m.group(2))
    m = re.search(r'Saldo ?p ?a ?ra o proxi ?mo ciclo ([\d.,]+) kWh', T); d['saldo_kwh'] = br(m.group(1)) if m else None
    m = re.search(r'MMGD\.Energia injetada no mes ([\d.,]+) kWh', T); d['injetado_kwh'] = br(m.group(1)) if m else None
    m = re.search(r'TUSD GDII com trib\. ([\d.,]+)', T); d['tusd_gdii_rs'] = br(m.group(1)) if m else 0.0
    m = re.search(r'Acrés\. Band\. (\w+) ([\d.,]+)', T); d['bandeira_rs'] = br(m.group(2)) if m else 0.0
    m = re.search(r'Ilum\. Púb\. Municipal ([\d.,]+)', T); d['cosip_rs'] = br(m.group(1)) if m else 0.0
    enc = re.findall(r'(?:Multa|Juros|IPCA)-NF-? ?\d+ ([\d.,]+)', T)
    d['multas_juros_rs'] = round(sum(br(x) for x in enc), 2)
    m = re.search(r'\nTOTAL ([\d.,]+)', T); d['total_rs'] = br(m.group(1)) if m else None
    if d['total_rs'] is None:
        m = re.search(r'TOTAL A PAGAR R\$ ([\d.,]+)', T); d['total_rs'] = br(m.group(1)) if m else None
    obs = []
    m = re.search(r'Vencto Dt reaviso Valor.*?\n ?(\d{2}/\d{2}/\d{2}) (\d{2}/\d{2}/\d{2}) ([\d.,]+)', T, re.S)
    if m:
        obs.append(f'Reaviso de débito: fatura venc. {m.group(1)} de R$ {m.group(3)} em aberto em {m.group(2)}')
    if 'impedimento à leitura' in T or 'Animais na caixa' in T:
        obs.append('Leitura impedida (animais na caixa do medidor)')
    if enc:
        obs.append('Multa/juros/IPCA por atraso de faturas anteriores')
    if 'Faturado pelo mínimo' in T:
        obs.append('Faturado pelo custo de disponibilidade')
    d['obs'] = '; '.join(obs)
    return d
