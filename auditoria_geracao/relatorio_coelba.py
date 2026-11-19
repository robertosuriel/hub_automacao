import re, glob, json, os

MES = {'jan':1,'fev':2,'mar':3,'abr':4,'mai':5,'jun':6,'jul':7,'ago':8,'set':9,'out':10,'nov':11,'dez':12}
def num(s):
    return float(s.replace('.', '').replace(',', '.')) if s not in ('-', '') else None

USINA = {
 '7084200845':'Riachão','7087961308':'Itaguaçu 1','7089712445':'Itaguaçu 4','7092251262':'Itaguaçu 7 (antiga)',
 '7093708452':'Itaguaçu 8','7093114668':'Itaguaçu 9','7095209127':'Itaguaçu 15','7095208228':'Itaguaçu 14',
 '7095207043':'Itaguaçu 13','7098720140':'Itaguaçu 7 (nova)','7090822300':'Itaguaçu 5','7090898080':'Itaguaçu 6',
 '7099408493':'Itaguaçu 16','7099472370':'Itaguaçu 17','7099477835':'Itaguaçu 18','7099508218':'Itaguaçu 19',
 '7093802912':'Itaguaçu 10','7093807787':'Itaguaçu 11','7093810311':'Itaguaçu 12','7086422110':'Itaguaçu 2',
 '7086396470':'Itaguaçu 3','7101856725':'Itaguaçu 22','7102413746':'Itaguaçu 21','7102983682':'Itaguaçu 20'}

def parse(path):
    return parse_texto(open(path, encoding='utf-8').read(), os.path.basename(path))

def parse_texto(texto, nome=''):
    """Recebe o texto do `pdftotext -layout` do relatório de micro e minigeração."""
    raw = texto.splitlines()
    L = [re.sub(r'\s+', ' ', l).strip() for l in raw]
    d = {'file': nome}
    for l in L:
        m = re.match(r'Cliente: (.*?) (\d{10})$', l)
        if m: d['cliente'], d['conta'] = m.group(1), m.group(2)
        m = re.match(r'Período: (.*)$', l)
        if m and 'periodo' not in d: d['periodo'] = m.group(1)
        m = re.match(r'Potencia do Sistema Fotovolta[ií]co:\s*(.*)$', l)
        if m: d['potencia'] = m.group(1).strip()
    # generator lines
    gens = []
    for i, l in enumerate(L):
        m = re.match(r'Gerador (\d+) (?:(\d{14,15}) )?(\d+) (\d+) (\d{2})/(\d{4}) ([\d,]+) ([\d,]+) ([\d,]+)$', l)
        if m:
            gens.append(dict(line=i, medidor=m.group(1), uc=m.group(2), de=int(m.group(3)), ate=int(m.group(4)),
                             mes=int(m.group(5)), ano=int(m.group(6)), inj=num(m.group(7)), saldo_ant=num(m.group(8)),
                             acum=num(m.group(9))))
    d['gens'] = gens
    # 13 months
    rows = []
    for l in L:
        m = re.match(r'(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)/(\d{4}) (\d{15}) ([\d,]+) ([\d,]+) ([\d,]+) ([\d,]+) ([\d,]+)$', l)
        if m:
            rows.append(dict(mes=MES[m.group(1)], ano=int(m.group(2)), uc=m.group(3), cons=num(m.group(4)), inj=num(m.group(5)),
                             fat=num(m.group(6)), comp=num(m.group(7)), cred=num(m.group(8))))
    d['rows'] = rows
    d['has_13'] = any('DADOS DOS ÚLTIMOS 13 MESES' in l for l in L)
    # footer
    d['footer'] = None
    for i, l in enumerate(L):
        if l.startswith('COMPENSADO ACUMULADO EXPIRAR'):
            t = next((x for x in L[i + 1:] if x), '').split()
            if len(t) < 6: continue
            d['footer'] = dict(tot_inj=num(t[0]), tot_comp=num(t[1]), saldo=num(t[2]), prox_expirar=num(t[3]),
                               ciclo=t[4], expirados=num(t[5]))
    # resumo
    res = []
    inres = False
    for l in L:
        if 'RESUMO DO SALDO' in l: inres = True; continue
        if inres:
            m = re.match(r'(\d{15}) ?(\*)? (\d+) ?% ([\d,]+)$', l)
            if m: res.append(dict(uc=m.group(1), star=bool(m.group(2)), pct=int(m.group(3)), saldo=num(m.group(4))))
            if l.startswith('VOCÊ SABIA'): break
    d['resumo'] = res
    # consumo blocks: Consumidor lines, assigned to the month of the preceding Gerador line
    cons = []
    gi = [g['line'] for g in gens]
    blocks = [i for i, l in enumerate(L) if l.startswith('Descrição Medidor De Até Consumo')]
    for i, l in enumerate(L):
        m = re.match(r'Consumidor (\d+) (\d{14,15}) (\d+) (\d+) ([\d,]+)(.*)$', l)
        if m:
            prev = [g for g in gens if g['line'] < i]
            mes = prev[-1]['mes'] if prev else None
            # utilization rows: lines in the same block (between block headers)
            b0 = max([b for b in blocks if b < i], default=i)
            nxt = [b for b in blocks if b > i] + [k for k, x in enumerate(L) if k > i and (x.startswith('DADOS DOS') or x.startswith('Gerador') or x in ('SETEMBRO 2026',) )]
            b1 = min(nxt) if nxt else len(L)
            util = 0.0
            for k in range(b0, b1):
                txt = L[k] if k != i else m.group(6)
                for u in re.finditer(r'(\d{2}/\d{4}\*?|-) ([\d,]+) ([\d,]+) (\d{2}/\d{4}|-)', txt):
                    util += num(u.group(2))
            cons.append(dict(medidor=m.group(1), uc=m.group(2), de=int(m.group(3)), ate=int(m.group(4)),
                             consumo=num(m.group(5)), mes=mes, util=round(util, 2)))
    d['cons'] = cons
    d['consumo_months'] = sorted({c['mes'] for c in cons if c['mes']})
    # generator UC
    ucs = [g['uc'] for g in gens if g['uc']]
    if not ucs:
        for k, l in enumerate(L):
            if re.fullmatch(r'\d{14}', l) and k + 2 < len(L) and L[k + 1].startswith('Gerador') and re.fullmatch(r'\d', L[k + 2]):
                ucs.append(l + L[k + 2])
    if not ucs and res:
        ucs = [res[0]['uc']]
    d['gen_uc'] = ucs[0] if ucs else None
    return d
