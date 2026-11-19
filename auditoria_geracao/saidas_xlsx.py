"""Planilha editável: indicadores como fórmulas, cores FIXAS e valores calculados gravados no arquivo."""
import io
import re
import zipfile
from datetime import date

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

from .calculo import AMARELO, CINZA, VERDE, VERMELHO, Linha, observacoes

CABECALHO = ["Usina", "codigo_cliente", "mesReferencia", "NUC", "Leitura anterior", "Leitura atual", "Dias",
             "Próxima leitura", "Injetado na fatura (kWh)", "Inversor no período (kWh)", "Portal do inversor",
             "Target (kWh)", "Relatório Coelba (kWh)", "Coelba ÷ Inversor", "Inversor × Target",
             "Coelba × Target", "Observação"]
LARGURAS = [14, 13, 12, 18, 13, 13, 6, 13, 14, 14, 12, 13, 13, 11, 11, 11, 70]
COR_LINHA = {VERMELHO: "F4B6B6", AMARELO: "FFEB9C", VERDE: "C6EFCE", CINZA: "D9D9D9"}

F_RAZAO = ('=IF(NOT(ISNUMBER(J{r})),"Sem comparação: falta inversor",IF(ISNUMBER(I{r}),I{r}/J{r},'
           'IF(ISNUMBER(M{r}),M{r}/J{r},"Sem comparação: falta injetado")))')
F_INV_TGT = '=IF(AND(ISNUMBER(J{r}),ISNUMBER(L{r})),J{r}/L{r}-1,"")'
F_COE_TGT = ('=IF(ISNUMBER(L{r}),IF(ISNUMBER(I{r}),I{r}/L{r}-1,IF(ISNUMBER(M{r}),M{r}/L{r}-1,"")),"")')


def _valores_calculados(l: Linha):
    """Replica as fórmulas em Python para gravar o resultado em cache."""
    inv, tgt, inj, rel = l.inversor, l.target, l.injetado_fatura, l.relatorio
    if inv is None:
        n = "Sem comparação: falta inversor"
    elif inj is not None:
        n = inj / inv
    elif rel is not None:
        n = rel / inv
    else:
        n = "Sem comparação: falta injetado"
    o = inv / tgt - 1 if inv is not None and tgt else ""
    base = inj if inj is not None else rel
    p = (base / tgt - 1 if base is not None else "") if tgt else ""
    return n, o, p


def gerar_xlsx(linhas, titulo_aba: str, rodape: str) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = titulo_aba[:31]
    ws.append(CABECALHO)
    for c in ws[1]:
        c.font = Font(bold=True, color="FFFFFF")
        c.fill = PatternFill("solid", fgColor="1F4E78")
        c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    cache = {}
    for i, l in enumerate(linhas, start=2):
        u = l.usina
        if l.target is not None:
            tgt = l.target
        else:
            tgt = u.obs_target or "Pendente"
        row = [u.nome, int(u.codigo_cliente), l.mes_ref or "Sem fatura", u.nuc, l.leit_ant, l.leit_atu, l.dias,
               l.prox,
               l.injetado_fatura if l.injetado_fatura is not None else "Não veio na fatura",
               round(l.inversor, 1) if l.inversor is not None else "Sem dado no portal",
               u.portal, round(tgt, 1) if isinstance(tgt, float) else tgt,
               l.relatorio if l.relatorio is not None else "Relatório não recebido",
               F_RAZAO.format(r=i), F_INV_TGT.format(r=i), F_COE_TGT.format(r=i)]
        obs = observacoes(l)
        if l.target_max is not None and l.target is not None:
            obs = f"Target em faixa: {min(l.target, l.target_max):,.0f} a {max(l.target, l.target_max):,.0f} kWh (Min–Max); " \
                  f"indicadores calculados sobre o Min. " + obs
        row.append(obs)
        ws.append(row)
        cache[i] = _valores_calculados(l)
        cor = COR_LINHA.get(l.cor)
        for c in ws[i]:
            if cor:
                c.fill = PatternFill("solid", fgColor=cor)  # fixa: o visualizador do WhatsApp ignora cond. format.
        for col in (5, 6, 8):
            ws.cell(i, col).number_format = "dd/mm/yyyy"
        for col in (9, 10, 12, 13):
            ws.cell(i, col).number_format = "#,##0"
        ws.cell(i, 14).number_format = "0.0%"
        ws.cell(i, 15).number_format = ws.cell(i, 16).number_format = "+0.0%;-0.0%"
        ws.cell(i, 17).alignment = Alignment(wrap_text=True, vertical="top")
    for k, w in enumerate(LARGURAS, start=1):
        ws.column_dimensions[get_column_letter(k)].width = w
    ws.freeze_panes = "B2"
    r = len(linhas) + 3
    for t in rodape.split("\n"):
        ws.cell(r, 1, t)
        r += 1
    buf = io.BytesIO()
    wb.save(buf)
    return _gravar_cache(buf.getvalue(), cache)


def _gravar_cache(xlsx: bytes, cache) -> bytes:
    """Insere <v> nas células de fórmula (sem LibreOffice) para o arquivo abrir com valores em qualquer visualizador."""
    zin = zipfile.ZipFile(io.BytesIO(xlsx))
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zout:
        for item in zin.infolist():
            dados = zin.read(item.filename)
            if item.filename == "xl/worksheets/sheet1.xml":
                xml = dados.decode("utf-8")

                def subs(m):
                    attrs, ref_col, ref_row, f = m.group(1), m.group(2), int(m.group(3)), m.group(4)
                    val = cache.get(ref_row, ("", "", ""))["NOP".index(ref_col)]
                    attrs = re.sub(r'\st="[^"]*"', "", attrs)
                    if isinstance(val, str):
                        return f'<c{attrs} t="str"><f>{f}</f><v>{val}</v></c>'
                    return f"<c{attrs}><f>{f}</f><v>{val!r}</v></c>"

                xml = re.sub(r'<c((?:\s[^>]*?)?\sr="([NOP])(\d+)"[^>]*)><f>(.*?)</f><v\s*/?>(?:</v>)?</c>', subs, xml)
                dados = xml.encode("utf-8")
            zout.writestr(item, dados)
    return out.getvalue()
