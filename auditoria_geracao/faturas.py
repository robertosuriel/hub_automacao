"""Fatura da geradora: acha o PDF pela planilha de controle + Drive e extrai período e injetado."""
import io
import re
import unicodedata
from datetime import datetime

from .cadastro import Usina
from .fatura_parser import extrai
from .pdftexto import pdf_para_texto

SPREADSHEET_ID = "1Ut5Y0LstIP7nhv7Jzyywc7SS7ObIPlO-3yEg-J8Pp5o"
ABAS_CONTROLE = [
    "Controle_BlueSolutions_Automação", "Controle_Criatech_Automação", "Controle_NCA_Automação",
    "Controle_GM_Automação", "Controle_RM_Automação", "Controle_DNA_Automação", "Controle_SoftDados_Automação",
    "Controle_SoftComp_Automação", "Controle_PRIMO_Automação", "Controle_DanielMatos_Automação",
]


def _norm(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s).replace("codigodocliente", "codigocliente")


def carregar_controle(client_sheets, abas=ABAS_CONTROLE):
    """Lê todas as abas de controle e devolve linhas {codigo_cliente, mesReferencia, numeroFatura, link, aba}."""
    planilha = client_sheets.open_by_key(SPREADSHEET_ID)
    linhas = []
    for nome in abas:
        try:
            valores = planilha.worksheet(nome).get_all_values()
        except Exception:
            continue
        if not valores:
            continue
        cab = {_norm(h): i for i, h in enumerate(valores[0])}
        ic, im, inf, il = (cab.get(k) for k in ("codigocliente", "mesreferencia", "numerofatura", "linkfatura"))
        if ic is None or im is None:
            continue
        for r in valores[1:]:
            if len(r) <= max(x for x in (ic, im) if x is not None):
                continue
            linhas.append(dict(
                codigo_cliente=re.sub(r"\D", "", r[ic]), mesReferencia=r[im].strip(),
                numeroFatura=r[inf].strip() if inf is not None and len(r) > inf else "",
                link=r[il].strip() if il is not None and len(r) > il else "", aba=nome))
    return linhas


def _id_drive(link):
    m = re.search(r"(?:/d/|id=)([\w-]{20,})", link or "")
    return m.group(1) if m else None


def baixar_pdf(drive, file_id) -> bytes:
    from googleapiclient.http import MediaIoBaseDownload
    buf = io.BytesIO()
    dl = MediaIoBaseDownload(buf, drive.files().get_media(fileId=file_id, supportsAllDrives=True))
    pronto = False
    while not pronto:
        _, pronto = dl.next_chunk()
    return buf.getvalue()


def mes_referencia_da_usina(usina: Usina, ano: int, mes: int) -> str:
    """Riachão usa a fatura do mês seguinte (ciclo começa no início do mês)."""
    if usina.mes_ref_seguinte:
        ano, mes = (ano + 1, 1) if mes == 12 else (ano, mes + 1)
    return f"{ano}/{mes:02d}"


def _dt(s):
    return datetime.strptime(s, "%d/%m/%Y").date() if s else None


def obter_fatura(usina: Usina, ano: int, mes: int, controle, drive):
    """Devolve (dados, avisos). dados tem leit_ant, leit_atu, dias, prox, injetado, mesReferencia."""
    ref = mes_referencia_da_usina(usina, ano, mes)
    avisos = []
    cand = [l for l in controle if l["codigo_cliente"].lstrip("0") == usina.codigo_cliente and l["mesReferencia"] == ref]
    if not cand:
        return None, [f"{usina.nome}: sem fatura {ref} na planilha de controle"]
    linha = next((c for c in cand if c["link"]), cand[0])
    fid = _id_drive(linha["link"])
    if not fid:
        return None, [f"{usina.nome}: fatura {ref} sem Link_Fatura"]
    meta = drive.files().get(fileId=fid, fields="name", supportsAllDrives=True).execute()
    nome = meta.get("name", "")
    # valida que o PDF é da conta certa (ex.: Itaguaçu 6 tinha link da conta 7099570690)
    m = re.match(r"\d{4}/\d{2}_0*(\d+)_", nome.replace("-", "/", 1)) or re.search(r"_0*(\d{10})_", nome)
    if m and m.group(1) != usina.codigo_cliente:
        avisos.append(f"{usina.nome}: o link da fatura {ref} abre o PDF da conta {m.group(1)} ({nome}); "
                      f"procurando o arquivo certo no Drive")
        achado = _procurar_no_drive(drive, ref, usina.codigo_cliente, linha["numeroFatura"])
        if not achado:
            return None, avisos + [f"{usina.nome}: PDF correto não encontrado no Drive"]
        fid = achado
    d = extrai(pdf_para_texto(baixar_pdf(drive, fid)))
    if d.get("uc") and d["uc"] != usina.nuc:
        avisos.append(f"{usina.nome}: NUC da fatura ({d['uc']}) difere do cadastro ({usina.nuc})")
    if not d.get("leit_ant_data"):
        return None, avisos + [f"{usina.nome}: não consegui ler o período de leitura da fatura {ref}"]
    if d.get("injetado_kwh") is None:
        avisos.append(f"{usina.nome}: a fatura {ref} veio sem o bloco MMGD de energia injetada")
    return dict(
        mesReferencia=ref, leit_ant=_dt(d["leit_ant_data"]), leit_atu=_dt(d["leit_atu_data"]), dias=d["dias"],
        prox=_dt(d.get("prox_leitura")), injetado=d.get("injetado_kwh"), uc=d.get("uc"), medidor=d.get("medidor"),
    ), avisos


def _procurar_no_drive(drive, ref, codigo, numero_fatura):
    q = f"name contains '{ref}_' and name contains '{codigo}' and trashed = false"
    r = drive.files().list(q=q, fields="files(id,name)", includeItemsFromAllDrives=True,
                           supportsAllDrives=True).execute()
    for f in r.get("files", []):
        if not numero_fatura or numero_fatura in f["name"]:
            return f["id"]
    return r["files"][0]["id"] if r.get("files") else None
