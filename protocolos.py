import os
import sys
import time
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from email.mime.text import MIMEText

import pandas as pd
import requests
from dotenv import load_dotenv
from selenium.webdriver.support.ui import WebDriverWait

from google.oauth2 import service_account
from googleapiclient.discovery import build

from extrator import configurar_driver, realizar_login_selenium_original, autenticar_google_sheets

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# --- CONFIGURAÇÃO INICIAL ---

if getattr(sys, 'frozen', False):
    base_path = sys._MEIPASS
else:
    base_path = os.path.dirname(os.path.abspath(__file__))

env_path = os.path.join(base_path, ".env")
load_dotenv(env_path)

credentials_path = os.path.join(base_path, "credentials.json")

PROTOCOLOS_SPREADSHEET_ID = "1_tGDCBzQ_7Swwf4XKfWsU1mv0XHNRlB3jEZYpLaN-yk"
ABA_PROTOCOLOS = "Protocolos"

# Colunas da aba "Protocolos" (nessa ordem, A..W):
# Cliente | CNPJ | UC (referencia) | No da Solicitacao | Tipo | Descricao |
# Data da Solicitacao | Situacao | Prazo | Ultima Atualizacao | Alerta Enviado |
# Descoberto em | Observacoes | Resultado | Historico | Endereco |
# Instancia | Protocolo origem | Resolvido | Situacao interna | Observacoes internas |
# Dias p/ prazo | Historico Prazo
#
# IMPORTANTE sobre a coluna "UC (referencia)": o pedidos-header NAO e por UC
# (confirmado: a mesma lista de ate 30 solicitacoes recentes volta
# independente de qual UC valida da conta voce passa). Entao essa coluna guarda
# so a UC usada na chamada, nao necessariamente a UC "dona" da solicitacao.
# O campo confiavel pra identificar o imovel e o Endereco (coluna P).

# Colunas Q..V (0-indexed 16..21), editadas manualmente pela equipe nas abas
# por cliente. O robo le essas colunas ANTES de sobrescrever a aba do cliente,
# pra nao perder edicao.
# Coluna W (0-indexed 22): Historico Prazo — preenchida automaticamente pelo robo
# quando o prazo muda (formato: "[data] Prazo: antigo → novo").
COLUNAS_MANUAIS_INDICE = {"instancia": 16, "protocolo_origem": 17, "resolvido": 18, "situacao_interna": 19, "observacoes_internas": 20}
COLUNA_HISTORICO_PRAZO = 22  # W

COR_HEADER = {"red": 0.043, "green": 0.42, "blue": 0.278}  # #0B6B47
COR_BANDA_PAR = {"red": 0.933, "green": 0.965, "blue": 0.945}
COR_BORDA = {"color": {"red": 0.85, "green": 0.85, "blue": 0.85}, "style": "SOLID"}
LARGURAS_COLUNAS = {
    0: 110, 1: 120, 2: 110, 3: 130, 4: 150, 5: 170, 6: 100, 7: 110, 8: 100,
    9: 130, 10: 100, 11: 130, 12: 160, 13: 260, 14: 280, 15: 260, 16: 80,
    17: 140, 18: 90, 19: 170, 20: 260, 21: 100, 22: 280,
}
TOTAL_COLUNAS = 23  # A..W


def formula_dias_prazo(linha):
    """Fórmula da coluna V (Dias p/ prazo). A planilha tem locale en_US
    (M/D/YYYY), mas gravamos Prazo como texto DD/MM/YYYY — se a gente deixar
    o Sheets 'adivinhar' o formato, ele não reconhece como data e a subtração
    direta (I-TODAY()) quebra com #VALUE!. Por isso parseia o texto na mão
    (dia/mês/ano) em vez de depender do Sheets reconhecer a data sozinho —
    funciona com qualquer locale."""
    return (
        f'=IF(OR(H{linha}="Finalizada",I{linha}="",I{linha}="00/00/0000"),"",'
        f'IFERROR(DATE(VALUE(RIGHT(I{linha},4)),VALUE(MID(I{linha},4,2)),VALUE(LEFT(I{linha},2)))-TODAY(),""))'
    )


# --- APIs da Agência Virtual (mesmo backend usado pelo extrator.py pra faturas) ---

def obter_ucs_cliente(login_user, token_ne_se):
    headers_api = {"User-Agent": "Mozilla/5.0", "Authorization": "Bearer " + token_ne_se}
    try:
        r = requests.get(
            f"https://apineprd.neoenergia.com/imoveis/1.1.0/clientes/{login_user}/ucs",
            params={
                "documento": login_user, "canalSolicitante": "AGC", "distribuidora": "COELBA",
                "usuario": "WSO2_CONEXAO", "indMaisUcs": "X", "tipoPerfil": "1",
            },
            headers=headers_api, timeout=30,
        )
        return [uc['uc'] for uc in r.json().get("ucs", [])]
    except Exception as e:
        print(f"  ⚠️ Erro ao obter UCs: {e}")
        return []


def obter_protocolo_uc(login_user, uc_codigo, token_ne_se):
    """Gera um 'protocolo' de sessão (registro Salesforce, válido pra conta
    inteira, não só a UC usada aqui). ATENÇÃO: cada chamada deste endpoint cria
    um registro "Contato" que entra na própria lista do pedidos-header —
    chame só uma vez por execução (nunca em loop por UC)."""
    headers_api = {"User-Agent": "Mozilla/5.0", "Authorization": "Bearer " + token_ne_se}
    try:
        r = requests.get(
            "https://apineprd.neoenergia.com/protocolo/1.1.0/obterProtocolo",
            params={
                "distribuidora": "COEL", "canalSolicitante": "AGC", "documento": login_user,
                "codCliente": uc_codigo, "recaptchaAnl": "true", "regiao": "NE",
            },
            headers=headers_api, timeout=30,
        )
        return r.json().get("protocoloLegado")
    except Exception as e:
        print(f"  ⚠️ Erro ao obter protocolo: {e}")
        return None


def consultar_solicitacoes_conta(login_user, uc_referencia, protocolo, token_ne_se):
    """pedidos-header retorna a lista da CONTA inteira (confirmado: UCs
    diferentes retornam a mesma lista item a item), limitada aos ~30 itens
    mais recentes, misturando solicitações reais com registros de "Contato"
    (estrutura "P", gerados a cada obterProtocolo). Só precisa ser chamado
    uma vez por execução, com qualquer UC válida da conta.

    Não captura exceção aqui de propósito: deixa propagar pro chamador
    registrar o erro, em vez de engolir silenciosamente."""
    headers_api = {"User-Agent": "Mozilla/5.0", "Authorization": "Bearer " + token_ne_se, "Accept": "application/json"}
    r = requests.get(
        f"https://apineprd.neoenergia.com/multilogin/2.0.0/servicos/acompanhamento-solicitacoes/{login_user}/pedidos-header",
        params={
            "regiao": "NE", "distribuidora": "COELBA", "codigo": uc_referencia, "protocolo": protocolo,
            "tipificacao": "1011601", "documentoSolicitante": login_user,
            "canalSolicitante": "AGC", "usuario": "WSO2_CONEXAO", "tipoPerfil": "1",
        },
        headers=headers_api, timeout=30,
    )
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
    corpo = r.json()
    if corpo.get("retorno", {}).get("tipo") == "E":
        raise RuntimeError(f"Erro retornado pela API: {corpo.get('retorno', {}).get('mensagem')}")
    dados = corpo.get("solicitacoes", []) or []
    # "estrutura": "P" são registros de contato/log gerados a cada consulta de
    # protocolo (tipo "Contato"), não são reclamações/solicitações reais.
    return [s for s in dados if s.get("estrutura") == "N"]


def buscar_detalhe_solicitacao(login_user, uc_referencia, numero, protocolo, token_ne_se):
    """Detalhe completo de uma solicitação (situação atual, endereço, prazo,
    última atualização real e histórico, incluindo o resultado quando
    finalizada). Funciona com qualquer UC válida da conta (não precisa ser a
    UC "dona" da solicitação — confirmado). O tempo de resposta varia bastante
    (observado entre ~1,5s e ~33s), por isso o timeout é mais folgado aqui."""
    headers_api = {"User-Agent": "Mozilla/5.0", "Authorization": "Bearer " + token_ne_se, "Accept": "application/json"}
    try:
        r = requests.get(
            f"https://apineprd.neoenergia.com/multilogin/2.0.0/servicos/acompanhamento-solicitacoes/{numero}/pedidos-detalhe",
            params={
                "documento": login_user, "regiao": "NE", "distribuidora": "COELBA",
                "tipoEstrutura": "N", "protocolo": protocolo, "tipificacao": "1010602",
                "documentoSolicitante": login_user, "canalSolicitante": "AGC",
                "usuario": "WSO2_CONEXAO", "codigo": uc_referencia, "protocoloSolicitacao": numero,
                "tipoPerfil": "1", "geraSsOs": "S",
            },
            headers=headers_api, timeout=45,
        )
        if r.status_code != 200:
            print(f"  ⚠️ Detalhe da solicitação {numero}: HTTP {r.status_code}")
            return {}
        dados = r.json()
        if dados.get("retorno", {}).get("tipo") == "E":
            print(f"  ⚠️ Detalhe da solicitação {numero}: {dados.get('retorno', {}).get('mensagem')}")
            return {}

        informacoes = (dados.get("informacoes") or [{}])[0]
        timeline = dados.get("acompanhamentoDetalhado") or []
        resultado = ""
        if timeline and timeline[-1].get("textoFluxo"):
            resultado = timeline[-1]["textoFluxo"]

        linhas_historico = []
        for evento in timeline:
            status = evento.get("statusFluxo", "")
            data_evento = evento.get("dataFluxo", "")
            hora_evento = evento.get("horaFluxo", "")
            texto = evento.get("textoFluxo")
            linha = f"{data_evento} {hora_evento} — {status}".strip()
            if texto:
                linha += f": {texto}"
            linhas_historico.append(linha)
        historico = "\n".join(linhas_historico)

        return {
            "situacao_atual": dados.get("situacao", ""),
            "descricao": informacoes.get("descricaoCodificacao", ""),
            "endereco": informacoes.get("enderecoImovel", ""),
            "prazo": informacoes.get("conclusaoDesejada", ""),
            "ultima_atualizacao": informacoes.get("ultimaAtualizacao", ""),
            "resultado": resultado,
            "historico": historico,
        }
    except Exception as e:
        print(f"  ⚠️ Erro ao buscar detalhe da solicitação {numero}: {e}")
        return {}


# --- GOOGLE SHEETS: LEITURA/ESCRITA DA ABA "Protocolos" ---

def _abrir_aba_protocolos():
    client = autenticar_google_sheets()
    planilha = client.open_by_key(PROTOCOLOS_SPREADSHEET_ID)
    return planilha.worksheet(ABA_PROTOCOLOS)


def aplicar_validacao_controle(aba, linha_inicio, linha_fim):
    """Checkbox (Resolvido, col S) e dropdown (Situacao interna, col T) só nas
    linhas indicadas. NUNCA aplicar isso adiantado em linhas sem dado — o
    Sheets grava 'FALSE' de verdade em célula de checkbox vazia, e isso já
    confundiu a detecção de fim de tabela do Sheets uma vez (linhas novas
    caindo centenas de linhas abaixo do esperado)."""
    try:
        client = autenticar_google_sheets()
        planilha = client.open_by_key(PROTOCOLOS_SPREADSHEET_ID)
        sheet_id = aba.id
        planilha.batch_update({"requests": [
            {
                "setDataValidation": {
                    "range": {"sheetId": sheet_id, "startRowIndex": linha_inicio - 1, "endRowIndex": linha_fim, "startColumnIndex": 18, "endColumnIndex": 19},
                    "rule": {"condition": {"type": "BOOLEAN"}, "strict": True},
                }
            },
            {
                "setDataValidation": {
                    "range": {"sheetId": sheet_id, "startRowIndex": linha_inicio - 1, "endRowIndex": linha_fim, "startColumnIndex": 19, "endColumnIndex": 20},
                    "rule": {
                        "condition": {
                            "type": "ONE_OF_LIST",
                            "values": [
                                {"userEnteredValue": "-"},
                                {"userEnteredValue": "Aguardando Coelba"},
                                {"userEnteredValue": "Aguardando cliente"},
                                {"userEnteredValue": "Insatisfeito - abrir nova"},
                                {"userEnteredValue": "Escalar p/ ANEEL"},
                                {"userEnteredValue": "Resolvido"},
                            ],
                        },
                        "showCustomUi": True,
                        "strict": True,
                    },
                }
            },
        ]})
    except Exception as e:
        print(f"  ⚠️ Não consegui aplicar validação de controle nas linhas {linha_inicio}-{linha_fim}: {e}")


def _estilizar_aba(aba, total_linhas, congelar_colunas=4):
    """Aplica o mesmo visual da aba 'Protocolos' (cabeçalho verde, zebra,
    bordas, larguras, filtro nativo) numa aba qualquer. 'total_linhas' deve
    ser a quantidade real de linhas com dado (nunca mais que isso, pelo
    mesmo motivo do aplicar_validacao_controle)."""
    try:
        client = autenticar_google_sheets()
        planilha = client.open_by_key(PROTOCOLOS_SPREADSHEET_ID)
        sheet_id = aba.id
        fim = max(total_linhas, 2)

        requests_batch = []
        # Remove zebra já existente (reaplicar sem remover dá erro "already
        # has alternating background colors" — isso roda de novo a cada
        # regeneração da aba, então precisa ser idempotente).
        meta = planilha.fetch_sheet_metadata()
        for sheet in meta.get("sheets", []):
            if sheet.get("properties", {}).get("sheetId") == sheet_id:
                for banda in sheet.get("bandedRanges", []):
                    requests_batch.append({"deleteBanding": {"bandedRangeId": banda["bandedRangeId"]}})
                break

        requests_batch += [
            {"updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1, "frozenColumnCount": congelar_colunas}},
                "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount",
            }},
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": TOTAL_COLUNAS},
                "cell": {"userEnteredFormat": {
                    "backgroundColor": COR_HEADER,
                    "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
                    "verticalAlignment": "MIDDLE",
                }},
                "fields": "userEnteredFormat(backgroundColor,textFormat,verticalAlignment)",
            }},
            {"updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 0, "endIndex": 1},
                "properties": {"pixelSize": 32}, "fields": "pixelSize",
            }},
            {"addBanding": {"bandedRange": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": fim, "startColumnIndex": 0, "endColumnIndex": TOTAL_COLUNAS},
                "rowProperties": {"headerColor": COR_HEADER, "firstBandColor": {"red": 1, "green": 1, "blue": 1}, "secondBandColor": COR_BANDA_PAR},
            }}},
            {"setBasicFilter": {"filter": {"range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": fim, "startColumnIndex": 0, "endColumnIndex": TOTAL_COLUNAS}}}},
            {"updateBorders": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": fim, "startColumnIndex": 0, "endColumnIndex": TOTAL_COLUNAS},
                "top": COR_BORDA, "bottom": COR_BORDA, "left": COR_BORDA, "right": COR_BORDA,
                "innerHorizontal": COR_BORDA, "innerVertical": COR_BORDA,
            }},
        ]
        for idx, largura in LARGURAS_COLUNAS.items():
            requests_batch.append({"updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": idx, "endIndex": idx + 1},
                "properties": {"pixelSize": largura}, "fields": "pixelSize",
            }})
        planilha.batch_update({"requests": requests_batch})
    except Exception as e:
        print(f"  ⚠️ Não consegui estilizar a aba '{aba.title}': {e}")


def obter_ou_criar_aba_cliente(planilha, cliente, cabecalho):
    nome = cliente.upper()
    for ws in planilha.worksheets():
        if ws.title == nome:
            return ws
    aba_nova = planilha.add_worksheet(title=nome, rows=200, cols=len(cabecalho))
    aba_nova.update(range_name="A1", values=[cabecalho], value_input_option="RAW")
    return aba_nova


def sincronizar_edicoes_do_cliente(aba_protocolos, planilha, cliente):
    """Lê a aba do cliente (se existir) e aplica na aba 'Protocolos' qualquer
    edição manual feita lá (Instancia, Protocolo origem, Resolvido, Situacao
    interna, Observacoes internas) — ANTES de regenerar a aba do cliente, pra
    não perder o que a equipe editou desde a última rodada."""
    nome = cliente.upper()
    aba_cliente = None
    for ws in planilha.worksheets():
        if ws.title == nome:
            aba_cliente = ws
            break
    if aba_cliente is None:
        return

    valores_cliente = aba_cliente.get_all_values()
    if len(valores_cliente) < 2:
        return

    valores_protocolos = aba_protocolos.get_all_values()
    linha_por_numero = {
        linha[3].strip(): i + 1
        for i, linha in enumerate(valores_protocolos)
        if i > 0 and len(linha) > 3 and linha[0].strip() == nome and linha[3].strip()
    }

    updates = []
    for linha in valores_cliente[1:]:
        if len(linha) <= COLUNAS_MANUAIS_INDICE["observacoes_internas"]:
            continue
        numero = linha[3].strip() if len(linha) > 3 else ""
        linha_protocolos = linha_por_numero.get(numero)
        if not linha_protocolos:
            continue
        valores_manuais = [linha[idx] for idx in range(16, 21)]  # Q..U
        atual = valores_protocolos[linha_protocolos - 1]
        atual_manuais = [atual[idx] if len(atual) > idx else "" for idx in range(16, 21)]
        if valores_manuais != atual_manuais:
            updates.append({"range": f"Q{linha_protocolos}:U{linha_protocolos}", "values": [valores_manuais]})

    if updates:
        aba_protocolos.batch_update(updates, value_input_option="USER_ENTERED")
        print(f"  [{cliente}] {len(updates)} edição(ões) manual(is) sincronizada(s) da aba do cliente.")


def regenerar_aba_cliente(aba_protocolos, planilha, cliente):
    """Reescreve a aba do cliente do zero com o estado atual (pós-sincronização
    e pós-atualização) da aba 'Protocolos', filtrado pra esse cliente."""
    nome = cliente.upper()
    valores = aba_protocolos.get_all_values()
    if not valores:
        return
    cabecalho = valores[0]
    linhas_cliente = [linha for linha in valores[1:] if linha and linha[0].strip() == nome]

    aba_cliente = obter_ou_criar_aba_cliente(planilha, cliente, cabecalho)
    total_linhas_atuais = len(aba_cliente.get_all_values())
    if total_linhas_atuais > 1:
        aba_cliente.batch_clear([f"A2:W{total_linhas_atuais}"])
    aba_cliente.update(range_name="A1", values=[cabecalho], value_input_option="RAW")
    if linhas_cliente:
        aba_cliente.update(range_name="A2", values=linhas_cliente, value_input_option="USER_ENTERED")

    _estilizar_aba(aba_cliente, len(linhas_cliente) + 1)
    if linhas_cliente:
        aplicar_validacao_controle(aba_cliente, 2, len(linhas_cliente) + 1)


def carregar_index_protocolos(aba):
    """Retorna { (cliente, numero): {'linha': int, 'situacao': str, 'alerta_enviado': str,
    'prazo': str, 'ultima_atualizacao': str, 'historico_prazo': str} }"""
    valores = aba.get_all_values()
    index = {}
    for i, linha in enumerate(valores):
        if i == 0:
            continue  # cabeçalho
        if len(linha) < 11:
            continue
        cliente = linha[0].strip()
        numero = linha[3].strip()
        if not cliente or not numero:
            continue
        index[(cliente, numero)] = {
            "linha": i + 1,
            "situacao": linha[7].strip(),
            "prazo": linha[8].strip() if len(linha) > 8 else "",
            "ultima_atualizacao": linha[9].strip() if len(linha) > 9 else "",
            "alerta_enviado": linha[10].strip(),
            "historico_prazo": linha[COLUNA_HISTORICO_PRAZO].strip() if len(linha) > COLUNA_HISTORICO_PRAZO else "",
        }
    return index, len(valores)


def carregar_dataframe_protocolos():
    """Lê a aba 'Protocolos' pronta pra exibir no painel do Streamlit."""
    aba = _abrir_aba_protocolos()
    valores = aba.get_all_values()
    if len(valores) < 2:
        return pd.DataFrame()
    cabecalho = valores[0]
    linhas = [
        linha + [""] * (len(cabecalho) - len(linha))
        for linha in valores[1:]
        if linha and len(linha) > 3 and linha[3].strip()
    ]
    df = pd.DataFrame(linhas, columns=cabecalho)
    if "Dias p/ prazo" in df.columns:
        df["Dias p/ prazo"] = pd.to_numeric(df["Dias p/ prazo"], errors="coerce")
    return df


# --- E-MAIL DE ALERTA (Gmail API com domain-wide delegation) ---
# Requer: Gmail API habilitada + delegação em todo o domínio autorizada no Workspace
# (escopo https://www.googleapis.com/auth/gmail.send) para a conta de serviço,
# além das variáveis ALERTA_EMAIL_REMETENTE e ALERTA_EMAIL_DESTINATARIOS no .env/secrets.
# Enquanto isso não estiver configurado, o alerta continua funcionando só pela planilha.

GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]


def _montar_servico_gmail():
    remetente = os.getenv("ALERTA_EMAIL_REMETENTE")
    if not remetente:
        return None, None
    try:
        creds = service_account.Credentials.from_service_account_file(
            credentials_path, scopes=GMAIL_SCOPES, subject=remetente
        )
        servico = build("gmail", "v1", credentials=creds)
        return servico, remetente
    except Exception as e:
        print(f"  ⚠️ Gmail API indisponível ({e}). Alerta fica só na planilha.")
        return None, None


def enviar_alerta_email(cliente, numero_solicitacao, tipo):
    servico, remetente = _montar_servico_gmail()
    if not servico:
        return False

    destinatarios = os.getenv("ALERTA_EMAIL_DESTINATARIOS", "")
    if not destinatarios:
        print("  ⚠️ ALERTA_EMAIL_DESTINATARIOS não configurado.")
        return False

    try:
        corpo = (
            f"A solicitação {numero_solicitacao} ({tipo}) do cliente {cliente.upper()} "
            f"foi finalizada na Coelba.\n\n"
            f"Planilha: https://docs.google.com/spreadsheets/d/{PROTOCOLOS_SPREADSHEET_ID}/edit"
        )
        msg = MIMEText(corpo)
        msg["to"] = destinatarios
        msg["from"] = remetente
        msg["subject"] = f"[Hub Automação] Solicitação {numero_solicitacao} finalizada - {cliente.upper()}"
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        servico.users().messages().send(userId="me", body={"raw": raw}).execute()
        return True
    except Exception as e:
        print(f"  ⚠️ Falha ao enviar e-mail de alerta: {e}")
        return False


# --- PROCESSAMENTO POR CLIENTE ---

def processar_protocolos_cliente(cliente, login_user, login_password):
    aba = _abrir_aba_protocolos()
    client_sheets = autenticar_google_sheets()
    planilha = client_sheets.open_by_key(PROTOCOLOS_SPREADSHEET_ID)

    # Antes de mexer em qualquer coisa: traz pra "Protocolos" o que a equipe
    # editou manualmente na aba desse cliente desde a última rodada.
    sincronizar_edicoes_do_cliente(aba, planilha, cliente)

    index, total_linhas = carregar_index_protocolos(aba)
    proxima_linha = total_linhas + 1

    novos = 0
    atualizados = 0
    alertas = 0
    driver = None

    try:
        driver = configurar_driver()
        driver.get("https://agenciavirtual.neoenergia.com/#/login")
        time.sleep(4)
        WebDriverWait(driver, 30).until(lambda d: d.execute_script("return document.readyState") == "complete")

        bearer_token = realizar_login_selenium_original(driver, login_user, login_password, f"protocolo_{cliente}")
        if not bearer_token:
            print(f"  ❌ [{cliente}] Falha no login.")
            return False, novos, atualizados, alertas

        token_ne_se = bearer_token.split(":")[1].split(",")[0].strip(' "{}')
        print(f"  ✅ [{cliente}] Login realizado!")
    finally:
        if driver:
            driver.quit()

    try:
        ucs = obter_ucs_cliente(login_user, token_ne_se)
        if not ucs:
            print(f"  ⚠️ [{cliente}] Nenhuma UC encontrada.")
            return False, novos, atualizados, alertas
        uc_referencia = ucs[0]

        protocolo = obter_protocolo_uc(login_user, uc_referencia, token_ne_se)
        if not protocolo:
            print(f"  ⚠️ [{cliente}] Não consegui obter protocolo de sessão.")
            return False, novos, atualizados, alertas

        # 1. Descobre solicitações novas na lista recente da conta (uma chamada só).
        try:
            solicitacoes_recentes = consultar_solicitacoes_conta(login_user, uc_referencia, protocolo, token_ne_se)
        except Exception as e:
            print(f"  ⚠️ [{cliente}] Erro ao consultar lista de solicitações: {e}")
            solicitacoes_recentes = []

        novas_linhas_buffer = []
        atualizacoes_buffer = []  # (linha, descricao, [situacao, prazo, ultima_atualizacao, alerta_enviado], resultado, historico, endereco, historico_prazo)
        agora = datetime.now().strftime("%d/%m/%Y %H:%M")
        vistos_nesta_rodada = set()
        # Protocolos abertos na lista recente que não mudaram de status —
        # serão checados via detalhe depois pra detectar mudança de prazo.
        abertos_sem_mudanca_status = []

        for s in solicitacoes_recentes:
            numero = str(s.get("numSolicitacao", "")).strip()
            tipo = s.get("tipoSolicitacao", "")
            data_solicitacao = s.get("datSolicitacao", "")
            situacao = s.get("statusSolicitacao", "")
            if not numero:
                continue
            vistos_nesta_rodada.add(numero)

            chave = (cliente.upper(), numero)
            registro = index.get(chave)

            if registro is None:
                detalhe = buscar_detalhe_solicitacao(login_user, uc_referencia, numero, protocolo, token_ne_se)
                alerta_enviado = "NÃO"
                if situacao.lower() == "finalizada":
                    if enviar_alerta_email(cliente, numero, tipo):
                        alerta_enviado = "SIM"
                    alertas += 1
                linha_valores = [
                    cliente.upper(), login_user, uc_referencia, numero, tipo,
                    detalhe.get("descricao", ""), data_solicitacao, situacao,
                    detalhe.get("prazo", ""), detalhe.get("ultima_atualizacao", "") or agora,
                    alerta_enviado, agora, "", detalhe.get("resultado", ""), detalhe.get("historico", ""),
                    detalhe.get("endereco", ""),
                    # Q..V: Instancia, Protocolo origem, Resolvido, Situacao interna,
                    # Observacoes internas (controle manual) e Dias p/ prazo (formula,
                    # recalcula sozinha e já referencia a linha certa).
                    "1", "", "FALSE", "-", "",
                    formula_dias_prazo(proxima_linha),
                ]
                # Coluna W (Historico Prazo) — vazia pra solicitações novas
                linha_valores.append("")
                novas_linhas_buffer.append(linha_valores)
                index[chave] = {"linha": proxima_linha, "situacao": situacao, "alerta_enviado": alerta_enviado,
                                "prazo": detalhe.get("prazo", ""), "ultima_atualizacao": detalhe.get("ultima_atualizacao", ""),
                                "historico_prazo": ""}
                proxima_linha += 1
                novos += 1

            elif registro["situacao"] != situacao:
                detalhe = buscar_detalhe_solicitacao(login_user, uc_referencia, numero, protocolo, token_ne_se)
                alerta_enviado = registro["alerta_enviado"]
                if situacao.lower() == "finalizada" and alerta_enviado != "SIM":
                    if enviar_alerta_email(cliente, numero, tipo):
                        alerta_enviado = "SIM"
                    alertas += 1
                # Checa se o prazo também mudou junto com o status
                novo_prazo = detalhe.get("prazo", "")
                hist_prazo = registro.get("historico_prazo", "")
                if novo_prazo and novo_prazo != registro.get("prazo", ""):
                    log_prazo = f"[{agora}] Prazo alterado: {registro.get('prazo', '?')} → {novo_prazo}"
                    hist_prazo = f"{hist_prazo}\n{log_prazo}".strip() if hist_prazo else log_prazo
                atualizacoes_buffer.append((
                    registro["linha"],
                    detalhe.get("descricao", ""),
                    [situacao, novo_prazo, detalhe.get("ultima_atualizacao", "") or agora, alerta_enviado],
                    detalhe.get("resultado", ""),
                    detalhe.get("historico", ""),
                    detalhe.get("endereco", ""),
                    hist_prazo,
                ))
                index[chave]["situacao"] = situacao
                index[chave]["alerta_enviado"] = alerta_enviado
                index[chave]["prazo"] = novo_prazo
                index[chave]["historico_prazo"] = hist_prazo
                atualizados += 1

            elif situacao.strip().lower() != "finalizada":
                # Status não mudou mas protocolo ainda aberto — guardar pra
                # checar mudança de prazo/atualização via detalhe.
                abertos_sem_mudanca_status.append((chave, registro))

        # 2. Protocolos abertos que precisam checagem de detalhe:
        #    a) os da lista recente cujo status não mudou (podem ter prazo novo)
        #    b) os que saíram da janela de ~30 itens mais recentes
        abertas_fora_da_lista = [
            (chave, dados) for chave, dados in index.items()
            if chave[0] == cliente.upper()
            and dados["situacao"].strip().lower() != "finalizada"
            and chave[1] not in vistos_nesta_rodada
        ]
        todos_abertos_pra_checar = abertos_sem_mudanca_status + abertas_fora_da_lista
        if todos_abertos_pra_checar:
            print(f"  [{cliente}] Checando {len(todos_abertos_pra_checar)} solicitação(ões) aberta(s) por mudanças de prazo/status...")
            with ThreadPoolExecutor(max_workers=10) as executor:
                futuros = {
                    executor.submit(buscar_detalhe_solicitacao, login_user, uc_referencia, chave[1], protocolo, token_ne_se): (chave, dados)
                    for chave, dados in todos_abertos_pra_checar
                }
                for futuro in as_completed(futuros):
                    chave, dados_antigos = futuros[futuro]
                    numero = chave[1]
                    try:
                        detalhe = futuro.result()
                    except Exception as e:
                        print(f"  ⚠️ [{cliente}] Erro ao atualizar solicitação {numero}: {e}")
                        continue

                    nova_situacao = detalhe.get("situacao_atual", "")
                    novo_prazo = detalhe.get("prazo", "")
                    nova_ult_atualizacao = detalhe.get("ultima_atualizacao", "")
                    print(f"    🔍 [{cliente}] Sol {numero}: API prazo='{novo_prazo}' planilha prazo='{dados_antigos.get('prazo', '')}' | API ult_at='{nova_ult_atualizacao}' planilha ult_at='{dados_antigos.get('ultima_atualizacao', '')}' | API status='{nova_situacao}' planilha status='{dados_antigos['situacao']}'")

                    mudou_status = nova_situacao and nova_situacao != dados_antigos["situacao"]
                    mudou_prazo = novo_prazo and novo_prazo != dados_antigos.get("prazo", "")
                    mudou_atualizacao = nova_ult_atualizacao and nova_ult_atualizacao != dados_antigos.get("ultima_atualizacao", "")

                    if not mudou_status and not mudou_prazo and not mudou_atualizacao:
                        continue

                    alerta_enviado = dados_antigos["alerta_enviado"]
                    if nova_situacao and nova_situacao.lower() == "finalizada" and alerta_enviado != "SIM":
                        if enviar_alerta_email(cliente, numero, ""):
                            alerta_enviado = "SIM"
                        alertas += 1

                    # Registra mudança de prazo na coluna W (Hist. Prazo)
                    hist_prazo = dados_antigos.get("historico_prazo", "")
                    if mudou_prazo:
                        log_prazo = f"[{agora}] Prazo alterado: {dados_antigos.get('prazo', '?')} → {novo_prazo}"
                        hist_prazo = f"{hist_prazo}\n{log_prazo}".strip() if hist_prazo else log_prazo
                        print(f"  📅 [{cliente}] Solicitação {numero}: prazo alterado de {dados_antigos.get('prazo', '?')} para {novo_prazo}")

                    atualizacoes_buffer.append((
                        dados_antigos["linha"],
                        detalhe.get("descricao", ""),
                        [nova_situacao or dados_antigos["situacao"], novo_prazo, nova_ult_atualizacao or agora, alerta_enviado],
                        detalhe.get("resultado", ""),
                        detalhe.get("historico", ""),
                        detalhe.get("endereco", ""),
                        hist_prazo,
                    ))
                    if mudou_status:
                        index[chave]["situacao"] = nova_situacao
                    index[chave]["alerta_enviado"] = alerta_enviado
                    index[chave]["prazo"] = novo_prazo
                    index[chave]["historico_prazo"] = hist_prazo
                    atualizados += 1

        if novas_linhas_buffer:
            # Solicitações de nível conta podem já ter sido descobertas numa
            # execução concorrente/anterior — relê a planilha (estado real, não
            # o índice carregado no início da função) antes de gravar.
            chaves_existentes = {
                (linha[0].strip(), linha[3].strip())
                for linha in aba.get_all_values()[1:]
                if len(linha) > 3
            }
            antes = len(novas_linhas_buffer)
            novas_linhas_buffer = [
                linha for linha in novas_linhas_buffer
                if (linha[0], linha[3]) not in chaves_existentes
            ]
            evitadas = antes - len(novas_linhas_buffer)
            if evitadas:
                print(f"  ⚠️ [{cliente}] {evitadas} duplicata(s) evitada(s) na checagem final.")
                novos -= evitadas
            if novas_linhas_buffer:
                linha_inicio_novas = total_linhas + 1
                # table_range="A1" força o Sheets a procurar o fim da tabela a
                # partir do topo, em vez de confiar na autodetecção (já vimos
                # ela se confundir com validação de dados em células vazias e
                # inserir linhas novas centenas de linhas abaixo do esperado).
                aba.append_rows(novas_linhas_buffer, value_input_option="USER_ENTERED", table_range="A1")
                # Checkbox/dropdown só nas linhas que acabaram de ser escritas
                # (nunca adiantado — foi aplicar isso em massa, antes dos dados
                # existirem, que causou o bug da "tabela fantasma").
                aplicar_validacao_controle(aba, linha_inicio_novas, linha_inicio_novas + len(novas_linhas_buffer) - 1)
        if atualizacoes_buffer:
            updates = []
            for item in atualizacoes_buffer:
                linha, descricao, valores_hk, resultado, historico, endereco = item[:6]
                hist_prazo = item[6] if len(item) > 6 else ""
                if descricao:
                    updates.append({"range": f"F{linha}", "values": [[descricao]]})
                updates.append({"range": f"H{linha}:K{linha}", "values": [valores_hk]})
                if hist_prazo:
                    updates.append({"range": f"W{linha}", "values": [[hist_prazo]]})
                if resultado:
                    updates.append({"range": f"N{linha}", "values": [[resultado]]})
                if historico:
                    updates.append({"range": f"O{linha}", "values": [[historico]]})
                if endereco:
                    updates.append({"range": f"P{linha}", "values": [[endereco]]})
            aba.batch_update(updates, value_input_option="USER_ENTERED")

        regenerar_aba_cliente(aba, planilha, cliente)
        return True, novos, atualizados, alertas

    except Exception as e:
        print(f"  ❌ [{cliente}] Erro inesperado: {e}")
        return False, novos, atualizados, alertas


# --- FUNÇÃO PRINCIPAL CHAMADA PELO STREAMLIT ---

def processar_protocolos(clientes_login):
    """
    clientes_login: { 'blue': (login_user, login_password), ... }
    """
    resultados = {}
    for cliente, (login_user, login_password) in clientes_login.items():
        nome_maiusculo = cliente.upper()
        print(f"\n--- Consultando protocolos: {nome_maiusculo} ---")
        try:
            sucesso, novos, atualizados, alertas = processar_protocolos_cliente(cliente, login_user, login_password)
        except Exception as e:
            print(f"  ❌ [{cliente}] Erro fatal: {e}")
            sucesso, novos, atualizados, alertas = False, 0, 0, 0

        if sucesso:
            resultados[nome_maiusculo] = f"✅ {novos} nova(s), {atualizados} atualizada(s), {alertas} alerta(s)"
        else:
            resultados[nome_maiusculo] = "❌ Falha no login ou na consulta"

    return resultados
