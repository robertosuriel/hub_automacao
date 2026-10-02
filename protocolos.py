import os
import sys
import time
import base64
from datetime import datetime
from email.mime.text import MIMEText

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

# Colunas da aba "Protocolos" (nessa ordem, A..M):
# Cliente | CNPJ | UC / Codigo do Cliente | No da Solicitacao | Tipo | Descricao |
# Data da Solicitacao | Situacao | Prazo | Ultima Atualizacao | Alerta Enviado | Descoberto em | Observacoes


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
    """Mesmo endpoint que o extrator.py usa pra gerar o 'protocoloLegado' das faturas,
    aqui chamado por UC pra usar na consulta de solicitações dessa UC."""
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
        print(f"  ⚠️ Erro ao obter protocolo da UC {uc_codigo}: {e}")
        return None


def consultar_solicitacoes_uc(login_user, uc_codigo, protocolo, token_ne_se):
    """Endpoint real por trás da tela 'Acompanhe Suas Solicitações' (descoberto via
    DevTools). Evita ter que navegar o site com Selenium pra isso."""
    headers_api = {"User-Agent": "Mozilla/5.0", "Authorization": "Bearer " + token_ne_se, "Accept": "application/json"}
    try:
        r = requests.get(
            f"https://apineprd.neoenergia.com/multilogin/2.0.0/servicos/acompanhamento-solicitacoes/{login_user}/pedidos-header",
            params={
                "regiao": "NE", "distribuidora": "COELBA", "codigo": uc_codigo, "protocolo": protocolo,
                "tipificacao": "1011601", "documentoSolicitante": login_user,
                "canalSolicitante": "AGC", "usuario": "WSO2_CONEXAO", "tipoPerfil": "1",
            },
            headers=headers_api, timeout=30,
        )
        if r.status_code != 200:
            return []
        dados = r.json().get("solicitacoes", []) or []
        # "estrutura": "P" são registros de contato/log gerados a cada consulta à
        # página (tipo "Contato"), não são reclamações/solicitações reais.
        return [s for s in dados if s.get("estrutura") == "N"]
    except Exception as e:
        print(f"  ⚠️ Erro ao consultar solicitações da UC {uc_codigo}: {e}")
        return []


def buscar_detalhe_solicitacao(login_user, uc_codigo, numero, protocolo, token_ne_se):
    """Endpoint por trás do detalhe expandido de uma solicitação (prazo, última
    atualização real e o histórico completo, incluindo o resultado quando
    finalizada). Só vale a pena chamar pra solicitações novas ou que mudaram
    de situação (evita uma chamada extra por solicitação já conhecida)."""
    headers_api = {"User-Agent": "Mozilla/5.0", "Authorization": "Bearer " + token_ne_se, "Accept": "application/json"}
    try:
        r = requests.get(
            f"https://apineprd.neoenergia.com/multilogin/2.0.0/servicos/acompanhamento-solicitacoes/{numero}/pedidos-detalhe",
            params={
                "documento": login_user, "regiao": "NE", "distribuidora": "COELBA",
                "tipoEstrutura": "N", "protocolo": protocolo, "tipificacao": "1010602",
                "documentoSolicitante": login_user, "canalSolicitante": "AGC",
                "usuario": "WSO2_CONEXAO", "codigo": uc_codigo, "protocoloSolicitacao": numero,
                "tipoPerfil": "1", "geraSsOs": "S",
            },
            headers=headers_api, timeout=30,
        )
        if r.status_code != 200:
            return {}
        dados = r.json()
        informacoes = (dados.get("informacoes") or [{}])[0]
        timeline = dados.get("acompanhamentoDetalhado") or []
        resultado = ""
        if timeline and timeline[-1].get("textoFluxo"):
            resultado = timeline[-1]["textoFluxo"]
        return {
            "descricao": informacoes.get("descricaoCodificacao", ""),
            "prazo": informacoes.get("conclusaoDesejada", ""),
            "ultima_atualizacao": informacoes.get("ultimaAtualizacao", ""),
            "resultado": resultado,
        }
    except Exception as e:
        print(f"  ⚠️ Erro ao buscar detalhe da solicitação {numero}: {e}")
        return {}


# --- GOOGLE SHEETS: LEITURA/ESCRITA DA ABA "Protocolos" ---

def _abrir_aba_protocolos():
    client = autenticar_google_sheets()
    planilha = client.open_by_key(PROTOCOLOS_SPREADSHEET_ID)
    return planilha.worksheet(ABA_PROTOCOLOS)


def carregar_index_protocolos(aba):
    """Retorna { (cliente, numero): {'linha': int, 'situacao': str, 'alerta_enviado': str} }"""
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
            "alerta_enviado": linha[10].strip(),
        }
    return index, len(valores)


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


def enviar_alerta_email(cliente, numero_solicitacao, tipo, uc_codigo):
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
            f"(UC {uc_codigo}) foi finalizada na Coelba.\n\n"
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

        novas_linhas_buffer = []
        atualizacoes_buffer = []  # (linha, [situacao, prazo, ultima_atualizacao, alerta_enviado])

        for uc_codigo in ucs:
            print(f"  [{cliente}] Consultando solicitações da UC {uc_codigo}...")
            protocolo = obter_protocolo_uc(login_user, uc_codigo, token_ne_se)
            if not protocolo:
                continue

            solicitacoes = consultar_solicitacoes_uc(login_user, uc_codigo, protocolo, token_ne_se)
            agora = datetime.now().strftime("%d/%m/%Y %H:%M")

            for s in solicitacoes:
                numero = str(s.get("numSolicitacao", "")).strip()
                tipo = s.get("tipoSolicitacao", "")
                data_solicitacao = s.get("datSolicitacao", "")
                situacao = s.get("statusSolicitacao", "")
                if not numero:
                    continue

                chave = (cliente.upper(), numero)
                registro = index.get(chave)

                if registro is None:
                    detalhe = buscar_detalhe_solicitacao(login_user, uc_codigo, numero, protocolo, token_ne_se)
                    alerta_enviado = "NÃO"
                    if situacao.lower() == "finalizada":
                        if enviar_alerta_email(cliente, numero, tipo, uc_codigo):
                            alerta_enviado = "SIM"
                        alertas += 1
                    linha_valores = [
                        cliente.upper(), login_user, uc_codigo, numero, tipo,
                        detalhe.get("descricao", ""), data_solicitacao, situacao,
                        detalhe.get("prazo", ""), detalhe.get("ultima_atualizacao", "") or agora,
                        alerta_enviado, agora, "", detalhe.get("resultado", ""),
                    ]
                    novas_linhas_buffer.append(linha_valores)
                    index[chave] = {"linha": proxima_linha, "situacao": situacao, "alerta_enviado": alerta_enviado}
                    proxima_linha += 1
                    novos += 1

                elif registro["situacao"] != situacao:
                    detalhe = buscar_detalhe_solicitacao(login_user, uc_codigo, numero, protocolo, token_ne_se)
                    alerta_enviado = registro["alerta_enviado"]
                    if situacao.lower() == "finalizada" and alerta_enviado != "SIM":
                        if enviar_alerta_email(cliente, numero, tipo, uc_codigo):
                            alerta_enviado = "SIM"
                        alertas += 1
                    atualizacoes_buffer.append((
                        registro["linha"],
                        detalhe.get("descricao", ""),
                        [situacao, detalhe.get("prazo", ""), detalhe.get("ultima_atualizacao", "") or agora, alerta_enviado],
                        detalhe.get("resultado", ""),
                    ))
                    index[chave]["situacao"] = situacao
                    index[chave]["alerta_enviado"] = alerta_enviado
                    atualizados += 1

        if novas_linhas_buffer:
            aba.append_rows(novas_linhas_buffer, value_input_option="USER_ENTERED")
        if atualizacoes_buffer:
            updates = []
            for linha, descricao, valores_hk, resultado in atualizacoes_buffer:
                if descricao:
                    updates.append({"range": f"F{linha}", "values": [[descricao]]})
                updates.append({"range": f"H{linha}:K{linha}", "values": [valores_hk]})
                if resultado:
                    updates.append({"range": f"N{linha}", "values": [[resultado]]})
            aba.batch_update(updates, value_input_option="USER_ENTERED")

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
