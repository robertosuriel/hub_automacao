import os
import re
import sys
import time
import base64
from datetime import datetime
from email.mime.text import MIMEText

import requests
from dotenv import load_dotenv
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

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

# Prefixos de solicitação consultáveis pela Agência Virtual (ver "Consulta de Reclamações.pdf")
PREFIXOS_TIPO = [
    ("70000", "Reclamação de 1ª instância"),
    ("48", "Verificação de equipamento"),
    ("50", "Religação"),
    ("49", "Desligamento"),
    ("41", "Aferição de medidor"),
    ("42", "Restituição de valores"),
    ("45", "Ligação nova e reativação"),
]


def identificar_tipo(numero_solicitacao):
    numero = str(numero_solicitacao).strip()
    for prefixo, tipo in PREFIXOS_TIPO:
        if numero.startswith(prefixo):
            return tipo
    return "Outro"


# --- API: DESCOBERTA DE UCs (mesma API já usada em extrator.py) ---

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


# --- NAVEGAÇÃO SELENIUM (baseado nas telas de "Consulta de Reclamações.pdf") ---
# ATENÇÃO: seletores baseados nos textos visíveis das telas. Como não foi possível
# validar contra o site ao vivo (exige login com credencial real), é bem provável
# que alguns ajustes sejam necessários após o primeiro teste real. Em caso de erro,
# screenshot + HTML da página são salvos para facilitar o ajuste.

def _salvar_debug(driver, nome):
    try:
        driver.save_screenshot(f"erro_protocolo_{nome}.png")
        with open(f"erro_protocolo_{nome}.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
    except Exception:
        pass


def _clicar_por_texto(driver, texto, tags=("a", "button", "span", "div"), timeout=15):
    xpath = " | ".join(f'//{tag}[contains(normalize-space(.), "{texto}")]' for tag in tags)
    elemento = WebDriverWait(driver, timeout).until(
        EC.element_to_be_clickable((By.XPATH, xpath))
    )
    elemento.click()
    return elemento


def navegar_para_unidade(driver, uc_codigo, cliente):
    """Vai em 'Para a Unidade Consumidora', pesquisa a UC e entra nela."""
    try:
        _clicar_por_texto(driver, "Para a Unidade Consumidora", tags=("a", "span"))
        time.sleep(1)
        _clicar_por_texto(driver, "Minhas Unidades Consumidoras", tags=("a", "span"))
    except Exception:
        pass  # pode já estar na listagem de unidades

    try:
        campo_busca = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//input[contains(@placeholder, 'Código')]"))
        )
        campo_busca.clear()
        campo_busca.send_keys(uc_codigo)
        time.sleep(1)
        campo_busca.send_keys("\n")
        time.sleep(2)

        linha = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, f"//*[contains(text(), '{uc_codigo}')]/ancestor::*[self::div][1]"))
        )
        linha.click()
        time.sleep(2)
        return True
    except Exception as e:
        print(f"  ⚠️ [{cliente}] Não consegui abrir a UC {uc_codigo}: {e}")
        _salvar_debug(driver, f"{cliente}_{uc_codigo}_unidade")
        return False


def abrir_acompanhe_solicitacoes(driver, cliente, uc_codigo):
    try:
        _clicar_por_texto(driver, "Para Você", tags=("a", "span"))
        time.sleep(1)
        _clicar_por_texto(driver, "Acompanhe suas Solicitações", tags=("a", "span"))
        time.sleep(2)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, "//*[contains(text(), 'Acompanhe Suas Solicitações')]"))
        )
        return True
    except Exception as e:
        print(f"  ⚠️ [{cliente}] Não consegui abrir 'Acompanhe suas Solicitações' na UC {uc_codigo}: {e}")
        _salvar_debug(driver, f"{cliente}_{uc_codigo}_solicitacoes")
        return False


PADRAO_LISTAGEM = re.compile(
    r"N[ºo°] da Solicita[çc][ãa]o\s*(?P<numero>\S+)\s*"
    r"Descri[çc][ãa]o\s*(?P<descricao>.+?)\s*"
    r"Data da Solicita[çc][ãa]o\s*em\s*(?P<data>\d{2}/\d{2}/\d{4})\s*"
    r"Situa[çc][ãa]o\s*(?P<situacao>\S+)",
    re.IGNORECASE,
)


def parse_pagina_atual(driver):
    try:
        texto = driver.find_element(By.TAG_NAME, "body").text
    except Exception:
        return []
    return [m.groupdict() for m in PADRAO_LISTAGEM.finditer(texto)]


def ir_proxima_pagina(driver):
    try:
        botao = driver.find_element(By.XPATH, "//*[contains(text(), 'Próximo')]")
        classes = (botao.get_attribute("class") or "").lower()
        if "disabled" in classes or not botao.is_enabled():
            return False
        botao.click()
        time.sleep(2)
        return True
    except Exception:
        return False


def listar_todas_solicitacoes(driver, cliente, uc_codigo, max_paginas=20):
    solicitacoes = []
    vistos = set()
    for _ in range(max_paginas):
        pagina = parse_pagina_atual(driver)
        novas = [s for s in pagina if s["numero"] not in vistos]
        if not novas and vistos:
            break
        for s in novas:
            vistos.add(s["numero"])
        solicitacoes.extend(novas)
        if not ir_proxima_pagina(driver):
            break
    return solicitacoes


PADRAO_DETALHE_PRAZO = re.compile(r"Prazo\s*(\d{2}/\d{2}/\d{4})")
PADRAO_DETALHE_ATUALIZACAO = re.compile(r"[UÚ]ltima Atualiza[çc][ãa]o\s*(\d{2}/\d{2}/\d{4})")
PADRAO_DETALHE_PROTOCOLO = re.compile(r"Protocolo Principal\s*(\S+)")


def buscar_detalhe_solicitacao(driver, numero, cliente):
    """Usa o campo de pesquisa pra achar a solicitação e expande pra ler Prazo/Última Atualização."""
    detalhe = {"prazo": "", "ultima_atualizacao": "", "protocolo_principal": ""}
    try:
        campo = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, "//input[contains(@placeholder, 'número da solicitação')]"))
        )
        campo.clear()
        campo.send_keys(numero)
        time.sleep(1)
        campo.send_keys("\n")
        time.sleep(2)

        seta = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, f"//*[contains(text(), '{numero}')]/ancestor::*[self::div][1]//*[self::button or self::svg or self::i][last()]"))
        )
        seta.click()
        time.sleep(1.5)

        texto = driver.find_element(By.TAG_NAME, "body").text
        m_prazo = PADRAO_DETALHE_PRAZO.search(texto)
        m_atualizacao = PADRAO_DETALHE_ATUALIZACAO.search(texto)
        m_protocolo = PADRAO_DETALHE_PROTOCOLO.search(texto)
        if m_prazo:
            detalhe["prazo"] = m_prazo.group(1)
        if m_atualizacao:
            detalhe["ultima_atualizacao"] = m_atualizacao.group(1)
        if m_protocolo:
            detalhe["protocolo_principal"] = m_protocolo.group(1)
    except Exception as e:
        print(f"  ⚠️ [{cliente}] Não consegui abrir detalhe da solicitação {numero}: {e}")
        _salvar_debug(driver, f"{cliente}_detalhe_{numero}")
    return detalhe


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


def enviar_alerta_email(cliente, numero_solicitacao, descricao, uc_codigo):
    servico, remetente = _montar_servico_gmail()
    if not servico:
        return False

    destinatarios = os.getenv("ALERTA_EMAIL_DESTINATARIOS", "")
    if not destinatarios:
        print("  ⚠️ ALERTA_EMAIL_DESTINATARIOS não configurado.")
        return False

    try:
        corpo = (
            f"A solicitação {numero_solicitacao} ({descricao}) do cliente {cliente.upper()} "
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

    driver = configurar_driver()
    novos = 0
    atualizados = 0
    alertas = 0

    try:
        driver.get("https://agenciavirtual.neoenergia.com/#/login")
        time.sleep(4)
        WebDriverWait(driver, 30).until(lambda d: d.execute_script("return document.readyState") == "complete")

        bearer_token = realizar_login_selenium_original(driver, login_user, login_password, f"protocolo_{cliente}")
        if not bearer_token:
            print(f"  ❌ [{cliente}] Falha no login.")
            return False, novos, atualizados, alertas

        token_ne_se = bearer_token.split(":")[1].split(",")[0].strip(' "{}')
        print(f"  ✅ [{cliente}] Login realizado!")

        ucs = obter_ucs_cliente(login_user, token_ne_se)
        if not ucs:
            print(f"  ⚠️ [{cliente}] Nenhuma UC encontrada.")
            return False, novos, atualizados, alertas

        novas_linhas_buffer = []
        atualizacoes_buffer = []  # (linha, [situacao, prazo, ultima_atualizacao, alerta_enviado])

        for uc_codigo in ucs:
            print(f"  [{cliente}] Consultando solicitações da UC {uc_codigo}...")
            if not navegar_para_unidade(driver, uc_codigo, cliente):
                continue
            if not abrir_acompanhe_solicitacoes(driver, cliente, uc_codigo):
                continue

            solicitacoes = listar_todas_solicitacoes(driver, cliente, uc_codigo)
            for s in solicitacoes:
                numero = s["numero"]
                situacao = s["situacao"]
                descricao = s["descricao"]
                data_solicitacao = s["data"]
                chave = (cliente.upper(), numero)
                registro = index.get(chave)

                if registro is None:
                    detalhe = buscar_detalhe_solicitacao(driver, numero, cliente)
                    agora = datetime.now().strftime("%d/%m/%Y %H:%M")
                    alerta_enviado = "NÃO"
                    if situacao.lower() == "finalizada":
                        if enviar_alerta_email(cliente, numero, descricao, uc_codigo):
                            alerta_enviado = "SIM"
                        alertas += 1
                    linha_valores = [
                        cliente.upper(), login_user, uc_codigo, numero,
                        identificar_tipo(numero), descricao, data_solicitacao, situacao,
                        detalhe.get("prazo", ""), agora, alerta_enviado, agora, "",
                    ]
                    novas_linhas_buffer.append(linha_valores)
                    index[chave] = {"linha": proxima_linha, "situacao": situacao, "alerta_enviado": alerta_enviado}
                    proxima_linha += 1
                    novos += 1
                    # volta pra listagem antes de continuar checando outras solicitações da mesma UC
                    abrir_acompanhe_solicitacoes(driver, cliente, uc_codigo)

                elif registro["situacao"] != situacao:
                    detalhe = buscar_detalhe_solicitacao(driver, numero, cliente)
                    agora = datetime.now().strftime("%d/%m/%Y %H:%M")
                    alerta_enviado = registro["alerta_enviado"]
                    if situacao.lower() == "finalizada" and alerta_enviado != "SIM":
                        if enviar_alerta_email(cliente, numero, descricao, uc_codigo):
                            alerta_enviado = "SIM"
                        alertas += 1
                    atualizacoes_buffer.append((
                        registro["linha"],
                        [situacao, detalhe.get("prazo", ""), agora, alerta_enviado],
                    ))
                    index[chave]["situacao"] = situacao
                    index[chave]["alerta_enviado"] = alerta_enviado
                    atualizados += 1
                    abrir_acompanhe_solicitacoes(driver, cliente, uc_codigo)

        if novas_linhas_buffer:
            aba.append_rows(novas_linhas_buffer, value_input_option="USER_ENTERED")
        if atualizacoes_buffer:
            updates = [
                {"range": f"H{linha}:K{linha}", "values": [valores]}
                for linha, valores in atualizacoes_buffer
            ]
            aba.batch_update(updates, value_input_option="USER_ENTERED")

        return True, novos, atualizados, alertas

    except Exception as e:
        print(f"  ❌ [{cliente}] Erro inesperado: {e}")
        _salvar_debug(driver, f"{cliente}_fatal")
        return False, novos, atualizados, alertas
    finally:
        driver.quit()


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
