import os
import sys
import time
import json
import base64
import requests
import pandas as pd
import gspread
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload
from oauth2client.service_account import ServiceAccountCredentials

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
df_lock = Lock()

SPREADSHEET_ID = "1Ut5Y0LstIP7nhv7Jzyywc7SS7ObIPlO-3yEg-J8Pp5o"
PASTA_DRIVE_ID = "1wbPLpNj_h1i3nLCEhVx2vdYyDiIval-9"

# --- FUNÇÕES AUXILIARES ---

def autenticar_google_sheets():
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
    return gspread.authorize(creds)

def autenticar_drive():
    SCOPES = ["https://www.googleapis.com/auth/drive"]
    creds = service_account.Credentials.from_service_account_file(credentials_path, scopes=SCOPES)
    return build("drive", "v3", credentials=creds)

def configurar_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36")
    chrome_options.add_argument("--log-level=3")
    
    # Parâmetros vitais para rodar em servidor Linux/Docker
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    
    chrome_options.binary_location = "/usr/bin/chromium"
    servico = Service("/usr/bin/chromedriver")
    
    return webdriver.Chrome(service=servico, options=chrome_options)

def realizar_login_selenium_original(driver, login_user, login_password):
    try:
        login_button = driver.find_element(By.XPATH, "/html/body/app-root/app-header/header/nav/div[1]/div/div/button/span[1]")
        login_button.click()
        print("  Login modal aberto. Aguardando campos...")
        time.sleep(3)
    except Exception:
        pass

    try:
        email_field = driver.find_element(By.XPATH, "/html/body/div[2]/div[2]/div/mat-dialog-container/app-dialog-login/mat-dialog-content/section/form/mat-horizontal-stepper/div[2]/div/div/mat-form-field[1]/div/div[1]/div[3]/input")
        email_field.clear()
        email_field.send_keys(login_user)
        print("  CNPJ preenchido. Aguardando site registrar...")
        time.sleep(1) # <-- MÁGICA DO ANGULAR: Espera o site "ler" a digitação

        password_field = driver.find_element(By.XPATH, "/html/body/div[2]/div[2]/div/mat-dialog-container/app-dialog-login/mat-dialog-content/section/form/mat-horizontal-stepper/div[2]/div/div/mat-form-field[2]/div/div[1]/div[3]/input")
        password_field.clear()
        password_field.send_keys(login_password)
        print("  Senha preenchida. Aguardando site registrar...")
        time.sleep(1) # <-- MÁGICA DO ANGULAR: Espera o site "ler" a digitação

        submit_button = driver.find_element(By.XPATH, "/html/body/div[2]/div[2]/div/mat-dialog-container/app-dialog-login/mat-dialog-content/section/form/mat-horizontal-stepper/div[2]/div/div/div[3]/app-neo-button/button/div")
        submit_button.click()
        
        print("  Credenciais enviadas. Aguardando token da Neoenergia...")
        time.sleep(2) # <-- MÁGICA DO ANGULAR: Respiro antes de procurar o token
        
        for i in range(20):
            token = driver.execute_script("return window.localStorage.getItem('tokenNeSe');")
            if token:
                return token
            time.sleep(1)
            
        return None
    except Exception as e:
        print(f"  ❌ Erro interno na tela de login: {e}")
        return None

# --- SHEETS E DRIVE ---

def extrair_faturas_e_flags(planilha_id, nome_aba):
    try:
        client = autenticar_google_sheets()
        aba = client.open_by_key(planilha_id).worksheet(nome_aba)
        dados = aba.get_values("C2:R")
        resultado = {}
        colunas = ['M', 'N', 'O', 'P', 'Q', 'R']
        indices = [10, 11, 12, 13, 14, 15]
        for linha in dados:
            if len(linha) >= 1:
                fatura = linha[0]
                flags = {}
                for coluna, indice in zip(colunas, indices):
                    if indice < len(linha):
                        flags[coluna] = (str(linha[indice]).upper() == 'TRUE')
                    else:
                        flags[coluna] = False
                resultado[fatura] = flags
        return resultado
    except Exception:
        return {}

def restaurar_flags(planilha_id, nome_aba, flags_salvas):
    try:
        client = autenticar_google_sheets()
        aba = client.open_by_key(planilha_id).worksheet(nome_aba)
        faturas_atuais = aba.get_values("C2:C")
        colunas = ['M', 'N', 'O', 'P', 'Q', 'R']
        for coluna in colunas:
            valores = []
            for linha in faturas_atuais:
                if linha and linha[0]:
                    fatura = linha[0]
                    val = flags_salvas.get(fatura, {}).get(coluna, False)
                    valores.append([val])
                else:
                    valores.append([False])
            if valores:
                aba.update(f"{coluna}2:{coluna}{len(valores)+1}", valores, value_input_option='USER_ENTERED')
    except Exception:
        pass

def escrever_no_google_sheets(df, planilha_id, nome_aba, intervalo="A2:G"):
    client_sheets = autenticar_google_sheets()
    planilha = client_sheets.open_by_key(planilha_id)
    aba = planilha.worksheet(nome_aba)
    
    aba.batch_clear([intervalo, "K2:K"])
    
    dados = df.astype(str).values.tolist()
    aba.update(intervalo, dados, value_input_option="USER_ENTERED")

def atualizar_links_sheets(planilha_id, nome_aba, df_filtrado):
    try:
        client_sheets = autenticar_google_sheets()
        planilha = client_sheets.open_by_key(planilha_id)
        aba = planilha.worksheet(nome_aba)
        col_numero_fatura = aba.col_values(3)[1:]
        
        df_filtrado["numeroFatura"] = df_filtrado["numeroFatura"].astype(str).str.strip()
        dict_links = dict(zip(df_filtrado["numeroFatura"], df_filtrado["link_drive"]))
        
        col_j = [str(dict_links.get(f.strip(), "")) for f in col_numero_fatura]
        
        if col_j:
            aba.update(f"J2:J{len(col_j)+1}", [[l] for l in col_j], value_input_option='USER_ENTERED')
    except Exception as e:
        print(f"Erro ao atualizar links: {e}")

# --- ARQUIVOS E DRIVE (COM CACHE) ---

def listar_arquivos_existentes(pasta_id):
    print("📂 Mapeando arquivos existentes no Drive (Cache)...")
    client_drive = autenticar_drive()
    arquivos_cache = {}
    page_token = None
    
    try:
        while True:
            results = client_drive.files().list(
                q=f"'{pasta_id}' in parents and trashed = false",
                pageSize=1000,
                fields="nextPageToken, files(id, name)",
                pageToken=page_token
            ).execute()
            
            for f in results.get('files', []):
                arquivos_cache[f['name']] = f['id']
                
            page_token = results.get('nextPageToken')
            if not page_token:
                break
        print(f"📂 Cache carregado: {len(arquivos_cache)} arquivos encontrados.")
        return arquivos_cache
    except Exception as e:
        print(f"⚠️ Erro ao criar cache do Drive: {e}")
        return {}

def upload_para_drive_conteudo_pdf(conteudo_pdf, nome_arquivo_drive, pasta_id):
    client_drive = autenticar_drive()
    file_metadata = {"name": nome_arquivo_drive, "parents": [pasta_id]}
    media = MediaInMemoryUpload(conteudo_pdf, mimetype="application/pdf")
    arquivo = client_drive.files().create(body=file_metadata, media_body=media, fields="id").execute()
    return arquivo.get("id")

def buscar_links_drive(df, pasta_id=None):
    if "file_id" not in df.columns:
        df["link_drive"] = pd.NA
        return df
    
    def criar_link(fid):
        if pd.notna(fid) and str(fid).strip():
            return f"https://drive.google.com/file/d/{str(fid).strip()}/view?usp=sharing"
        return pd.NA
        
    df["link_drive"] = df["file_id"].apply(criar_link)
    return df

# --- REQUISIÇÕES E PROCESSAMENTO ---

def baixar_pdf_fatura(numeroFatura, mesReferencia, codigo, tokenNeSe, protocolo_legado, login_user, cache_drive):
    nome_arquivo = f"{mesReferencia}_{codigo}_{numeroFatura}.pdf"
    
    if nome_arquivo in cache_drive:
        return numeroFatura, cache_drive[nome_arquivo], "EXISTE"

    url = f"https://apineprd.neoenergia.com/multilogin/2.0.0/servicos/faturas/{numeroFatura}/pdf"
    headers = {"Authorization": f"Bearer {tokenNeSe}", "Accept": "application/json", "User-Agent": "Mozilla/5.0"}
    params = {
        "codigo": codigo, "protocolo": protocolo_legado, "tipificacao": "1031607",
        "usuario": "WSO2_CONEXAO", "canalSolicitante": "AGC", "motivo": "10",
        "distribuidora": "COELBA", "regiao": "NE", "tipoPerfil": "1",
        "documento": login_user, "documentoSolicitante": login_user, "byPassActiv": ""
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=60)
        if response.status_code == 200:
            data = response.json()
            if "fileData" in data:
                pdf_bytes = base64.b64decode(data["fileData"])
                file_id = upload_para_drive_conteudo_pdf(pdf_bytes, nome_arquivo, PASTA_DRIVE_ID)
                cache_drive[nome_arquivo] = file_id 
                return numeroFatura, file_id, "BAIXADO"
    except Exception as e:
        print(f"⚠️ Erro na requisição fatura {numeroFatura}: {e}")
        
    return numeroFatura, None, "ERRO"

def preparar_dados_para_exportacao(df):
    ordem_status = {"Vencidas": 0, "A Vencer": 1, "Pago": 2}
    df["status_ordenado"] = df["situação"].map(ordem_status).fillna(3)
    df["vencimento"] = pd.to_datetime(df["vencimento"], errors="coerce")
    df = df.sort_values(by=["vencimento", "status_ordenado"])
    df = df.drop(columns=["status_ordenado"])
    return df

# --- LÓGICA PRINCIPAL DE PROCESSAMENTO (IMPORTANTE PARA O STREAMLIT) ---

def processar_cliente(cliente, login_user, login_password, worksheet):
    """
    Função principal que o Streamlit irá chamar para cada cliente selecionado.
    Contém a lógica de retentativas de login.
    """
    MAX_TENTATIVAS_LOGIN = 3
    tentativa_atual = 1
    tokenNeSe = None

    while tentativa_atual <= MAX_TENTATIVAS_LOGIN:
        print(f"  Tentativa de Login {tentativa_atual}/{MAX_TENTATIVAS_LOGIN}...")
        driver = configurar_driver()
        
        try:
            driver.get("https://agenciavirtual.neoenergia.com/#/login")
            time.sleep(4) # Espera a página carregar
            WebDriverWait(driver, 30).until(lambda d: d.execute_script("return document.readyState") == "complete")
            bearer_token = realizar_login_selenium_original(driver, login_user, login_password)
            
            if bearer_token:
                tokenNeSe = bearer_token.split(":")[1].split(",")[0].strip(' "{}')
                print("  ✅ Login realizado com sucesso!")
                driver.quit()
                break
            else:
                print("  ⚠️ Token não obtido.")
        except Exception as e:
             print(f"  ⚠️ Erro na tentativa de login: {e}")
             
        driver.quit()
        tentativa_atual += 1
        if tentativa_atual <= MAX_TENTATIVAS_LOGIN:
             print("  Aguardando 5 segundos antes de tentar novamente...\n")
             time.sleep(5)

    if not tokenNeSe:
        print(f"❌ Falha no login do cliente {cliente} após {MAX_TENTATIVAS_LOGIN} tentativas.")
        return False

    # 2. Obter Dados da API
    print("  Obtendo dados de UCs e Faturas...")
    headers_api = {"User-Agent": "Mozilla/5.0", "Authorization": "Bearer " + tokenNeSe}
    
    # UCs
    try:
        r_ucs = requests.get(f"https://apineprd.neoenergia.com/imoveis/1.1.0/clientes/{login_user}/ucs", 
                             params={"documento": login_user, "canalSolicitante": "AGC", "distribuidora": "COELBA", "usuario": "WSO2_CONEXAO", "indMaisUcs": "X", "tipoPerfil": "1"}, 
                             headers=headers_api, timeout=30)
        codigos_uc = [uc['uc'] for uc in r_ucs.json().get("ucs", [])]
    except Exception as e:
        print(f"  ❌ Erro ao obter UCs: {e}")
        return False

    if not codigos_uc:
        print("  ❌ Nenhuma UC encontrada.")
        return False

    # Protocolo
    try:
        r_proto = requests.get("https://apineprd.neoenergia.com/protocolo/1.1.0/obterProtocolo",
                               params={"distribuidora": "COEL", "canalSolicitante": "AGC", "documento": login_user, "codCliente": codigos_uc[0], "recaptchaAnl": "true", "regiao": "NE"},
                               headers=headers_api, timeout=30)
        protocolo = r_proto.json().get('protocoloLegado')
    except:
        protocolo = None

    # Faturas
    dados_coletados = []
    for codigo in codigos_uc:
        try:
            params = {"codigo": codigo, "documento": login_user, "canalSolicitante": "AGC", "usuario": "WSO2_CONEXAO", "protocolo": protocolo, "byPassActiv": "X", "documentoSolicitante": login_user, "documentoCliente": login_user, "distribuidora": "COELBA", "tipoPerfil": "1"}
            r_fat = requests.get("https://apineprd.neoenergia.com/multilogin/2.0.0/servicos/faturas/ucs/faturas", headers=headers_api, params=params, timeout=30)
            lista = r_fat.json().get("faturas", []) if r_fat.status_code == 200 else []
            if lista:
                for f in lista:
                    dados_coletados.append({
                        "codigo_cliente": codigo, "mesReferencia": f.get("mesReferencia", "N/A"), "numeroFatura": f.get("numeroFatura", "N/A"),
                        "emissão": f.get("dataEmissao", "N/A"), "vencimento": f.get("dataVencimento", "N/A"),
                        "valor": f.get("valorEmissao", "N/A").replace(".", ","), "situação": f.get("statusFatura", "N/A")
                    })
            else:
                dados_coletados.append({"codigo_cliente": codigo, "vencimento": "N/A", "numeroFatura": "N/A", "situação": "N/A", "valor": "N/A", "emissão": "N/A", "mesReferencia": "N/A"})
        except: pass

    if not dados_coletados:
        print("  ❌ Nenhuma fatura encontrada.")
        return False

    # 3. Processamento de Tabela
    df_geral = pd.DataFrame(dados_coletados)
    df_geral['vencimento'] = pd.to_datetime(df_geral['vencimento'], errors='coerce')
    df_geral = df_geral.dropna(subset=['vencimento'])
    df_geral = df_geral[df_geral['vencimento'] >= pd.to_datetime("2024-12-01")]
    df_geral['vencimento'] = df_geral['vencimento'].dt.strftime('%Y-%m-%d').fillna('N/A')
    
    print("  Atualizando Sheets...")
    flags_salvas = extrair_faturas_e_flags(SPREADSHEET_ID, worksheet)
    df_ordenado = preparar_dados_para_exportacao(df_geral)
    escrever_no_google_sheets(df_ordenado, SPREADSHEET_ID, worksheet)
    df_ordenado["file_id"] = pd.NA

    # 4. Downloads Inteligentes (Com Cache)
    faturas_validas = df_ordenado[df_ordenado["numeroFatura"] != "N/A"].copy()
    
    if not faturas_validas.empty:
        cache_drive = listar_arquivos_existentes(PASTA_DRIVE_ID)
        
        total = len(faturas_validas)
        print(f"  🚀 Iniciando verificação de {total} faturas...")
        
        def processar_thread(row):
            return baixar_pdf_fatura(row["numeroFatura"], row["mesReferencia"], row["codigo_cliente"], tokenNeSe, protocolo, login_user, cache_drive)

        falhas = []
        concluidos = 0
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futu_map = {executor.submit(processar_thread, row): row for _, row in faturas_validas.iterrows()}
            
            for fut in as_completed(futu_map):
                concluidos += 1
                try:
                    num, fid, status = fut.result()
                    
                    if status == "EXISTE":
                        print(f"   [{concluidos}/{total}] 📂 Já existe: {num}")
                    elif status == "BAIXADO":
                        print(f"   [{concluidos}/{total}] ⬇️ BAIXADO AGORA: {num}")
                    else:
                        print(f"   [{concluidos}/{total}] ⚠️ Falha/Vazio: {num}")
                        if not fid: falhas.append(futu_map[fut])

                    if fid:
                        with df_lock:
                            df_ordenado["file_id"] = df_ordenado["file_id"].astype(object)
                            df_ordenado.loc[df_ordenado["numeroFatura"] == num, "file_id"] = str(fid)
                except:
                    falhas.append(futu_map[fut])

        if falhas:
            print(f"  🔁 Tentando baixar {len(falhas)} arquivos que falharam...")
            with ThreadPoolExecutor(max_workers=3) as exc:
                for row in falhas:
                    exc.submit(processar_thread, row)

    # 5. Finalização
    print("  Atualizando links e restaurando checkboxes...")
    df_ordenado = buscar_links_drive(df_ordenado, PASTA_DRIVE_ID)
    atualizar_links_sheets(SPREADSHEET_ID, worksheet, df_ordenado)
    restaurar_flags(SPREADSHEET_ID, worksheet, flags_salvas)

    print(f"🎉 Cliente {cliente.upper()} finalizado com sucesso.")
    return True


# --- BLOCO QUE RODA SE VOCÊ CHAMAR O SCRIPT DIRETAMENTE (TESTE) ---
if __name__ == "__main__":
    clientes = ['blue', 'criatech', 'soft', 'softcomp', 'DNA', 'NCA']
    resultados = {}

    for cliente in clientes:
        print(f"\n{'='*50}")
        print(f"--- Iniciando cliente: {cliente.upper()} ---")
        print(f"{'='*50}")
        
        login_user = os.getenv(f'{cliente.upper()}_LOGIN_USER')
        login_password = os.getenv(f'{cliente.upper()}_LOGIN_PASSWORD')
        worksheet = os.getenv(f'{cliente.upper()}_WORKSHEET')

        if not all([login_user, login_password, worksheet]):
            print(f"Dados incompletos no .env para {cliente.upper()}")
            resultados[cliente] = "Falha (Dados .env incompletos)"
            continue

        sucesso = processar_cliente(cliente, login_user, login_password, worksheet)
        
        if sucesso:
            resultados[cliente] = "Sucesso"
        else:
            resultados[cliente] = "Falha"

    # --- RELATÓRIO FINAL DO SCRIPT ---
    print("\n" + "="*50)
    print("🚀 RELATÓRIO FINAL DE PROCESSAMENTO 🚀")
    print("="*50)
    for cliente, status in resultados.items():
        simbolo = "✅" if status == "Sucesso" else "❌"
        print(f"{simbolo} {cliente.ljust(15).upper()} : {status}")
    print("="*50)
