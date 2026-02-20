import os
import sys
import io
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Bibliotecas de PDF e Imagem
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib.colors import red
from PyPDF2 import PdfReader, PdfWriter

# Bibliotecas Google e Rede
from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload
import requests

# --- CONFIGURAÇÕES INICIAIS ---
if getattr(sys, 'frozen', False):
    base_path = sys._MEIPASS
else:
    base_path = os.path.dirname(os.path.abspath(__file__))

env_path = os.path.join(base_path, ".env")
load_dotenv(env_path)

credentials_path = os.path.join(base_path, "credentials.json")
SPREADSHEET_ID = "1Ut5Y0LstIP7nhv7Jzyywc7SS7ObIPlO-3yEg-J8Pp5o"
PASTA_DRIVE_PAGO = "1kHvWYkoQyL2WnjKDhYGks1jsoTh7zZ1k"

print_lock = Lock()

def safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)

# --- AUTENTICAÇÃO ---
def autenticar_google_sheets():
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
    return gspread.authorize(creds)

def autenticar_drive():
    SCOPES = ["https://www.googleapis.com/auth/drive"]
    creds = service_account.Credentials.from_service_account_file(credentials_path, scopes=SCOPES)
    return build("drive", "v3", credentials=creds)

# --- OTIMIZAÇÃO 1: MARCA D'ÁGUA EM MEMÓRIA ---
def criar_marca_dagua_cache():
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)
    c.setFillColor(red)
    c.setFont("Helvetica-Bold", 120)
    width, height = letter
    c.saveState()
    c.translate(width/2, height/2)
    c.rotate(45)
    c.setFillAlpha(0.3)
    c.drawString(-120, 50, "PAGO")
    c.drawString(-370, -50, "SOL ONLINE")
    c.restoreState()
    c.save()
    buffer.seek(0)
    return PdfReader(buffer).pages[0]

MARCA_DAGUA_PAGE = criar_marca_dagua_cache()

def adicionar_marca_dagua_rapida(pdf_bytes):
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        writer = PdfWriter()
        
        for page in reader.pages:
            page.merge_page(MARCA_DAGUA_PAGE)
            writer.add_page(page)
        
        output_buffer = io.BytesIO()
        writer.write(output_buffer)
        return output_buffer.getvalue()
    except Exception as e:
        safe_print(f"❌ Erro ao adicionar marca d'água: {e}")
        return None

# --- OTIMIZAÇÃO 2: CACHE DE ARQUIVOS ---
def mapear_arquivos_drive(pasta_id):
    safe_print("📂 Mapeando arquivos existentes no Drive...")
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
        safe_print(f"📂 Cache carregado: {len(arquivos_cache)} arquivos encontrados.")
        return arquivos_cache
    except Exception as e:
        safe_print(f"⚠️ Erro ao criar cache do Drive: {e}")
        return {}

def obter_nome_arquivo_drive(client_drive, file_id):
    try:
        res = client_drive.files().get(fileId=file_id, fields='name').execute()
        return res.get('name')
    except:
        return None

def baixar_pdf_memoria(client_drive, file_id):
    try:
        request = client_drive.files().get_media(fileId=file_id)
        return request.execute()
    except:
        return None

def upload_simples(client_drive, conteudo_pdf, nome_arquivo, pasta_id):
    file_metadata = {"name": nome_arquivo, "parents": [pasta_id]}
    media = MediaInMemoryUpload(conteudo_pdf, mimetype="application/pdf")
    arquivo = client_drive.files().create(body=file_metadata, media_body=media, fields="id").execute()
    return arquivo.get("id")

# --- PROCESSAMENTO INDIVIDUAL ---
def processar_linha_thread(dados):
    linha_num, link_drive, cache_drive, client_drive_local = dados
    
    if "drive.google.com" not in link_drive:
        return linha_num, None 
    
    try:
        try:
            file_id_original = link_drive.split("/file/d/")[1].split("/")[0]
        except:
            return linha_num, None

        nome_original = obter_nome_arquivo_drive(client_drive_local, file_id_original)
        if nome_original:
            nome_sem_ext = os.path.splitext(nome_original)[0]
            nome_arquivo_pago = f"pago_{nome_sem_ext}.pdf"
        else:
            nome_arquivo_pago = f"pago_{file_id_original}.pdf"

        if nome_arquivo_pago in cache_drive:
            file_id_final = cache_drive[nome_arquivo_pago]
            link_final = f"https://drive.google.com/file/d/{file_id_final}/view"
            safe_print(f"⚡ [Linha {linha_num}] Já existe no cache.")
            return linha_num, link_final

        safe_print(f"⬇️ [Linha {linha_num}] Baixando...")
        pdf_bytes = baixar_pdf_memoria(client_drive_local, file_id_original)
        if not pdf_bytes or not pdf_bytes.startswith(b'%PDF'):
            return linha_num, None

        pdf_com_marca = adicionar_marca_dagua_rapida(pdf_bytes)
        if not pdf_com_marca:
            return linha_num, None

        safe_print(f"⬆️ [Linha {linha_num}] Fazendo Upload...")
        file_id_novo = upload_simples(client_drive_local, pdf_com_marca, nome_arquivo_pago, PASTA_DRIVE_PAGO)
        
        cache_drive[nome_arquivo_pago] = file_id_novo
        
        link_final = f"https://drive.google.com/file/d/{file_id_novo}/view"
        return linha_num, link_final

    except Exception as e:
        safe_print(f"❌ Erro thread linha {linha_num}: {e}")
        return linha_num, None

# --- PROCESSAMENTO DA ABA ---
def processar_aba_otimizada(nome_aba, cache_drive):
    client_sheets = autenticar_google_sheets()
    try:
        aba = client_sheets.open_by_key(SPREADSHEET_ID).worksheet(nome_aba)
    except:
        safe_print(f"❌ Aba {nome_aba} não encontrada.")
        return 0, 0

    coluna_j = aba.col_values(10)
    tarefas = []
    
    for i, link in enumerate(coluna_j):
        linha_num = i + 1
        if linha_num == 1: continue 
        if link and "drive.google.com" in link:
             tarefas.append((linha_num, link))
    
    if not tarefas:
        safe_print(f"⚠️ Nenhum link na aba {nome_aba}")
        return 0, 0

    safe_print(f"🚀 Iniciando {len(tarefas)} tarefas na aba {nome_aba}...")

    resultados = {}
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        def wrapper(t):
            local_drive = autenticar_drive() 
            return processar_linha_thread((t[0], t[1], cache_drive, local_drive))

        futures = [executor.submit(wrapper, t) for t in tarefas]
        
        for future in as_completed(futures):
            l_num, link_res = future.result()
            if link_res:
                resultados[l_num] = link_res
    
    if resultados:
        safe_print("💾 Salvando todos os links pagos na planilha...")
        updates = []
        for l_num, link in resultados.items():
            updates.append({
                'range': f'K{l_num}',
                'values': [[link]]
            })
        
        if updates:
            aba.batch_update(updates, value_input_option='USER_ENTERED')
            
    return len(resultados), len(tarefas) - len(resultados)

# --- FUNÇÃO PRINCIPAL CHAMADA PELO STREAMLIT ---
def processar_faturas_pagas(clientes_selecionados):
    start_time = time.time()
    cache_drive = mapear_arquivos_drive(PASTA_DRIVE_PAGO)
    
    resultados_finais = {}
    
    for cliente in clientes_selecionados:
        safe_print(f"\n--- Processando PAGO para: {cliente} ---")
        worksheet_nome = os.getenv(f'{cliente}_WORKSHEET')
        
        if not worksheet_nome:
            safe_print(f"⚠️ Worksheet não encontrada no .env para {cliente}")
            resultados_finais[cliente] = "Falha (Aba não configurada no .env)"
            continue
            
        suc, falha = processar_aba_otimizada(worksheet_nome, cache_drive)
        resultados_finais[cliente] = f"✅ Sucesso: {suc} | ❌ Falhas: {falha}"
        
    safe_print(f"\n🏁 Concluído em {time.time() - start_time:.2f} segundos.")
    return resultados_finais
