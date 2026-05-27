import streamlit as st
import os
import sys
import threading
import json
from dotenv import load_dotenv

# --- TRUQUE DE SEGURANÇA PARA A NUVEM ---
if "google_credentials" in st.secrets:
    with open("credentials.json", "w", encoding="utf-8") as f:
        json.dump(dict(st.secrets["google_credentials"]), f)
            
from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx
from extrator import processar_cliente
from gerador_pagos import processar_faturas_pagas, gerar_pdf_pago_upload

load_dotenv(".env")

# 1. CONFIGURAÇÃO DA PÁGINA
st.set_page_config(page_title="Hub de Automação Sol Online", page_icon="☀️", layout="wide")

# --- INJEÇÃO DE CSS (Visual Personalizado) ---
st.markdown("""
<style>
    /* 1. Gradiente da Barra Lateral (Invertido: Rosa no topo -> Laranja embaixo) */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #FF3366 0%, #FACC15 100%);
        color: white;
    }
    /* Força os textos da barra lateral a ficarem brancos */
    [data-testid="stSidebar"] * {
        color: white !important;
    }
    /* Ajuste para a logo não ficar colada no teto */
    [data-testid="stSidebarUserContent"] img {
        margin-top: 20px;
        margin-bottom: 15px;
    }
    
    /* 2. Estilo dos Botões Principais (Rosa da Marca) */
    div.stButton > button[kind="primary"] {
        background-color: #FF3366;
        color: white;
        border: none;
        border-radius: 8px;
        font-weight: 700;
        padding: 0.75rem 1.5rem;
        transition: all 0.3s ease;
    }
    div.stButton > button[kind="primary"]:hover {
        background-color: #E62E5C;
        box-shadow: 0 4px 12px rgba(255, 51, 102, 0.3);
        transform: translateY(-2px);
    }

    /* 3. Novo Header (Título) Profissional */
    .main-header {
        background: linear-gradient(to right, #ffffff, #f8f9fa);
        padding: 25px;
        border-radius: 15px;
        border-left: 8px solid #FF3366; /* Linha de destaque rosa */
        box-shadow: 0 6px 15px rgba(0,0,0,0.05);
        margin-bottom: 30px;
        display: flex;
        align-items: center;
    }
    .main-header h1 {
        color: #31333F;
        margin: 0;
        font-size: 2.2rem;
        font-weight: 800;
        letter-spacing: -1px;
    }
    /* Ícone do sol no header */
    .header-icon {
        font-size: 2.5rem;
        margin-right: 15px;
        text-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }

    /* Ajustes gerais de espaçamento */
    .block-container {
        padding-top: 2rem;
    }
</style>
""", unsafe_allow_html=True)

# --- COMPONENTE DE LOG EM TEMPO REAL ---
class StreamlitRedirect:
    def __init__(self, st_empty):
        self.st_empty = st_empty
        self.text = ""
        self.ctx = get_script_run_ctx()

    def write(self, string):
        if not string: return
        self.text += string
        linhas = self.text.split('\n')[-15:]
        try:
            if self.ctx:
                add_script_run_ctx(threading.current_thread(), self.ctx)
            self.st_empty.code('\n'.join(linhas), language='bash')
        except Exception:
            pass

    def flush(self):
        pass

clientes_disponiveis = ['blue', 'criatech', 'soft', 'softcomp', 'DNA', 'NCA', 'GM']

# --- BARRA LATERAL (MENU) ---
try:
    # Use container width para ajustar o logo
    st.sidebar.image("logo.png", use_container_width=True)
except Exception:
    st.sidebar.warning("Logo não encontrado (logo.png)")

st.sidebar.title("🛠️ Ferramentas")
st.sidebar.markdown("---")
modulo_selecionado = st.sidebar.radio(
    "Escolha o processo:",
    ["1. Extrair Faturas (Coelba)", "2. Gerar PDFs 'PAGO'"]
)

# --- ÁREA PRINCIPAL COM O NOVO HEADER ---
# Usamos HTML personalizado para criar o header bonito
st.markdown(f"""
<div class="main-header">
    <span class="header-icon">☀️</span>
    <h1>{modulo_selecionado}</h1>
</div>
""", unsafe_allow_html=True)


# --- OPÇÕES DE SELEÇÃO DE CLIENTES ---
col1, col2 = st.columns([1, 2])
with col1:
    modo = st.radio("Modo de Execução:", ["Rodar Todos", "Selecionar Específicos"])

with col2:
    if modo == "Rodar Todos":
        clientes_selecionados = clientes_disponiveis
        st.info("Todos os clientes serão processados na sequência.")
    else:
        clientes_selecionados = st.multiselect("Selecione os clientes:", clientes_disponiveis, default=[clientes_disponiveis[0]])


# ==========================================
# MÓDULO 1: EXTRAIR FATURAS COELBA
# ==========================================
if "Extrair Faturas" in modulo_selecionado:
    st.markdown("Esse robô fará login na Coelba, baixará as faturas e atualizará a Planilha (Coluna J).")
    st.divider()
    
    if st.button("▶️ Iniciar Extração Coelba", type="primary", use_container_width=True):
        if not clientes_selecionados:
            st.warning("⚠️ Selecione pelo menos um cliente para continuar.")
        else:
            st.info(f"Iniciando extração para: {', '.join(clientes_selecionados)}")
            
            barra_progresso = st.progress(0)
            texto_status = st.empty()
            caixa_log = st.empty()
            
            resultados = {}
            old_stdout = sys.stdout
            sys.stdout = StreamlitRedirect(caixa_log)
            
            try:
                for i, cliente in enumerate(clientes_selecionados):
                    texto_status.write(f"**Extraindo:** {cliente.upper()} ({i+1}/{len(clientes_selecionados)})")
                    
                    try:
                        login_user = str(st.secrets[f"{cliente.upper()}_LOGIN_USER"])
                        login_password = str(st.secrets[f"{cliente.upper()}_LOGIN_PASSWORD"])
                        
                        MAPA_ABAS = {
                            "blue": "Controle_BlueSolutions_Automação",
                            "criatech": "Controle_Criatech_Automação",
                            "soft": "Controle_SoftDados_Automação",
                            "softcomp": "Controle_SoftComp_Automação",
                            "DNA": "Controle_DNA_Automação",
                            "NCA": "Controle_NCA_Automação",
                            "GM": "Controle_GMEnergia_Automação"
                        }
                        worksheet = MAPA_ABAS.get(cliente)
                        
                    except KeyError:
                        resultados[cliente] = "❌ Falha (Dados faltando no Cofre/Secrets)"
                        continue
                    
                    with st.spinner(f"O robô está trabalhando na conta {cliente}..."):
                        sucesso = processar_cliente(cliente, login_user, login_password, worksheet)
                    
                    if sucesso:
                        resultados[cliente] = "✅ Sucesso"
                    else:
                        resultados[cliente] = "❌ Falha no Login"
                        
                        for img_name in [f"erro_sem_token_{cliente}.png", f"erro_botao_{cliente}.png", f"erro_fatal_{cliente}.png"]:
                            if os.path.exists(img_name):
                                st.error(f"📸 O robô travou nesta tela (Conta {cliente.upper()}):")
                                st.image(img_name)

                    barra_progresso.progress((i + 1) / len(clientes_selecionados))
            finally:
                sys.stdout = old_stdout
                
            texto_status.success("🎉 Extração da Coelba concluída!")
            
            st.divider()
            st.subheader("📊 Relatório de Execução - Extração")
            for cli, status in resultados.items():
                if "✅" in status:
                    st.success(f"**{cli.upper()}**: {status}")
                else:
                    st.error(f"**{cli.upper()}**: {status}")

# ==========================================
# MÓDULO 2: GERAR PDFS 'PAGO'
# ==========================================
elif "Gerar PDFs 'PAGO'" in modulo_selecionado:
    st.markdown("Esse robô lerá a Coluna J da planilha, aplicará a marca d'água de PAGO e salvará o link na Coluna K.")
    st.markdown("### Upload manual de fatura física escaneada")
    arquivo_pdf_manual = st.file_uploader(
        "Envie um PDF de fatura física escaneada para gerar a versão com marca d'água PAGO:",
        type=["pdf"],
        key="upload_pdf_fisico"
    )

    if arquivo_pdf_manual is not None:
        nome_original = arquivo_pdf_manual.name
        pdf_original_bytes = arquivo_pdf_manual.getvalue()

        if st.button("Gerar PDF com marca d'água (Upload Manual)", use_container_width=True):
            with st.spinner("Aplicando marca d'água no PDF enviado..."):
                pdf_com_marca = gerar_pdf_pago_upload(pdf_original_bytes)

            if pdf_com_marca:
                nome_saida = f"pago_{nome_original}"
                st.success("✅ PDF processado com sucesso. Faça o download abaixo.")
                st.download_button(
                    label="⬇️ Baixar PDF com marca d'água",
                    data=pdf_com_marca,
                    file_name=nome_saida,
                    mime="application/pdf",
                    use_container_width=True
                )
            else:
                st.error("❌ Não foi possível processar o PDF enviado. Verifique se o arquivo está válido.")

    st.divider()
    
    if st.button("▶️ Iniciar Geração de Pagos", type="primary", use_container_width=True):
        if not clientes_selecionados:
            st.warning("⚠️ Selecione pelo menos um cliente para continuar.")
        else:
            st.info(f"Iniciando processamento PAGO para: {', '.join(clientes_selecionados)}")
            
            texto_status = st.empty()
            caixa_log = st.empty()
            
            old_stdout = sys.stdout
            sys.stdout = StreamlitRedirect(caixa_log)
            
            try:
                with st.spinner("Lendo planilhas e aplicando marcas d'água... isso pode levar alguns minutos."):
                    
                    MAPA_ABAS = {
                        "blue": "Controle_BlueSolutions_Automação",
                        "criatech": "Controle_Criatech_Automação",
                        "soft": "Controle_SoftDados_Automação",
                        "softcomp": "Controle_SoftComp_Automação",
                        "DNA": "Controle_DNA_Automação",
                        "NCA": "Controle_NCA_Automação",
                        "GM": "Controle_GMEnergia_Automação"
                    }
                    
                    clientes_com_aba = {}
                    for cli in clientes_selecionados:
                        clientes_com_aba[cli] = MAPA_ABAS.get(cli)

                    resultados_pagos = processar_faturas_pagas(clientes_com_aba)
            finally:
                sys.stdout = old_stdout
            
            texto_status.success("🎉 Processamento de Pagos concluído!")
            
            st.divider()
            st.subheader("📊 Relatório de Execução - PDFs Pagos")
            for cli, status in resultados_pagos.items():
                if "✅" in status:
                    st.success(f"**{cli.upper()}**: {status}")
                else:
                    st.error(f"**{cli.upper()}**: {status}")
