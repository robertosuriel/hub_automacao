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
from gerador_pagos import processar_faturas_pagas

load_dotenv(".env")

# 1. ATUALIZANDO TÍTULO E ÍCONE PARA SOL ONLINE
st.set_page_config(page_title="Hub de Automação Sol Online", page_icon="☀️", layout="wide")

# --- INJEÇÃO DE IDENTIDADE VISUAL (CSS) ---
# Copiando o gradiente e botões do painel da Sol Online
st.markdown("""
<style>
    /* Gradiente da Sol Online na barra lateral */
    [data-testid="stSidebar"] {
        background: linear-gradient(150deg, #FACC15 0%, #FF3366 100%);
        color: white;
    }
    /* Força os textos da barra lateral a ficarem brancos para dar contraste com o gradiente */
    [data-testid="stSidebar"] * {
        color: white !important;
    }
    /* Cor do botão primário para o Rosa/Vermelho da marca */
    div.stButton > button[kind="primary"] {
        background-color: #FF3366;
        color: white;
        border: none;
        border-radius: 6px;
        font-weight: bold;
    }
    div.stButton > button[kind="primary"]:hover {
        background-color: #E62E5C;
        color: white;
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

clientes_disponiveis = ['blue', 'criatech', 'soft', 'softcomp', 'DNA', 'NCA']

# --- BARRA LATERAL (MENU) COM O LOGO ---
# Insere o logo branco no topo da barra lateral
try:
    st.sidebar.image("logo.png", use_container_width=True)
except Exception:
    pass # Caso você esqueça de subir a imagem, o código não quebra

st.sidebar.title("🛠️ Ferramentas")
st.sidebar.markdown("---")
modulo_selecionado = st.sidebar.radio(
    "Escolha o processo:",
    ["1. Extrair Faturas (Coelba)", "2. Gerar PDFs 'PAGO'"]
)

# Atualizando o título principal
st.title(f"☀️ {modulo_selecionado}")
st.markdown("---")

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
                            "NCA": "Controle_NCA_Automação"
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
                        "NCA": "Controle_NCA_Automação"
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
