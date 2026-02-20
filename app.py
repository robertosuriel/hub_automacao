import streamlit as st
import os
import sys
import threading
import json
from dotenv import load_dotenv


if "google_credentials" in st.secrets:
    with open("credentials.json", "w", encoding="utf-8") as f:
        json.dump(dict(st.secrets["google_credentials"]), f)
            
# Importa as bibliotecas do Streamlit que resolvem o erro de "NoSessionContext" nas Threads
from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx

# Importa as funções dos seus robôs
from extrator import processar_cliente
from gerador_pagos import processar_faturas_pagas

load_dotenv(".env")

st.set_page_config(page_title="Hub de Automação Blue", page_icon="⚡", layout="wide")

# --- COMPONENTE DE LOG EM TEMPO REAL (CORRIGIDO PARA MULTITHREADING) ---
class StreamlitRedirect:
    def __init__(self, st_empty):
        self.st_empty = st_empty
        self.text = ""
        # 1. Salva o "RG" (Contexto) da sessão do usuário que apertou o botão
        self.ctx = get_script_run_ctx()

    def write(self, string):
        if not string: return
        
        self.text += string
        linhas = self.text.split('\n')[-15:]
        
        try:
            # 2. Injeta o "RG" do usuário na Thread atual antes de tentar atualizar a tela
            if self.ctx:
                add_script_run_ctx(threading.current_thread(), self.ctx)
                
            self.st_empty.code('\n'.join(linhas), language='bash')
        except Exception:
            # Se der algum conflito visual, simplesmente ignora para não travar o robô
            pass

    def flush(self):
        pass


clientes_disponiveis = ['blue', 'criatech', 'soft', 'softcomp', 'DNA', 'NCA']

# --- BARRA LATERAL (MENU) ---
st.sidebar.title("🛠️ Ferramentas")
modulo_selecionado = st.sidebar.radio(
    "Escolha o processo:",
    ["1. Extrair Faturas (Coelba)", "2. Gerar PDFs 'PAGO'"]
)

st.title(f"⚡ {modulo_selecionado}")
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
                    
                    login_user = os.getenv(f'{cliente.upper()}_LOGIN_USER')
                    login_password = os.getenv(f'{cliente.upper()}_LOGIN_PASSWORD')
                    worksheet = os.getenv(f'{cliente.upper()}_WORKSHEET')
                    
                    if not all([login_user, login_password, worksheet]):
                        resultados[cliente] = "❌ Falha (Dados .env)"
                        continue
                    
                    with st.spinner(f"O robô está trabalhando na conta {cliente}..."):
                        sucesso = processar_cliente(cliente, login_user, login_password, worksheet)
                    
                    resultados[cliente] = "✅ Sucesso" if sucesso else "❌ Falha"
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
                    
                    # 1. Monta o pacote de dados mastigado para o gerador_pagos.py
                    clientes_com_aba = {}
                    for cli in clientes_selecionados:
                        # Tenta achar no ambiente
                        aba = os.getenv(f"{cli.upper()}_WORKSHEET")
                        # Se não achar, busca direto no cofre do Streamlit
                        if not aba and f"{cli.upper()}_WORKSHEET" in st.secrets:
                            aba = st.secrets[f"{cli.upper()}_WORKSHEET"]
                            
                        clientes_com_aba[cli] = aba

                    # 2. Envia para processar
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
                    st.warning(f"**{cli.upper()}**: {status}")
