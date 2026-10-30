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
import pandas as pd
from extrator import processar_cliente
from gerador_pagos import processar_faturas_pagas, gerar_pdf_pago_upload
from protocolos import processar_protocolos, carregar_dataframe_protocolos

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
        try:
            sys.__stdout__.write(string)
        except Exception:
            pass
        linhas = self.text.split('\n')[-15:]
        try:
            if self.ctx:
                add_script_run_ctx(threading.current_thread(), self.ctx)
            self.st_empty.code('\n'.join(linhas), language='bash')
        except Exception:
            pass

    def flush(self):
        pass

clientes_disponiveis = ['blue', 'criatech', 'soft', 'softcomp', 'DNA', 'NCA', 'GM', 'RM', 'PRIMO']

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
    ["1. Extrair Faturas (Coelba)", "2. Gerar PDFs 'PAGO'", "3. Acompanhar Protocolos (Coelba)"]
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
            log_redirect = StreamlitRedirect(caixa_log)
            sys.stdout = log_redirect
            
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
                            "GM": "Controle_GM_Automação",
                            "RM": "Controle_RM_Automação",
                            "PRIMO": "Controle_PRIMO_Automação"
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
                        resultados[cliente] = "❌ Falha (login ou consulta de faturas — veja o log acima)"
                        
                        for img_name in [f"erro_sem_token_{cliente}.png", f"erro_botao_{cliente}.png", f"erro_fatal_{cliente}.png"]:
                            if os.path.exists(img_name):
                                st.error(f"📸 O robô travou nesta tela (Conta {cliente.upper()}):")
                                st.image(img_name)

                    barra_progresso.progress((i + 1) / len(clientes_selecionados))
            finally:
                sys.stdout = old_stdout
                
            texto_status.success("🎉 Extração da Coelba concluída!")
            with st.expander("📜 Log completo da execução"):
                st.code(log_redirect.text, language="bash")
            
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
            log_redirect = StreamlitRedirect(caixa_log)
            sys.stdout = log_redirect
            
            try:
                with st.spinner("Lendo planilhas e aplicando marcas d'água... isso pode levar alguns minutos."):
                    
                    MAPA_ABAS = {
                        "blue": "Controle_BlueSolutions_Automação",
                        "criatech": "Controle_Criatech_Automação",
                        "soft": "Controle_SoftDados_Automação",
                        "softcomp": "Controle_SoftComp_Automação",
                        "DNA": "Controle_DNA_Automação",
                        "NCA": "Controle_NCA_Automação",
                        "GM": "Controle_GM_Automação",
                        "RM": "Controle_RM_Automação",
                        "PRIMO": "Controle_PRIMO_Automação"
                    }
                    
                    clientes_com_aba = {}
                    for cli in clientes_selecionados:
                        clientes_com_aba[cli] = MAPA_ABAS.get(cli)

                    resultados_pagos = processar_faturas_pagas(clientes_com_aba)
            finally:
                sys.stdout = old_stdout
            
            texto_status.success("🎉 Processamento de Pagos concluído!")
            with st.expander("📜 Log completo da execução"):
                st.code(log_redirect.text, language="bash")
            
            st.divider()
            st.subheader("📊 Relatório de Execução - PDFs Pagos")
            for cli, status in resultados_pagos.items():
                if "✅" in status:
                    st.success(f"**{cli.upper()}**: {status}")
                else:
                    st.error(f"**{cli.upper()}**: {status}")

# ==========================================
# MÓDULO 3: ACOMPANHAR PROTOCOLOS (COELBA)
# ==========================================
elif "Acompanhar Protocolos" in modulo_selecionado:
    st.markdown(
        "Esse robô faz login na Coelba, varre 'Acompanhe Suas Solicitações' de cada Unidade "
        "Consumidora e atualiza a planilha de Protocolos. Quando uma solicitação vira "
        "**Finalizada**, marca o alerta na planilha e tenta enviar um e-mail automático."
    )
    st.link_button(
        "📄 Abrir planilha de Protocolos",
        "https://docs.google.com/spreadsheets/d/1_tGDCBzQ_7Swwf4XKfWsU1mv0XHNRlB3jEZYpLaN-yk/edit",
        use_container_width=True,
    )
    st.info(
        "⚠️ Chamados de **falta de energia** não são consultáveis pela Agência Virtual "
        "(confirmado no manual da Coelba). Continue registrando esses na aba "
        "'Falta de Energia (Manual)' da planilha."
    )
    st.divider()

    # ---------- PAINEL VISUAL ----------
    st.subheader("📋 Visão Geral dos Protocolos")
    try:
        df_protocolos = carregar_dataframe_protocolos()
    except Exception as e:
        df_protocolos = None
        st.error(f"Não consegui carregar a planilha agora: {e}")

    if df_protocolos is not None and not df_protocolos.empty:
        situacao_col = df_protocolos["Situacao"] if "Situacao" in df_protocolos.columns else pd.Series(dtype=str)
        dias_col = df_protocolos["Dias p/ prazo"] if "Dias p/ prazo" in df_protocolos.columns else pd.Series(dtype=float)

        total = len(df_protocolos)
        finalizados = int((situacao_col == "Finalizada").sum())
        abertos = total - finalizados
        vencendo = int(((dias_col <= 3) & (dias_col >= 0)).sum())
        vencidos = int((dias_col < 0).sum())

        col_a, col_b, col_c, col_d = st.columns(4)
        col_a.metric("Total", total)
        col_b.metric("Em aberto", abertos)
        col_c.metric("Vencendo (≤3 dias)", vencendo)
        col_d.metric("Vencidos", vencidos)

        col_f1, col_f2, col_f3 = st.columns([1, 1, 2])
        with col_f1:
            opcoes_cliente = sorted(df_protocolos["Cliente"].unique()) if "Cliente" in df_protocolos.columns else []
            filtro_cliente = st.multiselect("Cliente:", opcoes_cliente, default=opcoes_cliente)
        with col_f2:
            filtro_situacao = st.selectbox("Situação:", ["Todas", "Em aberto", "Finalizadas"])
        with col_f3:
            busca = st.text_input("Buscar (nº, endereço, tipo...):", "")

        df_filtrado = df_protocolos
        if "Cliente" in df_filtrado.columns:
            df_filtrado = df_filtrado[df_filtrado["Cliente"].isin(filtro_cliente)]
        if filtro_situacao == "Em aberto":
            df_filtrado = df_filtrado[df_filtrado["Situacao"] != "Finalizada"]
        elif filtro_situacao == "Finalizadas":
            df_filtrado = df_filtrado[df_filtrado["Situacao"] == "Finalizada"]
        if busca:
            mascara = df_filtrado.apply(lambda linha: busca.lower() in " ".join(linha.astype(str)).lower(), axis=1)
            df_filtrado = df_filtrado[mascara]

        colunas_exibir = [
            "Cliente", "No da Solicitacao", "Tipo", "Descricao", "Situacao", "Prazo",
            "Dias p/ prazo", "Resultado", "Endereco", "NUC (UC real)", "Situacao interna", "Observacoes internas",
        ]
        colunas_exibir = [c for c in colunas_exibir if c in df_filtrado.columns]

        def cor_situacao(valor):
            if valor == "Finalizada":
                return "background-color: #CEEAD6; color: #0D5235"
            if valor == "Em andamento":
                return "background-color: #FDE293; color: #6B3F05"
            return ""

        def cor_prazo(valor):
            if pd.isna(valor):
                return ""
            if valor < 0:
                return "background-color: #F4C7C3; color: #8C1D18"
            if valor <= 3:
                return "background-color: #FEEFC3; color: #6B3F05"
            return ""

        # st.dataframe com Styler não respeita Styler.format() pra exibição (só
        # usa .map pra cor) — por isso convertemos o texto ANTES, separado do
        # cálculo de cor, que continua baseado no valor numérico original.
        dias_numericos = df_filtrado["Dias p/ prazo"] if "Dias p/ prazo" in colunas_exibir else None
        df_exibicao = df_filtrado[colunas_exibir].copy()
        if dias_numericos is not None:
            df_exibicao["Dias p/ prazo"] = dias_numericos.apply(lambda v: "" if pd.isna(v) else f"{int(v)} d")

        estilo = df_exibicao.style
        if "Situacao" in colunas_exibir:
            estilo = estilo.map(cor_situacao, subset=["Situacao"])
        if dias_numericos is not None:
            cores_prazo = dias_numericos.apply(cor_prazo)
            estilo = estilo.apply(lambda _: cores_prazo, subset=["Dias p/ prazo"])

        st.dataframe(estilo, use_container_width=True, height=450, hide_index=True)
        st.caption(f"Mostrando {len(df_filtrado)} de {total} protocolo(s).")
    else:
        st.info("Nenhum protocolo na planilha ainda. Rode a consulta abaixo pra começar a popular.")

    st.divider()

    if st.button("▶️ Iniciar Consulta de Protocolos", type="primary", use_container_width=True):
        if not clientes_selecionados:
            st.warning("⚠️ Selecione pelo menos um cliente para continuar.")
        else:
            st.info(f"Iniciando consulta de protocolos para: {', '.join(clientes_selecionados)}")

            texto_status = st.empty()
            caixa_log = st.empty()

            old_stdout = sys.stdout
            sys.stdout = StreamlitRedirect(caixa_log)

            clientes_login = {}
            resultados_protocolos = {}
            try:
                for cliente in clientes_selecionados:
                    try:
                        login_user = str(st.secrets[f"{cliente.upper()}_LOGIN_USER"])
                        login_password = str(st.secrets[f"{cliente.upper()}_LOGIN_PASSWORD"])
                        clientes_login[cliente] = (login_user, login_password)
                    except KeyError:
                        resultados_protocolos[cliente.upper()] = "❌ Falha (Dados faltando no Cofre/Secrets)"

                with st.spinner("O robô está consultando as solicitações na Coelba..."):
                    resultados_protocolos.update(processar_protocolos(clientes_login))
            finally:
                sys.stdout = old_stdout

            texto_status.success("🎉 Consulta de Protocolos concluída!")

            st.divider()
            st.subheader("📊 Relatório de Execução - Protocolos")
            for cli, status in resultados_protocolos.items():
                if "✅" in status:
                    st.success(f"**{cli}**: {status}")
                else:
                    st.error(f"**{cli}**: {status}")

                for img_name in [
                    f"erro_sem_token_protocolo_{cli.lower()}.png",
                    f"erro_botao_protocolo_{cli.lower()}.png",
                    f"erro_fatal_protocolo_{cli.lower()}.png",
                ]:
                    if os.path.exists(img_name):
                        st.error(f"📸 O robô travou nesta tela (Conta {cli}):")
                        st.image(img_name)
