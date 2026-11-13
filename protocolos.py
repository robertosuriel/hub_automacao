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

from extrator import configurar_driver, realizar_login_selenium_original, autenticar_google_sheets, SPREADSHEET_ID as CONTROLE_SPREADSHEET_ID

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
ABA_UCS_DEPARA = "UCs (de-para)"
CABECALHO_UCS_DEPARA = ["Cliente", "Codigo Cliente", "NUC", "Razao Social", "Endereco", "Bairro", "Municipio"]

# --- Abertura de reclamação (Fale Conosco > Ouvidoria > Reclamação na Agência
# Virtual) — fluxo completo mapeado ao vivo em 13/11 (logado como BLUE, até a
# tela de confirmação final). Passos reais da Coelba, nessa ordem: Protocolo
# (opcional) -> Manifestação (Motivo/Detalhe/Relato/Telefone) -> [só se Motivo
# = Faturas: Faturas (mês/vencimento/valor, VALIDADO contra fatura real via
# "Localizar fatura") -> Solicitar Leitura] -> E-mail (pra onde a resposta
# vai: e-mail de contato, e-mail da área logada, ou outro) -> Dados Adicionais
# (anexo opcional) -> Envio.
# ATENÇÃO — descoberta importante do teste ao vivo: clicar "Continuar" em
# "Dados Adicionais" (mesmo sem anexar nada) É O ENVIO FINAL, sem nenhuma
# tela de confirmação depois. Não existe um passo de revisar-antes-de-enviar;
# a Coelba trata esse clique como a manifestação enviada e já devolve o
# protocolo. Qualquer automação futura (ou teste manual) precisa tratar esse
# clique como irreversível.
# Ainda não existe automação que submeta isso pra Coelba de verdade — por
# enquanto o formulário do Hub só grava aqui, numa fila, pra alguém abrir
# manualmente até a automação de envio ficar pronta.
ABA_FILA_RECLAMACOES = "Fila de Reclamações"
# Estrutura revisada em 2026-11-13 com a equipe (reunião "Automatização de
# Abertura de Chamados"): o fluxo de seleção agora é Tipo de solicitação ->
# Beneficiária/Geradora -> busca (nome/NUC/usina) -> confirmação, podendo
# cobrir mais de uma UC por chamado quando elas são da mesma usina/CNPJ
# (a Coelba exige agrupar nesse caso). Uma linha nessa fila == um chamado
# real a abrir (já agrupado); "UCs Incluidas" guarda todas as UCs cobertas.
# "E-mail para Resposta" foi adicionado depois de confirmar ao vivo que a
# Coelba exige esse campo (passo "E-mail", antes de Dados Adicionais).
CABECALHO_FILA_RECLAMACOES = [
    "Status", "Data de Criação", "Criado por", "Tipo de Solicitação",
    "Beneficiária ou Geradora", "Cliente", "UCs Incluidas", "Usina Atrelada",
    "Motivo Principal", "Detalhes do Motivo", "Relato da Manifestação",
    "Telefone para Contato", "E-mail para Resposta", "Ponto de Referencia",
    "Protocolo Anterior", "Responsável por Abrir", "Observação",
    "Mes de Referencia", "Data de Vencimento", "Valor da Fatura", "Protocolo Coelba",
]
STATUS_FILA_PENDENTE = "Pendente de envio"

TIPOS_SOLICITACAO_RECLAMACAO = ["Ouvidoria (Reclamação)", "Falta de Energia"]

# Motivo Principal: as 18 opções exatas do dropdown da Ouvidoria. Detalhes do
# Motivo depende do Motivo Principal escolhido — mapeado em 13/11 via Claude
# in Chrome, logado como BLUE (ver sessão de mapeamento), lendo as opções
# reais do dropdown pra cada um dos 18 motivos.
MOTIVOS_PRINCIPAIS_RECLAMACAO = [
    "Alteração Cadastral", "Apresentação/ Entrega de Contas", "Atendimento",
    "Cobrança Indevida de Atividade Acessória", "Cobrança por Irregularidade",
    "Danos Elétricos", "Erro de Leitura", "Faturas",
    "Indisp. de Agência/Posto de Atendimento", "Interrupção do Fornecimento de Energia",
    "Outros", "Prazos", "Problemas de instalação interna na UC", "Suspensão Indevida",
    "Tarifas", "Tensão de Fornecimento", "Valores Mínimos Faturáveis", "Variação de Consumo",
]
DETALHES_MOTIVO_RECLAMACAO = {
    "Alteração Cadastral": [
        "Alt. Cad. incorretas ou não solicitadas", "Alteração contratual",
        "Alteração da Data de Vencimento", "Alterações cadastrais não efetivadas",
        "Dificuldades para Alterar Cadastro", "Erro cadastro inicial para ligação UC",
        "Troca de Titularidade",
    ],
    "Apresentação/ Entrega de Contas": [
        "2ª via não entregue/Decl. de Quit. Anual", "Contas entregues fora do prazo previsto",
        "Entrega aviso/reaviso na fatura", "Entrega fatura Molhada/rasgada/aberta",
        "Fatura não recebida", "Fatura não recebida no endereço postal",
        "Problema recorrente entrega da fatura",
    ],
    "Atendimento": [
        "Atend. canais digitais da Concessionária", "Conduta inadeq. Empregado/prestador serv",
        "Espera acima de 1 minuto no teleatend.", "Espera atend. presencial superior 45mim",
        "Estrutura inadequada no atend presencial", "Falta de acesso a canais de atendimento",
        "Falta de atendimento prioritário", "Falta material obrigatório no atend.Pres",
        "Informação Incorreta da concessionária", "NENHUMA DAS ANTERIORES",
        "Serviço mal executado",
    ],
    "Cobrança Indevida de Atividade Acessória": [
        "Aferição em medidor laboratório próprio", "Alug./cessão onerosa imóv/espaço físico",
        "Arrecad. convênios fatura de energia", "Compartilhamento de infraestrutura",
        "Consultoria de ativ. acessória Regulada", "Elab. Projeto, Constr., Manut. Rede, SE",
        "Inst. cogeração ( P&D/Eficiência Energ.)", "NENHUMA DAS ANTERIORES",
        "Operacion. serviço de crédito tributário", "Propaganda na fat. energia/pg eletronica",
        "Receb. Fatura terceiros estrutura próp.", "Serviços de comunicação dados",
    ],
    "Cobrança por Irregularidade": [
        "Cobrança a não titular em período irreg.", "Deficiência na caracterização da irreg.",
        "Erro na aplicação do inciso da cobrança", "Fat. da recup. de consumo sem comunicado",
        "Fat. da recup. de consumo sem TOI", "Inconsistência de valores cobrados",
        "Inconsistência período da irregularidade",
    ],
    "Danos Elétricos": [
        "Concessionária se exime ressarcir Art210", "Deficiência na caract. nexo causalidade",
        "Exigência para apresentar laudo", "Não aceitação de Laudo apresentado",
        "Recusa no cadastro de Ressarcimento Dano", "Ressarcimento não realizado no prazo",
        "Substituição Equip. de qualidade inferio", "Valor ressarcimento inferior ao devido",
    ],
    "Erro de Leitura": ["Erro Leitura do medidor", "Leitura GD"],
    "Faturas": [
        "Ausência de faturamento", "BANDEIRAS TARIFÁRIAS",
        "Bônus de redução voluntária no consumo", "Classificação tarifária incorreta",
        "Cob Outros Serviços/Exclusão ñ realizada", "Cobrança Arts. 115 ou 175 Res. 414/10",
        "Cobrança Indevida Déb Terceiros/Anterior", "Compensação GD", "Critérios de faturamento",
        "Defeito na impressão", "Encargos / tributos incorretos",
        "Falta/erro inform obrigatórias na fatura", "Faturamento GD",
        "Faturamento Incorreto/Valores Faturados", "FATURAMENTO MICROGERAÇÃO",
        "Intervalo de faturamento incorreto", "Multas, Juros e Out. taxas cobradas(CIP)",
        "NENHUMA DAS ANTERIORES", "Pagamento em duplicidade", "Parcelamento de Débitos",
        "Perda de benefício irrig./Baixa Renda", "Proporcionalização de tarifa",
        "Relatórios GD", "Remanejamento de Leitura", "Tarifa Branca",
        "Tarifa errada qdo. Da Reclassificação UC",
    ],
    "Indisp. de Agência/Posto de Atendimento": [
        "Agência / Posto de atendimento fechado", "Falta de agência / posto de atendimento",
        "Inexistência Pto Arrecada no Município",
    ],
    "Interrupção do Fornecimento de Energia": [
        "FALTA DE ENERGIA PONTUAL", "Interrupção Frequente do Fornecimento",
        "Interrupção programada",
    ],
    "Outros": [
        "Canais de atendimento não regulados", "Compensação descumprimento prazo ñ real",
        "CONEXÃO DE MICROGERAÇÃO", "Danos não elétricos", "Débito automático",
        "DEVOLUÇÃO DE VAL. ANTECIP. FINANCEIRA", "Fatura paga e não baixada",
        "Iluminação Pública", "Limpeza de Terreno", "Medição Eletrônica", "Negativação",
        "NENHUMA DAS ANTERIORES", "Outros não enquadrados demais tipologias",
        "Participação Financeira", "Poda de Árvore", "Poste em mau Estado",
        "Relocação de Poste", "Serviço não solicitado pelo titular",
        "Serviços Massificados", "Substituição de equipamento de rede",
    ],
    "Prazos": [
        "Aferição de Medidor", "Atraso da reclamação de 1ª instância", "Aumento de Carga",
        "Conexão GD", "Extensão de rede não atendida", "Ligação Nova não atendida no prazo",
        "Ligação provisória não atendida no prazo", "Mudança de Local de Medidor",
        "Religação não atendida no prazo", "Vistoria GD",
    ],
    "Problemas de instalação interna na UC": [
        "Defeito Instal interna Eficiencia Energ.", "Defeito Instalações internas de LPT",
        "Fuga de Corrente", "Reprovação do padrão de entrada da UC",
    ],
    "Suspensão Indevida": [
        "Corte UC Bx Renda s/observar o mín 30 d", "Corte UC com conta paga",
        "Corte UC com Débitos não reconhecidos", "Corte UC conta reclamação pendente",
        "Corte UC débito vencido após 90 dias", "Corte UC fora horário comercial/úteis",
        "Corte UC na vigência de aviso regulado", "Corte UC sem aviso regulado",
    ],
    "Tarifas": [
        "Preço unid. de energia elétrica/demanda", "Tarifas Homologadas pela Aneel",
    ],
    "Tensão de Fornecimento": [
        "Flutuação/Variação/Oscilação de Tensão", "Prazo para obras de adequação de tensão",
        "Qualidade do Fornecimento de Energia",
    ],
    "Valores Mínimos Faturáveis": [
        "Cobrança de disponibilidade durante susp", "Faturamento com base custo disponib.",
    ],
    "Variação de Consumo": [
        "Aferição do Medidor", "Consumo elevado", "Variação brusca de consumo",
    ],
}
# Motivos que, no site da Coelba, abrem um passo extra ("Faturas", antes de
# E-mail) pedindo mês de referência/vencimento/valor — e esse passo VALIDA
# os 3 campos contra uma fatura real (botão "Localizar fatura"; dado
# inventado trava com "Fatura não localizada"). O robô de envio real vai
# precisar do dado real da fatura, não dá pra inventar.
MOTIVOS_COM_PASSO_FATURA = {"Faturas"}

# Abas da planilha de controle (mesma usada pelo módulo de Faturas) com o
# de-para de usina geradora. "clienteContato" liga cada UC beneficiária (via
# código do cliente) ao nome da usina que gera a energia dela; "unidadesGeradoras"
# liga o nome da usina ao código do cliente DA USINA (que é, ela mesma, uma UC
# da conta na Coelba). Ver carregar_mapa_geradoras / identificar_geradora.
ABA_CLIENTE_CONTATO = "clienteContato"
ABA_UNIDADES_GERADORAS = "unidadesGeradoras"

# --- Colunas da aba "Protocolos" (nessa ordem, A..AH — 34 colunas) ---
# Definidas pela equipe (Leticia/Mariane/Monalisa/Andressa) numa aba "RASCUNHO"
# depois de alinhar o que precisava aparecer. Index 0-based.
COLUNA_CLIENTE = 0
COLUNA_RESPONSAVEL = 1                  # MANUAL (dropdown — ver RESPONSAVEIS)
COLUNA_RAZAO_SOCIAL = 2
COLUNA_CNPJ = 3
COLUNA_COD_GERADORA = 4
COLUNA_NUC_GERADORA = 5
COLUNA_ENDERECO_GERADORA = 6
COLUNA_COD_BENEFICIARIA = 7
COLUNA_NUC_BENEFICIARIA = 8
COLUNA_ENDERECO_BENEFICIARIA = 9
COLUNA_UC_REFERENCIA = 10
COLUNA_NUMERO_SOLICITACAO = 11
COLUNA_ALERTA = 12                      # automático (ver calcular_alerta) — e-mail/notificação fica pra depois
COLUNA_TIPO = 13
COLUNA_DESCRICAO = 14
COLUNA_DESCRICAO_INTERNA = 15           # MANUAL — resumo da equipe em linguagem própria
COLUNA_DATA_SOLICITACAO = 16
COLUNA_SITUACAO = 17
COLUNA_PRAZO = 18
COLUNA_DIAS_PRAZO = 19                  # fórmula (ver formula_dias_prazo)
COLUNA_HISTORICO_PRAZO = 20
COLUNA_ULTIMA_ATUALIZACAO = 21
COLUNA_ALERTA_ENVIADO = 22
COLUNA_DESCOBERTO_EM = 23
COLUNA_OBSERVACOES = 24
COLUNA_RESULTADO = 25
COLUNA_HISTORICO = 26
COLUNA_ENDERECO = 27
COLUNA_INSTANCIA = 28                   # MANUAL
COLUNA_PROTOCOLO_ORIGEM = 29            # MANUAL
COLUNA_RESOLVIDO = 30                   # MANUAL (checkbox)
COLUNA_SITUACAO_INTERNA = 31            # MANUAL (dropdown)
COLUNA_OBSERVACOES_INTERNAS = 32        # MANUAL
COLUNA_NUC_REAL = 33

TOTAL_COLUNAS = 34  # A..AH

CABECALHO_PROTOCOLOS = [
    "Cliente", "Responsavel", "Razao Social", "CNPJ",
    "Codigo Cliente Geradora", "NUC Geradora", "Endereco Geradora",
    "Codigo Cliente Beneficiaria", "NUC Beneficiaria", "Endereco Beneficiaria",
    "UC (referencia)", "No da Solicitacao", "Alerta", "Tipo", "Descricao",
    "Descricao Interna", "Data da Solicitacao", "Situacao", "Prazo",
    "Dias p/ prazo", "Hist. Prazo", "Ultima Atualizacao", "Alerta Enviado",
    "Descoberto em", "Observacoes", "Resultado", "Historico", "Endereco",
    "Instancia", "Protocolo origem", "Resolvido", "Situacao interna",
    "Observacoes internas", "NUC (UC real)",
]
assert len(CABECALHO_PROTOCOLOS) == TOTAL_COLUNAS

# Colunas manuais (editadas pela equipe nas abas por cliente). O robô lê essas
# colunas ANTES de sobrescrever a aba do cliente, pra não perder edição. Não
# são todas contíguas — sincroniza célula a célula (ver sincronizar_edicoes_do_cliente).
COLUNAS_MANUAIS_INDICE = {
    "responsavel": COLUNA_RESPONSAVEL,
    "descricao_interna": COLUNA_DESCRICAO_INTERNA,
    "instancia": COLUNA_INSTANCIA,
    "protocolo_origem": COLUNA_PROTOCOLO_ORIGEM,
    "resolvido": COLUNA_RESOLVIDO,
    "situacao_interna": COLUNA_SITUACAO_INTERNA,
    "observacoes_internas": COLUNA_OBSERVACOES_INTERNAS,
}

RESPONSAVEIS = ["-", "Leticia", "Mariane", "Monalisa", "Andressa"]

COR_HEADER = {"red": 0.043, "green": 0.42, "blue": 0.278}  # #0B6B47
COR_BANDA_PAR = {"red": 0.933, "green": 0.965, "blue": 0.945}
COR_BORDA = {"color": {"red": 0.85, "green": 0.85, "blue": 0.85}, "style": "SOLID"}
LARGURAS_COLUNAS = {
    COLUNA_CLIENTE: 110, COLUNA_RESPONSAVEL: 110, COLUNA_RAZAO_SOCIAL: 220, COLUNA_CNPJ: 130,
    COLUNA_COD_GERADORA: 150, COLUNA_NUC_GERADORA: 150, COLUNA_ENDERECO_GERADORA: 260,
    COLUNA_COD_BENEFICIARIA: 150, COLUNA_NUC_BENEFICIARIA: 150, COLUNA_ENDERECO_BENEFICIARIA: 260,
    COLUNA_UC_REFERENCIA: 110, COLUNA_NUMERO_SOLICITACAO: 130, COLUNA_ALERTA: 180,
    COLUNA_TIPO: 150, COLUNA_DESCRICAO: 170, COLUNA_DESCRICAO_INTERNA: 220,
    COLUNA_DATA_SOLICITACAO: 100, COLUNA_SITUACAO: 110, COLUNA_PRAZO: 100,
    COLUNA_DIAS_PRAZO: 100, COLUNA_HISTORICO_PRAZO: 280, COLUNA_ULTIMA_ATUALIZACAO: 130,
    COLUNA_ALERTA_ENVIADO: 100, COLUNA_DESCOBERTO_EM: 130, COLUNA_OBSERVACOES: 160,
    COLUNA_RESULTADO: 260, COLUNA_HISTORICO: 280, COLUNA_ENDERECO: 260,
    COLUNA_INSTANCIA: 80, COLUNA_PROTOCOLO_ORIGEM: 140, COLUNA_RESOLVIDO: 90,
    COLUNA_SITUACAO_INTERNA: 170, COLUNA_OBSERVACOES_INTERNAS: 260, COLUNA_NUC_REAL: 160,
}


def normalizar_numero(numero):
    """A API da Coelba às vezes devolve o mesmo número de solicitação com
    zeros à esquerda (ex.: '004101702012' num dia, '4101702012' noutro) —
    confirmado em produção, causava duplicata porque a chave de dedup não
    batia. Números de 12 dígitos (700002.../300002...) nunca têm esse
    problema por já estarem no tamanho máximo; só afeta os mais curtos.
    Remove zeros à esquerda pra sempre comparar/gravar na mesma forma.
    Também usada pra casar código de cliente entre a nossa lista de UCs e a
    planilha de controle externa (clienteContato/unidadesGeradoras), que usa
    o mesmo número sem padding."""
    numero = str(numero).strip()
    sem_zeros = numero.lstrip("0")
    return sem_zeros if sem_zeros else numero


def _coluna_letra(indice):
    """Converte um índice de coluna 0-indexed pra letra do Sheets (0->A, 25->Z, 26->AA...)."""
    letra = ""
    n = indice + 1
    while n > 0:
        n, resto = divmod(n - 1, 26)
        letra = chr(65 + resto) + letra
    return letra


def formula_dias_prazo(linha):
    """Fórmula da coluna "Dias p/ prazo". A planilha tem locale en_US
    (M/D/YYYY), mas gravamos Prazo como texto DD/MM/YYYY — se a gente deixar
    o Sheets 'adivinhar' o formato, ele não reconhece como data e a subtração
    direta (Prazo-TODAY()) quebra com #VALUE!. Por isso parseia o texto na mão
    (dia/mês/ano) em vez de depender do Sheets reconhecer a data sozinho —
    funciona com qualquer locale. Usa _coluna_letra() pras colunas de
    Situacao/Prazo em vez de letra fixa — já tomamos um susto antes com uma
    linha antiga carregando a fórmula velha depois que as colunas mudaram de
    posição."""
    col_situacao = _coluna_letra(COLUNA_SITUACAO)
    col_prazo = _coluna_letra(COLUNA_PRAZO)
    return (
        f'=IF(OR({col_situacao}{linha}="Finalizada",{col_prazo}{linha}="",{col_prazo}{linha}="00/00/0000"),"",'
        f'IFERROR(DATE(VALUE(RIGHT({col_prazo}{linha},4)),VALUE(MID({col_prazo}{linha},4,2)),VALUE(LEFT({col_prazo}{linha},2)))-TODAY(),""))'
    )


def _dias_ate_prazo(prazo_str, situacao):
    """Mesma lógica da fórmula do Sheets, mas em Python — usada pra calcular
    o texto de ALERTA no momento em que a linha é escrita (não dá pra
    depender do valor da fórmula, que só recalcula depois que a célula
    existe de verdade na planilha)."""
    if (situacao or "").strip().lower() == "finalizada":
        return None
    prazo_str = (prazo_str or "").strip()
    if not prazo_str or prazo_str == "00/00/0000":
        return None
    try:
        dia, mes, ano = prazo_str.split("/")
        data_prazo = datetime(int(ano), int(mes), int(dia))
        return (data_prazo.date() - datetime.now().date()).days
    except Exception:
        return None


def _contar_prorrogacoes(hist_prazo):
    return len([l for l in str(hist_prazo or "").split("\n") if l.strip()])


def calcular_alerta(situacao, prazo_str, hist_prazo):
    """Resumo automático e curto de alertas pra essa linha (pra ficar visível
    na planilha sem precisar abrir o painel). Por enquanto só texto — enviar
    e-mail/notificação a partir daqui fica pra decidir depois."""
    partes = []
    dias = _dias_ate_prazo(prazo_str, situacao)
    if dias is not None:
        if dias < 0:
            partes.append(f"Vencido há {abs(dias)}d")
        elif dias <= 3:
            partes.append(f"Vencendo em {dias}d")
    qtd_prorrogacoes = _contar_prorrogacoes(hist_prazo)
    if qtd_prorrogacoes > 4:
        partes.append(f"{qtd_prorrogacoes} prorrogações")
    return " / ".join(partes)


# --- APIs da Agência Virtual (mesmo backend usado pelo extrator.py pra faturas) ---

def _formatar_nuc(codigo_uc_aneel):
    """Converte o codigoUcAneel cru da API (ex: "000632012602803") pro
    formato que aparece na tela da Coelba (ex: "6.320.126.028-03"). Se o
    numero de digitos nao bater com o esperado (12, apos tirar zeros a
    esquerda), devolve cru em vez de arriscar formatar errado."""
    digitos = str(codigo_uc_aneel or "").lstrip("0")
    if len(digitos) != 12:
        return str(codigo_uc_aneel or "")
    return f"{digitos[0]}.{digitos[1:4]}.{digitos[4:7]}.{digitos[7:10]}-{digitos[10:12]}"


def obter_ucs_cliente(login_user, token_ne_se):
    """Lista de UCs da conta. Cada item traz 'uc' (codigo usado nas outras
    chamadas de API), 'nuc' (Numero da Unidade Consumidora formatado) e o
    endereco, pra permitir identificar qual UC e a dona de cada solicitacao
    (ver identificar_uc_beneficiaria) e qual e a usina geradora vinculada
    (ver identificar_geradora)."""
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
        resultado = []
        for item in r.json().get("ucs", []):
            local = item.get("local") or {}
            resultado.append({
                "uc": item.get("uc", ""),
                "nuc": _formatar_nuc(item.get("codigoUcAneel", "")),
                "nome_cliente": item.get("nomeCliente", ""),
                "endereco": local.get("endereco", ""),
                "bairro": local.get("bairro", ""),
                "municipio": local.get("municipio", ""),
            })
        return resultado
    except Exception as e:
        print(f"  ⚠️ Erro ao obter UCs: {e}")
        return []


def _normalizar_texto_endereco(texto):
    import re
    import unicodedata
    if not texto:
        return ""
    texto = unicodedata.normalize("NFKD", str(texto)).encode("ascii", "ignore").decode("ascii")
    texto = texto.upper()
    return re.sub(r"[^A-Z0-9]+", " ", texto).strip()


def _endereco_completo_uc(uc):
    partes = [uc.get("endereco", ""), uc.get("bairro", ""), uc.get("municipio", "")]
    return ", ".join(p for p in partes if p)


def identificar_uc_beneficiaria(endereco_solicitacao, ucs_info):
    """Casa o Endereco de uma solicitacao (vindo de pedidos-detalhe) contra a
    lista de UCs do cliente (cada uma com endereco vindo da API de UCs), pra
    achar a UC "beneficiária" — a UC real/consumidora dona da solicitação.
    Devolve o dict da UC (uc/nuc/endereco/...) se achar exatamente uma
    compatível; None se não achar nenhuma ou achar mais de uma (ambíguo) —
    melhor deixar em branco do que arriscar errado."""
    alvo = _normalizar_texto_endereco(endereco_solicitacao)
    if not alvo:
        return None
    candidatas = []
    for uc in ucs_info:
        endereco_uc = _normalizar_texto_endereco(
            f"{uc.get('endereco', '')} {uc.get('bairro', '')} {uc.get('municipio', '')}"
        )
        endereco_uc_curto = _normalizar_texto_endereco(uc.get("endereco", ""))
        if not endereco_uc_curto:
            continue
        if endereco_uc_curto in alvo or alvo in endereco_uc:
            candidatas.append(uc)
    nucs_unicos = {c["nuc"] for c in candidatas if c.get("nuc")}
    if len(nucs_unicos) == 1:
        return candidatas[0]
    return None


def carregar_mapa_geradoras(client_sheets):
    """Lê as abas 'clienteContato' e 'unidadesGeradoras' da planilha de
    controle (a mesma usada pelo módulo de Faturas) e monta dois mapas:
    código do cliente da UC beneficiária (normalizado, sem zeros à esquerda)
    -> nome da usina; e nome da usina -> código do cliente da UC GERADORA.
    Usado pra achar a usina geradora vinculada a uma solicitação, a partir
    da UC beneficiária dela (ver identificar_geradora). Se der erro (planilha
    fora do ar, aba renomeada etc), devolve mapas vazios — a geradora
    simplesmente fica em branco, não trava o resto do processamento."""
    try:
        planilha_controle = client_sheets.open_by_key(CONTROLE_SPREADSHEET_ID)

        valores_contato = planilha_controle.worksheet(ABA_CLIENTE_CONTATO).get_all_values()
        cab_contato = valores_contato[0]
        idx_codigo = cab_contato.index("contato do cliente")
        idx_usina = cab_contato.index("Usina")
        beneficiaria_para_usina = {}
        for linha in valores_contato[1:]:
            if len(linha) <= max(idx_codigo, idx_usina):
                continue
            codigo = normalizar_numero(linha[idx_codigo])
            usina = linha[idx_usina].strip()
            if codigo and usina:
                beneficiaria_para_usina[codigo] = usina

        valores_geradoras = planilha_controle.worksheet(ABA_UNIDADES_GERADORAS).get_all_values()
        # Essa aba tem uma linha de lixo antes do cabeçalho real (["F"]).
        idx_cab = next((i for i, linha in enumerate(valores_geradoras) if "codigo_cliente" in linha), None)
        usina_para_geradora = {}
        if idx_cab is not None:
            cab_g = valores_geradoras[idx_cab]
            idx_cod_g = cab_g.index("codigo_cliente")
            idx_usina_g = cab_g.index("Usina")
            for linha in valores_geradoras[idx_cab + 1:]:
                if len(linha) <= max(idx_cod_g, idx_usina_g):
                    continue
                usina = linha[idx_usina_g].strip()
                codigo_g = normalizar_numero(linha[idx_cod_g])
                if usina and codigo_g:
                    usina_para_geradora[usina] = codigo_g

        return beneficiaria_para_usina, usina_para_geradora
    except Exception as e:
        print(f"  ⚠️ Não consegui carregar o mapa de usinas geradoras: {e}")
        return {}, {}


def carregar_nomes_clientes(client_sheets):
    """Lê a aba 'clienteContato' e monta o mapa código do cliente (normalizado)
    -> nome_cliente (o nome da pessoa/empresa dona daquela UC específica, ex:
    'Udilã Cortes' — diferente da Razão Social da conta Sol, que é a mesma pra
    todas as UCs e não ajuda a distinguir uma da outra na busca do módulo de
    Abrir Reclamação). Mesma tolerância a erro de carregar_mapa_geradoras:
    aba fora do ar vira dict vazio, não trava o resto."""
    try:
        planilha_controle = client_sheets.open_by_key(CONTROLE_SPREADSHEET_ID)
        valores_contato = planilha_controle.worksheet(ABA_CLIENTE_CONTATO).get_all_values()
        cab_contato = valores_contato[0]
        idx_codigo = cab_contato.index("contato do cliente")
        idx_nome = cab_contato.index("nome_cliente")
        nomes = {}
        for linha in valores_contato[1:]:
            if len(linha) <= max(idx_codigo, idx_nome):
                continue
            codigo = normalizar_numero(linha[idx_codigo])
            nome = linha[idx_nome].strip()
            if codigo and nome:
                nomes[codigo] = nome
        return nomes
    except Exception as e:
        print(f"  ⚠️ Não consegui carregar os nomes de clientes: {e}")
        return {}


def identificar_geradora(codigo_beneficiaria, ucs_cliente, beneficiaria_para_usina, usina_para_geradora):
    """A partir do código do cliente da UC beneficiária (a UC real da
    solicitação), acha a UC geradora vinculada, cruzando clienteContato +
    unidadesGeradoras + a lista de UCs do cliente (a geradora também é uma
    UC da mesma conta na Coelba). Devolve o dict da UC geradora (uc/nuc/
    endereco/...) ou None se a cadeia falhar em algum passo — melhor em
    branco do que errado."""
    if not codigo_beneficiaria:
        return None
    codigo_norm = normalizar_numero(codigo_beneficiaria)
    usina = beneficiaria_para_usina.get(codigo_norm, "")
    if not usina:
        return None
    codigo_geradora = usina_para_geradora.get(usina, "")
    if not codigo_geradora:
        return None
    for uc in ucs_cliente:
        if normalizar_numero(uc.get("uc", "")) == codigo_geradora:
            return uc
    return None


def identificar_geradora_e_beneficiaria(endereco_solicitacao, ucs_cliente, beneficiaria_para_usina, usina_para_geradora):
    """Função de conveniência: a partir do endereço de uma solicitação,
    devolve um dict com os 7 campos derivados (geradora e beneficiária:
    código, NUC, endereço; mais o NUC "real" mantido por compatibilidade
    com a coluna antiga). Campos ficam em branco quando a cadeia de match
    falha em algum passo — sempre melhor branco do que errado."""
    resultado = {
        "cod_geradora": "", "nuc_geradora": "", "endereco_geradora": "",
        "cod_beneficiaria": "", "nuc_beneficiaria": "", "endereco_beneficiaria": "",
        "nuc_real": "",
    }
    uc_beneficiaria = identificar_uc_beneficiaria(endereco_solicitacao, ucs_cliente)
    if not uc_beneficiaria:
        return resultado
    resultado["nuc_real"] = uc_beneficiaria.get("nuc", "")
    resultado["cod_beneficiaria"] = uc_beneficiaria.get("uc", "")
    resultado["nuc_beneficiaria"] = uc_beneficiaria.get("nuc", "")
    resultado["endereco_beneficiaria"] = _endereco_completo_uc(uc_beneficiaria)

    uc_geradora = identificar_geradora(uc_beneficiaria.get("uc", ""), ucs_cliente, beneficiaria_para_usina, usina_para_geradora)
    if uc_geradora:
        resultado["cod_geradora"] = uc_geradora.get("uc", "")
        resultado["nuc_geradora"] = uc_geradora.get("nuc", "")
        resultado["endereco_geradora"] = _endereco_completo_uc(uc_geradora)
    return resultado


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
    """Checkbox (Resolvido), dropdown (Situacao interna) e dropdown
    (Responsavel) só nas linhas indicadas. NUNCA aplicar isso adiantado em
    linhas sem dado — o Sheets grava 'FALSE' de verdade em célula de checkbox
    vazia, e isso já confundiu a detecção de fim de tabela do Sheets uma vez
    (linhas novas caindo centenas de linhas abaixo do esperado)."""
    try:
        client = autenticar_google_sheets()
        planilha = client.open_by_key(PROTOCOLOS_SPREADSHEET_ID)
        sheet_id = aba.id
        planilha.batch_update({"requests": [
            {
                "setDataValidation": {
                    "range": {"sheetId": sheet_id, "startRowIndex": linha_inicio - 1, "endRowIndex": linha_fim, "startColumnIndex": COLUNA_RESOLVIDO, "endColumnIndex": COLUNA_RESOLVIDO + 1},
                    "rule": {"condition": {"type": "BOOLEAN"}, "strict": True},
                }
            },
            {
                "setDataValidation": {
                    "range": {"sheetId": sheet_id, "startRowIndex": linha_inicio - 1, "endRowIndex": linha_fim, "startColumnIndex": COLUNA_SITUACAO_INTERNA, "endColumnIndex": COLUNA_SITUACAO_INTERNA + 1},
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
            {
                "setDataValidation": {
                    "range": {"sheetId": sheet_id, "startRowIndex": linha_inicio - 1, "endRowIndex": linha_fim, "startColumnIndex": COLUNA_RESPONSAVEL, "endColumnIndex": COLUNA_RESPONSAVEL + 1},
                    "rule": {
                        "condition": {
                            "type": "ONE_OF_LIST",
                            "values": [{"userEnteredValue": nome} for nome in RESPONSAVEIS],
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
        filtro_existente = None
        for sheet in meta.get("sheets", []):
            if sheet.get("properties", {}).get("sheetId") == sheet_id:
                for banda in sheet.get("bandedRanges", []):
                    requests_batch.append({"deleteBanding": {"bandedRangeId": banda["bandedRangeId"]}})
                # Preserva os critérios de filtro que a equipe tiver configurado
                # na aba (ex: filtrar por um número de solicitação) — sem isso,
                # cada rodada do robô sobrescrevia o filtro com um "em branco",
                # apagando o que a pessoa tinha selecionado.
                filtro_existente = sheet.get("basicFilter")
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
                # SEM headerColor de propósito: esse range já começa na 1a linha
                # de DADOS (não tem o cabeçalho real dentro dele), mas o Sheets
                # trata a 1a linha de qualquer banded range com headerColor como
                # "linha de cabeçalho" e pinta ela por cima do zebrado — isso
                # pintava a linha 2 (1a linha de dados) igual ao cabeçalho real.
                "rowProperties": {"firstBandColor": {"red": 1, "green": 1, "blue": 1}, "secondBandColor": COR_BANDA_PAR},
            }}},
            {"setBasicFilter": {"filter": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": fim, "startColumnIndex": 0, "endColumnIndex": TOTAL_COLUNAS},
                **({"criteria": filtro_existente["criteria"]} if filtro_existente and filtro_existente.get("criteria") else {}),
                **({"sortSpecs": filtro_existente["sortSpecs"]} if filtro_existente and filtro_existente.get("sortSpecs") else {}),
            }}},
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


def _estilizar_aba_simples(planilha, aba, qtd_colunas, total_linhas, largura_colunas=180):
    """Formatação básica (cabeçalho, zebra, bordas, filtro, largura) pra abas
    auxiliares que não seguem o layout da 'Protocolos' (ex: UCs de-para)."""
    try:
        sheet_id = aba.id
        fim = max(total_linhas, 2)
        requests_batch = []
        meta = planilha.fetch_sheet_metadata()
        for sheet in meta.get("sheets", []):
            if sheet.get("properties", {}).get("sheetId") == sheet_id:
                for banda in sheet.get("bandedRanges", []):
                    requests_batch.append({"deleteBanding": {"bandedRangeId": banda["bandedRangeId"]}})
                break
        requests_batch += [
            {"updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1, "frozenColumnCount": 1}},
                "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount",
            }},
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": qtd_colunas},
                "cell": {"userEnteredFormat": {
                    "backgroundColor": COR_HEADER,
                    "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
                    "verticalAlignment": "MIDDLE",
                }},
                "fields": "userEnteredFormat(backgroundColor,textFormat,verticalAlignment)",
            }},
            {"addBanding": {"bandedRange": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": fim, "startColumnIndex": 0, "endColumnIndex": qtd_colunas},
                "rowProperties": {"firstBandColor": {"red": 1, "green": 1, "blue": 1}, "secondBandColor": COR_BANDA_PAR},
            }}},
            {"setBasicFilter": {"filter": {"range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": fim, "startColumnIndex": 0, "endColumnIndex": qtd_colunas}}}},
            {"updateBorders": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": fim, "startColumnIndex": 0, "endColumnIndex": qtd_colunas},
                "top": COR_BORDA, "bottom": COR_BORDA, "left": COR_BORDA, "right": COR_BORDA,
                "innerHorizontal": COR_BORDA, "innerVertical": COR_BORDA,
            }},
            {"updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": qtd_colunas},
                "properties": {"pixelSize": largura_colunas}, "fields": "pixelSize",
            }},
        ]
        planilha.batch_update({"requests": requests_batch})
    except Exception as e:
        print(f"  ⚠️ Não consegui estilizar a aba '{aba.title}': {e}")


def atualizar_aba_ucs_depara(planilha, cliente, ucs):
    """Escreve/atualiza, na aba 'UCs (de-para)', a lista de UCs desse cliente
    (código, NUC, razão social, endereço) vinda da API. Serve pra conferência
    manual do matching endereço→UC (ver identificar_uc_beneficiaria) e como
    base pro preenchimento retroativo de linhas antigas. Substitui só as
    linhas desse cliente a cada rodada (a UC pode ter mudado de endereço)."""
    nome = cliente.upper()
    try:
        aba = planilha.worksheet(ABA_UCS_DEPARA)
    except Exception:
        aba = planilha.add_worksheet(title=ABA_UCS_DEPARA, rows=200, cols=len(CABECALHO_UCS_DEPARA))
        aba.update(range_name="A1", values=[CABECALHO_UCS_DEPARA], value_input_option="RAW")

    valores = aba.get_all_values()
    if not valores:
        aba.update(range_name="A1", values=[CABECALHO_UCS_DEPARA], value_input_option="RAW")
        valores = [CABECALHO_UCS_DEPARA]

    linhas_outros_clientes = [linha for linha in valores[1:] if linha and linha[0].strip() != nome]
    linhas_novas = [
        [nome, uc.get("uc", ""), uc.get("nuc", ""), uc.get("nome_cliente", ""),
         uc.get("endereco", ""), uc.get("bairro", ""), uc.get("municipio", "")]
        for uc in ucs
    ]

    todas_linhas = linhas_outros_clientes + linhas_novas
    total_linhas_atuais = len(valores)
    if total_linhas_atuais > 1:
        aba.batch_clear([f"A2:{_coluna_letra(len(CABECALHO_UCS_DEPARA) - 1)}{total_linhas_atuais}"])
    if todas_linhas:
        aba.update(range_name="A2", values=todas_linhas, value_input_option="RAW")

    _estilizar_aba_simples(planilha, aba, len(CABECALHO_UCS_DEPARA), len(todas_linhas) + 1)


def registrar_reclamacao_na_fila(tipo_solicitacao, beneficiaria_ou_geradora, cliente, ucs_incluidas,
                                   usina, motivo_principal, detalhes_motivo, relato, telefone,
                                   email_resposta="", ponto_referencia="", protocolo_anterior="",
                                   responsavel="", observacao="", mes_referencia="", data_vencimento="",
                                   valor_fatura="", criado_por=""):
    """Grava um chamado rascunhado pelo formulário do Hub na aba 'Fila de
    Reclamações', com status 'Pendente de envio'. Uma chamada == um chamado
    real a abrir na Coelba; `ucs_incluidas` já vem com todas as UCs cobertas
    por ele (agrupadas pelo chamador quando são da mesma usina/CNPJ — a Coelba
    exige isso nesse caso). Ainda não existe automação que envie isso pra
    Coelba de verdade (ver nota em ABA_FILA_RECLAMACOES) — alguém da equipe
    precisa abrir manualmente até essa parte ficar pronta."""
    client = autenticar_google_sheets()
    planilha = client.open_by_key(PROTOCOLOS_SPREADSHEET_ID)
    try:
        aba = planilha.worksheet(ABA_FILA_RECLAMACOES)
        cabecalho_atual = aba.row_values(1)
        if cabecalho_atual != CABECALHO_FILA_RECLAMACOES:
            # Estrutura da fila mudou (ver histórico de ABA_FILA_RECLAMACOES) —
            # essa aba ainda não tem uso real em produção, então é seguro
            # limpar e recomeçar com o cabeçalho atual em vez de ficar com
            # colunas desalinhadas.
            aba.clear()
            aba.update(range_name="A1", values=[CABECALHO_FILA_RECLAMACOES], value_input_option="RAW")
    except Exception:
        aba = planilha.add_worksheet(title=ABA_FILA_RECLAMACOES, rows=200, cols=len(CABECALHO_FILA_RECLAMACOES))
        aba.update(range_name="A1", values=[CABECALHO_FILA_RECLAMACOES], value_input_option="RAW")

    linha = [
        STATUS_FILA_PENDENTE, datetime.now().strftime("%d/%m/%Y %H:%M"), criado_por,
        tipo_solicitacao, beneficiaria_ou_geradora, cliente.upper(), ucs_incluidas, usina,
        motivo_principal, detalhes_motivo, relato, telefone, email_resposta, ponto_referencia,
        protocolo_anterior, responsavel, observacao, mes_referencia, data_vencimento,
        valor_fatura, "",
    ]
    # RAW (não USER_ENTERED): essa aba não tem fórmula nenhuma, e USER_ENTERED
    # deixa o Sheets "ajudar" convertendo campos tipo UCs Incluidas em número e
    # comendo o zero à esquerda (007099424626 virava 7099424626).
    aba.append_rows([linha], value_input_option="RAW", table_range="A1")

    total_linhas = len(aba.get_all_values())
    _estilizar_aba_simples(planilha, aba, len(CABECALHO_FILA_RECLAMACOES), total_linhas, largura_colunas=160)
    return True


def construir_catalogo_ucs(client_sheets=None):
    """Monta o catálogo de UCs pra alimentar a busca do módulo 'Abrir
    Reclamação' (Nome Cliente / NUC / Usina Atrelada), a partir da aba
    'UCs (de-para)' (já preenchida pelos módulos de Faturas/Protocolos, sem
    precisar logar de novo na Coelba) cruzada com o mapa de geradoras e com
    os nomes reais de cliente (clienteContato!nome_cliente — a pessoa/empresa
    dona daquela UC específica, não a Razão Social da conta Sol, que é igual
    pra todas as UCs e não ajuda a achar uma UC na busca). Cada item: cliente,
    uc_codigo, nuc, razao_social, nome_cliente (nome real, pode vir vazio se
    a UC não estiver em clienteContato), endereco, usina (vazio se a UC não
    for beneficiária de nenhuma usina), eh_geradora (bool)."""
    client_sheets = client_sheets or autenticar_google_sheets()
    beneficiaria_para_usina, usina_para_geradora = carregar_mapa_geradoras(client_sheets)
    nomes_clientes = carregar_nomes_clientes(client_sheets)
    codigos_geradoras = set(usina_para_geradora.values())

    planilha = client_sheets.open_by_key(PROTOCOLOS_SPREADSHEET_ID)
    try:
        valores = planilha.worksheet(ABA_UCS_DEPARA).get_all_values()
    except Exception:
        return []

    catalogo = []
    for linha in valores[1:]:
        if len(linha) < len(CABECALHO_UCS_DEPARA) or not linha[0].strip():
            continue
        cliente, uc_codigo, nuc, razao_social, endereco, bairro, municipio = linha[:7]
        codigo_norm = normalizar_numero(uc_codigo)
        catalogo.append({
            "cliente": cliente,
            "uc_codigo": uc_codigo,
            "nuc": nuc,
            "razao_social": razao_social,
            "nome_cliente": nomes_clientes.get(codigo_norm, ""),
            "endereco": ", ".join(p for p in [endereco, bairro, municipio] if p),
            "usina": beneficiaria_para_usina.get(codigo_norm, ""),
            "eh_geradora": codigo_norm in codigos_geradoras,
        })
    return catalogo


def contar_chamados_por_uc(df_protocolos):
    """Conta quantas linhas da aba 'Protocolos' já referenciam cada UC (como
    beneficiária ou como geradora), pra mostrar 'Nº de chamados já abertos'
    na tela de abrir reclamação. Retorna dict normalizar_numero(codigo) -> int."""
    contagem = {}
    if df_protocolos is None or df_protocolos.empty:
        return contagem
    for coluna in ["Codigo Cliente Beneficiaria", "Codigo Cliente Geradora"]:
        if coluna not in df_protocolos.columns:
            continue
        for valor in df_protocolos[coluna]:
            codigo = normalizar_numero(str(valor)) if valor else ""
            if codigo:
                contagem[codigo] = contagem.get(codigo, 0) + 1
    return contagem


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
    edição manual feita lá (Responsavel, Descricao Interna, Instancia,
    Protocolo origem, Resolvido, Situacao interna, Observacoes internas) —
    ANTES de regenerar a aba do cliente, pra não perder o que a equipe editou
    desde a última rodada. Os índices manuais não são todos contíguos, então
    sincroniza célula a célula em vez de depender de um único range contíguo."""
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
        normalizar_numero(linha[COLUNA_NUMERO_SOLICITACAO]): i + 1
        for i, linha in enumerate(valores_protocolos)
        if i > 0 and len(linha) > COLUNA_NUMERO_SOLICITACAO and linha[0].strip() == nome and linha[COLUNA_NUMERO_SOLICITACAO].strip()
    }

    indices_manuais = sorted(COLUNAS_MANUAIS_INDICE.values())
    maior_indice_manual = indices_manuais[-1]

    updates = []
    for linha in valores_cliente[1:]:
        if len(linha) <= maior_indice_manual:
            continue
        numero = normalizar_numero(linha[COLUNA_NUMERO_SOLICITACAO]) if len(linha) > COLUNA_NUMERO_SOLICITACAO else ""
        linha_protocolos = linha_por_numero.get(numero)
        if not linha_protocolos:
            continue
        atual = valores_protocolos[linha_protocolos - 1]
        for idx in indices_manuais:
            valor_cliente = linha[idx] if len(linha) > idx else ""
            valor_atual = atual[idx] if len(atual) > idx else ""
            if valor_cliente != valor_atual:
                coluna = _coluna_letra(idx)
                updates.append({"range": f"{coluna}{linha_protocolos}", "values": [[valor_cliente]]})

    if updates:
        aba_protocolos.batch_update(updates, value_input_option="USER_ENTERED")
        print(f"  [{cliente}] {len(updates)} edição(ões) manual(is) sincronizada(s) da aba do cliente.")


def sincronizar_todos_os_clientes(clientes):
    """Roda sincronizar_edicoes_do_cliente pra cada cliente da lista, trazendo
    pra 'Protocolos' qualquer edição manual feita nas abas por cliente (ex:
    Responsavel, Situacao interna) — sem precisar logar na Coelba nem esperar
    a próxima consulta rodar. É o que faz o painel do Streamlit (que lê só a
    aba 'Protocolos') refletir uma edição feita na aba do cliente."""
    client = autenticar_google_sheets()
    planilha = client.open_by_key(PROTOCOLOS_SPREADSHEET_ID)
    aba = planilha.worksheet(ABA_PROTOCOLOS)
    for cliente in clientes:
        try:
            sincronizar_edicoes_do_cliente(aba, planilha, cliente)
        except Exception as e:
            print(f"  ⚠️ Erro ao sincronizar {cliente}: {e}")


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
        aba_cliente.batch_clear([f"A2:{_coluna_letra(TOTAL_COLUNAS - 1)}{total_linhas_atuais}"])
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
        if len(linha) <= COLUNA_NUMERO_SOLICITACAO:
            continue
        cliente = linha[0].strip()
        numero = normalizar_numero(linha[COLUNA_NUMERO_SOLICITACAO])
        if not cliente or not numero:
            continue
        index[(cliente, numero)] = {
            "linha": i + 1,
            "situacao": linha[COLUNA_SITUACAO].strip() if len(linha) > COLUNA_SITUACAO else "",
            "prazo": linha[COLUNA_PRAZO].strip() if len(linha) > COLUNA_PRAZO else "",
            "ultima_atualizacao": linha[COLUNA_ULTIMA_ATUALIZACAO].strip() if len(linha) > COLUNA_ULTIMA_ATUALIZACAO else "",
            "alerta_enviado": linha[COLUNA_ALERTA_ENVIADO].strip() if len(linha) > COLUNA_ALERTA_ENVIADO else "",
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
        if linha and len(linha) > COLUNA_NUMERO_SOLICITACAO and linha[COLUNA_NUMERO_SOLICITACAO].strip()
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
# O conteúdo da coluna "Alerta" (ver calcular_alerta) é independente disso —
# e-mail/notificação automática a partir dela ainda não foi decidido.

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
        uc_referencia = ucs[0]["uc"]
        razao_social = ucs[0].get("nome_cliente", "")
        try:
            atualizar_aba_ucs_depara(planilha, cliente, ucs)
        except Exception as e:
            print(f"  ⚠️ [{cliente}] Não consegui atualizar a aba 'UCs (de-para)': {e}")

        mapa_beneficiaria_usina, mapa_usina_geradora = carregar_mapa_geradoras(client_sheets)

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
        # atualizacoes_buffer: (linha, descricao, [situacao, prazo, ultima_atualizacao,
        # alerta_enviado], resultado, historico, endereco, hist_prazo, info_geradora, alerta_texto)
        atualizacoes_buffer = []
        agora = datetime.now().strftime("%d/%m/%Y %H:%M")
        vistos_nesta_rodada = set()
        # Protocolos abertos na lista recente que não mudaram de status —
        # serão checados via detalhe depois pra detectar mudança de prazo.
        abertos_sem_mudanca_status = []

        for s in solicitacoes_recentes:
            numero = normalizar_numero(s.get("numSolicitacao", ""))
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
                info = identificar_geradora_e_beneficiaria(detalhe.get("endereco", ""), ucs, mapa_beneficiaria_usina, mapa_usina_geradora)
                alerta_texto = calcular_alerta(situacao, detalhe.get("prazo", ""), "")
                linha_valores = [
                    cliente.upper(),                                  # Cliente
                    "",                                                # Responsavel (manual)
                    razao_social,                                      # Razao Social
                    login_user,                                        # CNPJ
                    info["cod_geradora"], info["nuc_geradora"], info["endereco_geradora"],
                    info["cod_beneficiaria"], info["nuc_beneficiaria"], info["endereco_beneficiaria"],
                    uc_referencia,                                      # UC (referencia)
                    numero,                                             # No da Solicitacao
                    alerta_texto,                                       # Alerta
                    tipo,                                               # Tipo
                    detalhe.get("descricao", ""),                       # Descricao
                    "",                                                 # Descricao Interna (manual)
                    data_solicitacao,                                   # Data da Solicitacao
                    situacao,                                           # Situacao
                    detalhe.get("prazo", ""),                           # Prazo
                    formula_dias_prazo(proxima_linha),                  # Dias p/ prazo (formula)
                    "",                                                 # Hist. Prazo
                    detalhe.get("ultima_atualizacao", "") or agora,     # Ultima Atualizacao
                    alerta_enviado,                                     # Alerta Enviado
                    agora,                                              # Descoberto em
                    "",                                                 # Observacoes
                    detalhe.get("resultado", ""),                       # Resultado
                    detalhe.get("historico", ""),                       # Historico
                    detalhe.get("endereco", ""),                        # Endereco
                    "1",                                                # Instancia (manual)
                    "",                                                 # Protocolo origem (manual)
                    "FALSE",                                            # Resolvido (manual)
                    "-",                                                # Situacao interna (manual)
                    "",                                                 # Observacoes internas (manual)
                    info["nuc_real"],                                   # NUC (UC real)
                ]
                assert len(linha_valores) == TOTAL_COLUNAS
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
                info = identificar_geradora_e_beneficiaria(detalhe.get("endereco", ""), ucs, mapa_beneficiaria_usina, mapa_usina_geradora)
                alerta_texto = calcular_alerta(situacao, novo_prazo or registro.get("prazo", ""), hist_prazo)
                atualizacoes_buffer.append((
                    registro["linha"],
                    detalhe.get("descricao", ""),
                    [situacao, novo_prazo, detalhe.get("ultima_atualizacao", "") or agora, alerta_enviado],
                    detalhe.get("resultado", ""),
                    detalhe.get("historico", ""),
                    detalhe.get("endereco", ""),
                    hist_prazo,
                    info,
                    alerta_texto,
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

                    # Registra mudança de prazo na coluna Hist. Prazo
                    hist_prazo = dados_antigos.get("historico_prazo", "")
                    if mudou_prazo:
                        log_prazo = f"[{agora}] Prazo alterado: {dados_antigos.get('prazo', '?')} → {novo_prazo}"
                        hist_prazo = f"{hist_prazo}\n{log_prazo}".strip() if hist_prazo else log_prazo
                        print(f"  📅 [{cliente}] Solicitação {numero}: prazo alterado de {dados_antigos.get('prazo', '?')} para {novo_prazo}")

                    info = identificar_geradora_e_beneficiaria(detalhe.get("endereco", ""), ucs, mapa_beneficiaria_usina, mapa_usina_geradora)
                    situacao_final = nova_situacao or dados_antigos["situacao"]
                    alerta_texto = calcular_alerta(situacao_final, novo_prazo or dados_antigos.get("prazo", ""), hist_prazo)

                    atualizacoes_buffer.append((
                        dados_antigos["linha"],
                        detalhe.get("descricao", ""),
                        [situacao_final, novo_prazo, nova_ult_atualizacao or agora, alerta_enviado],
                        detalhe.get("resultado", ""),
                        detalhe.get("historico", ""),
                        detalhe.get("endereco", ""),
                        hist_prazo,
                        info,
                        alerta_texto,
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
                (linha[0].strip(), normalizar_numero(linha[COLUNA_NUMERO_SOLICITACAO]))
                for linha in aba.get_all_values()[1:]
                if len(linha) > COLUNA_NUMERO_SOLICITACAO
            }
            antes = len(novas_linhas_buffer)
            novas_linhas_buffer = [
                linha for linha in novas_linhas_buffer
                if (linha[0], normalizar_numero(linha[COLUNA_NUMERO_SOLICITACAO])) not in chaves_existentes
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
            for linha, descricao, valores_situacao, resultado, historico, endereco, hist_prazo, info, alerta_texto in atualizacoes_buffer:
                situacao_v, prazo_v, ultima_v, alerta_env_v = valores_situacao
                if descricao:
                    updates.append({"range": f"{_coluna_letra(COLUNA_DESCRICAO)}{linha}", "values": [[descricao]]})
                updates.append({"range": f"{_coluna_letra(COLUNA_SITUACAO)}{linha}", "values": [[situacao_v]]})
                updates.append({"range": f"{_coluna_letra(COLUNA_PRAZO)}{linha}", "values": [[prazo_v]]})
                updates.append({"range": f"{_coluna_letra(COLUNA_ULTIMA_ATUALIZACAO)}{linha}", "values": [[ultima_v]]})
                updates.append({"range": f"{_coluna_letra(COLUNA_ALERTA_ENVIADO)}{linha}", "values": [[alerta_env_v]]})
                if hist_prazo:
                    updates.append({"range": f"{_coluna_letra(COLUNA_HISTORICO_PRAZO)}{linha}", "values": [[hist_prazo]]})
                if resultado:
                    updates.append({"range": f"{_coluna_letra(COLUNA_RESULTADO)}{linha}", "values": [[resultado]]})
                if historico:
                    updates.append({"range": f"{_coluna_letra(COLUNA_HISTORICO)}{linha}", "values": [[historico]]})
                if endereco:
                    updates.append({"range": f"{_coluna_letra(COLUNA_ENDERECO)}{linha}", "values": [[endereco]]})
                # Alerta é sempre reescrito (mesmo vazio), já que precisa
                # "desligar" quando deixa de ser verdade (ex: prazo deixou de
                # estar vencendo porque a Coelba empurrou pra frente).
                updates.append({"range": f"{_coluna_letra(COLUNA_ALERTA)}{linha}", "values": [[alerta_texto]]})
                if info.get("nuc_real"):
                    updates.append({"range": f"{_coluna_letra(COLUNA_NUC_REAL)}{linha}", "values": [[info['nuc_real']]]})
                if info.get("cod_beneficiaria"):
                    updates.append({"range": f"{_coluna_letra(COLUNA_COD_BENEFICIARIA)}{linha}", "values": [[info['cod_beneficiaria']]]})
                    updates.append({"range": f"{_coluna_letra(COLUNA_NUC_BENEFICIARIA)}{linha}", "values": [[info['nuc_beneficiaria']]]})
                    updates.append({"range": f"{_coluna_letra(COLUNA_ENDERECO_BENEFICIARIA)}{linha}", "values": [[info['endereco_beneficiaria']]]})
                if info.get("cod_geradora"):
                    updates.append({"range": f"{_coluna_letra(COLUNA_COD_GERADORA)}{linha}", "values": [[info['cod_geradora']]]})
                    updates.append({"range": f"{_coluna_letra(COLUNA_NUC_GERADORA)}{linha}", "values": [[info['nuc_geradora']]]})
                    updates.append({"range": f"{_coluna_letra(COLUNA_ENDERECO_GERADORA)}{linha}", "values": [[info['endereco_geradora']]]})
            aba.batch_update(updates, value_input_option="USER_ENTERED")

        regenerar_aba_cliente(aba, planilha, cliente)
        return True, novos, atualizados, alertas

    except Exception as e:
        print(f"  ❌ [{cliente}] Erro inesperado: {e}")
        return False, novos, atualizados, alertas


def backfillar_colunas_derivadas():
    """Preenche retroativamente as colunas calculadas (NUC/Geradora/
    Beneficiária/Alerta/Razão Social) nas linhas que já existiam antes dessas
    colunas, usando a aba 'UCs (de-para)' + a planilha de controle externa —
    sem precisar logar na Coelba de novo. Precisa que o robô já tenha rodado
    pelo menos uma vez pra cada cliente (pra 'UCs (de-para)' ter dado), senão
    o cliente é simplesmente ignorado. Roda sob demanda, não faz parte do
    fluxo normal."""
    client = autenticar_google_sheets()
    planilha = client.open_by_key(PROTOCOLOS_SPREADSHEET_ID)
    aba = planilha.worksheet(ABA_PROTOCOLOS)
    try:
        aba_depara = planilha.worksheet(ABA_UCS_DEPARA)
    except Exception:
        print("  ⚠️ Aba 'UCs (de-para)' ainda não existe — rode a consulta de protocolos pelo menos uma vez antes.")
        return 0

    valores_depara = aba_depara.get_all_values()
    ucs_por_cliente = {}
    for linha in valores_depara[1:]:
        if len(linha) < len(CABECALHO_UCS_DEPARA):
            continue
        cliente = linha[0].strip()
        ucs_por_cliente.setdefault(cliente, []).append({
            "uc": linha[1], "nuc": linha[2], "nome_cliente": linha[3],
            "endereco": linha[4], "bairro": linha[5], "municipio": linha[6],
        })

    mapa_beneficiaria_usina, mapa_usina_geradora = carregar_mapa_geradoras(client)

    valores = aba.get_all_values()
    updates = []
    clientes_afetados = set()
    for i, linha in enumerate(valores[1:], start=2):
        cliente = linha[0].strip() if linha else ""
        ucs_cliente = ucs_por_cliente.get(cliente)
        if not ucs_cliente:
            continue
        endereco = linha[COLUNA_ENDERECO] if len(linha) > COLUNA_ENDERECO else ""
        situacao = linha[COLUNA_SITUACAO] if len(linha) > COLUNA_SITUACAO else ""
        prazo = linha[COLUNA_PRAZO] if len(linha) > COLUNA_PRAZO else ""
        hist_prazo = linha[COLUNA_HISTORICO_PRAZO] if len(linha) > COLUNA_HISTORICO_PRAZO else ""

        nuc_atual = linha[COLUNA_NUC_REAL] if len(linha) > COLUNA_NUC_REAL else ""
        razao_atual = linha[COLUNA_RAZAO_SOCIAL] if len(linha) > COLUNA_RAZAO_SOCIAL else ""
        cod_benef_atual = linha[COLUNA_COD_BENEFICIARIA] if len(linha) > COLUNA_COD_BENEFICIARIA else ""
        cod_ger_atual = linha[COLUNA_COD_GERADORA] if len(linha) > COLUNA_COD_GERADORA else ""
        alerta_atual = linha[COLUNA_ALERTA] if len(linha) > COLUNA_ALERTA else ""

        if not nuc_atual or not cod_benef_atual or not cod_ger_atual:
            info = identificar_geradora_e_beneficiaria(endereco, ucs_cliente, mapa_beneficiaria_usina, mapa_usina_geradora)
            if not nuc_atual and info["nuc_real"]:
                updates.append({"range": f"{_coluna_letra(COLUNA_NUC_REAL)}{i}", "values": [[info["nuc_real"]]]})
                clientes_afetados.add(cliente)
            if not cod_benef_atual and info["cod_beneficiaria"]:
                updates.append({"range": f"{_coluna_letra(COLUNA_COD_BENEFICIARIA)}{i}", "values": [[info["cod_beneficiaria"]]]})
                updates.append({"range": f"{_coluna_letra(COLUNA_NUC_BENEFICIARIA)}{i}", "values": [[info["nuc_beneficiaria"]]]})
                updates.append({"range": f"{_coluna_letra(COLUNA_ENDERECO_BENEFICIARIA)}{i}", "values": [[info["endereco_beneficiaria"]]]})
                clientes_afetados.add(cliente)
            if not cod_ger_atual and info["cod_geradora"]:
                updates.append({"range": f"{_coluna_letra(COLUNA_COD_GERADORA)}{i}", "values": [[info["cod_geradora"]]]})
                updates.append({"range": f"{_coluna_letra(COLUNA_NUC_GERADORA)}{i}", "values": [[info["nuc_geradora"]]]})
                updates.append({"range": f"{_coluna_letra(COLUNA_ENDERECO_GERADORA)}{i}", "values": [[info["endereco_geradora"]]]})
                clientes_afetados.add(cliente)

        if not alerta_atual:
            alerta_texto = calcular_alerta(situacao, prazo, hist_prazo)
            if alerta_texto:
                updates.append({"range": f"{_coluna_letra(COLUNA_ALERTA)}{i}", "values": [[alerta_texto]]})
                clientes_afetados.add(cliente)

        nome_cliente = ucs_cliente[0].get("nome_cliente", "")
        if not razao_atual and nome_cliente:
            updates.append({"range": f"{_coluna_letra(COLUNA_RAZAO_SOCIAL)}{i}", "values": [[nome_cliente]]})
            clientes_afetados.add(cliente)

    if updates:
        aba.batch_update(updates, value_input_option="USER_ENTERED")

    # As abas por cliente são cópias estáticas da "Protocolos" — sem
    # regenerá-las aqui, o backfill fica invisível pra equipe (que só olha
    # a aba do próprio cliente, não a "Protocolos" inteira).
    for cliente in sorted(clientes_afetados):
        try:
            regenerar_aba_cliente(aba, planilha, cliente.lower())
        except Exception as e:
            print(f"  ⚠️ Não consegui regenerar a aba de {cliente}: {e}")

    print(f"  ✅ Backfill concluído: {len(updates)} célula(s) preenchida(s) em {len(clientes_afetados)} cliente(s).")
    return len(updates)


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
