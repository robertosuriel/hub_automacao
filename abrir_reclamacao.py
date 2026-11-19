# -*- coding: utf-8 -*-
"""Automação real (Selenium) pra abrir Reclamação de Ouvidoria e Falta de
Energia na Coelba, clicando na UI de verdade — em vez de montar o corpo da
chamada de API diretamente.

Por quê via UI e não via API direta (como os outros módulos do Hub)? Porque
não temos o código interno (codificacao/tipificacao/grupoCodificacao) de
todos os 18 Motivos da Ouvidoria — só de 1 ("Outros"), capturado ao vivo.
Rodando pela UI, é a própria Coelba que traduz o texto escolhido pro código
certo; a gente só INTERCEPTA a chamada de rede (via CDP, sem precisar de
lib extra tipo selenium-wire) pra ir aprendendo esses códigos a cada envio
real, e registrar numa aba de referência ("Códigos Reclamação Capturados")
pra, no futuro, trocar por chamada de API direta nos casos já conhecidos.

⚠️ MAPEADO AO VIVO em 19/11 (Claude in Chrome, conta BLUE) até o penúltimo
passo de cada fluxo, SEM nunca clicar no botão de envio final — os
seletores abaixo não foram testados ponta-a-ponta com um envio de verdade.
A parte mais incerta é `navegar_ate_uc` (o clique na "seta" da UC na lista
de Unidades Consumidoras foi feito por coordenada na exploração, não por um
seletor confirmado). Testar com um caso de baixo risco, de preferência com
o Chrome visível (não headless) na primeira vez, antes de confiar nisso
sem supervisão.
"""
import json
import re
import time
import unicodedata

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from extrator import configurar_driver, realizar_login_selenium_original, _clicar


URL_HOME = "https://agenciavirtual.neoenergia.com/#/home"

# Motivos da Ouvidoria pros quais já sabemos que existe um passo extra de
# "Faturas" (que valida mês/vencimento/valor contra uma fatura real — ver
# protocolos.MOTIVOS_COM_PASSO_FATURA). Sem o dado real da fatura, não tem
# como passar por esse passo; melhor abortar do que adivinhar.
MOTIVOS_SEM_SUPORTE_AUTOMATICO = {"Faturas"}


# --- Interceptor de rede (CDP) ----------------------------------------------

_INTERCEPTOR_JS = """
window.__payloadsCapturados = window.__payloadsCapturados || [];
(function() {
  if (window.__fetchInterceptado) return;
  window.__fetchInterceptado = true;
  const origFetch = window.fetch;
  window.fetch = function(input, init) {
    try {
      const url = typeof input === 'string' ? input : (input && input.url) || '';
      if (url.indexOf('cadastrar-ouvidoria') !== -1 || url.indexOf('falta-energia') !== -1) {
        window.__payloadsCapturados.push({url: url, body: init && init.body});
      }
    } catch (e) {}
    return origFetch.apply(this, arguments);
  };
})();
"""


def instalar_interceptor(driver):
    """Injeta o interceptor de fetch ANTES de qualquer script da página rodar
    (CDP Page.addScriptToEvaluateOnNewDocument) — chamar logo depois de
    configurar_driver(), antes do primeiro driver.get()."""
    try:
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": _INTERCEPTOR_JS})
    except Exception as e:
        print(f"  ⚠️ Não consegui instalar o interceptor de payloads: {e}")


def pegar_payloads_capturados(driver):
    """Lê e esvazia a lista de payloads capturados pelo interceptor desde a
    última leitura. Cada item: {"url": ..., "body": dict-ou-None}."""
    try:
        payloads = driver.execute_script(
            "const p = window.__payloadsCapturados || []; window.__payloadsCapturados = []; return p;"
        )
        resultado = []
        for p in payloads or []:
            corpo = p.get("body")
            if isinstance(corpo, str):
                try:
                    corpo = json.loads(corpo)
                except Exception:
                    pass
            resultado.append({"url": p.get("url", ""), "body": corpo})
        return resultado
    except Exception as e:
        print(f"  ⚠️ Não consegui ler os payloads capturados: {e}")
        return []


def _sem_acento_e_seguro(texto):
    """Sanitiza o Relato: a Coelba rejeitou um relato de teste com hífen
    ('Este Relato não é válido!'). Troca pontuação arriscada por espaço e
    mantém só letras/números/acentuação comum e pontuação básica."""
    if not texto:
        return texto
    texto = re.sub(r"[-_/\\|~^]+", " ", texto)
    texto = re.sub(r"[^\w\sÀ-ÿ.,!?():ºª%]", " ", texto, flags=re.UNICODE)
    return re.sub(r"\s+", " ", texto).strip()


# --- Helpers de navegação (Angular Material por texto, não por id) --------

def _xp_mat_select_por_label(texto_label):
    return f"//mat-label[normalize-space(text())='{texto_label}']/ancestor::mat-form-field//mat-select"


def _xp_input_por_label(texto_label):
    return f"//mat-label[normalize-space(text())='{texto_label}']/ancestor::mat-form-field//input"


def _xp_textarea_por_label(texto_label):
    return f"//mat-label[normalize-space(text())='{texto_label}']/ancestor::mat-form-field//textarea"


def selecionar_mat_select(driver, texto_label, texto_opcao, timeout=15):
    """Abre o mat-select logo abaixo do <mat-label> com esse texto e clica na
    mat-option cujo texto bate exatamente. Usado pra Motivo Principal e
    Detalhes do Motivo — a Coelba decide o código interno a partir do texto
    escolhido, a gente não precisa saber o código."""
    select = WebDriverWait(driver, timeout).until(
        EC.element_to_be_clickable((By.XPATH, _xp_mat_select_por_label(texto_label)))
    )
    _clicar(driver, select)
    opcao = WebDriverWait(driver, timeout).until(
        EC.element_to_be_clickable((By.XPATH, f"//mat-option[normalize-space(.)='{texto_opcao}']"))
    )
    _clicar(driver, opcao)
    time.sleep(0.3)


def preencher_input_por_label(driver, texto_label, valor, timeout=15):
    campo = WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.XPATH, _xp_input_por_label(texto_label)))
    )
    campo.clear()
    campo.send_keys(valor)


def preencher_textarea_por_label(driver, texto_label, valor, timeout=15):
    campo = WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.XPATH, _xp_textarea_por_label(texto_label)))
    )
    campo.clear()
    campo.send_keys(valor)


def clicar_mat_radio_por_texto(driver, texto_contendo, timeout=15):
    el = WebDriverWait(driver, timeout).until(
        EC.element_to_be_clickable((By.XPATH, f"//mat-radio-button[contains(normalize-space(.),'{texto_contendo}')]"))
    )
    _clicar(driver, el)
    time.sleep(0.2)


def clicar_texto_exato(driver, texto, timeout=15):
    """Clica no elemento cujo texto bate exatamente — usado nos radios
    'simples' do fluxo de Falta de Energia (não são mat-radio-button)."""
    el = WebDriverWait(driver, timeout).until(
        EC.element_to_be_clickable((By.XPATH, f"//*[normalize-space(text())='{texto}']"))
    )
    _clicar(driver, el)
    time.sleep(0.2)


def _xp_botao(texto):
    """XPath de <button> pelo texto, sem diferenciar maiúsculas (o site pode
    ter 'CONTINUAR' no HTML e 'Continuar' só por CSS)."""
    baixo = "translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz')"
    return f"//button[contains({baixo},'{texto.lower()}') or contains(normalize-space(.),'{texto}')]"


def _continuar_habilitado(driver, espera=3):
    fim = time.time() + espera
    while time.time() < fim:
        try:
            for b in driver.find_elements(By.XPATH, _xp_botao("Continuar")):
                if b.is_displayed() and b.is_enabled() and b.get_attribute("disabled") is None                         and "disabled" not in (b.get_attribute("class") or ""):
                    return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


def selecionar_opcao_simples(driver, texto, timeout=15):
    """Marca uma opção de rádio 'simples' (Falta de Energia) e confirma que o
    botão Continuar ficou habilitado — clicar só no texto nem sempre marca o
    rádio. Tenta: (1) input[type=radio] mais próximo, (2) clique nativo no
    texto, (3) clique JS no texto. Se nenhuma funcionar, erra com o HTML real."""
    el = WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.XPATH, f"//*[normalize-space(text())='{texto}']"))
    )
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    time.sleep(0.3)
    estrategias = [
        lambda: driver.execute_script(
            "let n=arguments[0]; for(let i=0;i<6&&n;i++){let r=n.querySelector&&n.querySelector(\"input[type='radio']\");"
            "if(r){r.click();return true;} n=n.parentElement;} return false;", el),
        lambda: el.click(),
        lambda: driver.execute_script("arguments[0].click();", el),
    ]
    for estrategia in estrategias:
        try:
            estrategia()
        except Exception:
            continue
        marcado = driver.execute_script(
            "let r=arguments[0].closest('mat-radio-button'); return !!(r && r.classList.contains('mat-radio-checked'));", el)
        if marcado or _continuar_habilitado(driver):
            return
    html = driver.execute_script(
        "let n=arguments[0]; for(let i=0;i<3&&n.parentElement;i++) n=n.parentElement; return n.outerHTML;", el
    )
    raise ErroAbrirReclamacao(
        "opcao_nao_marcou",
        f"Cliquei em '{texto}' mas o Continuar não habilitou. HTML: {' '.join(html.split())[:700]}",
    )


def clicar_botao(driver, texto_contendo, timeout=15):
    el = WebDriverWait(driver, timeout).until(
        EC.element_to_be_clickable((By.XPATH, _xp_botao(texto_contendo)))
    )
    _clicar(driver, el)
    time.sleep(0.3)


def fechar_modal_se_existir(driver, texto_botao, timeout=5):
    """Clica num botão de modal com esse texto, se aparecer dentro do
    timeout. Não é erro se não aparecer — devolve False nesse caso."""
    try:
        el = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable((By.XPATH, _xp_botao(texto_botao)))
        )
        _clicar(driver, el)
        time.sleep(0.3)
        return True
    except Exception:
        return False


def _texto_da_pagina_contem(driver, trecho):
    try:
        return trecho in driver.find_element(By.TAG_NAME, "body").text
    except Exception:
        return False


class ErroAbrirReclamacao(Exception):
    """Erro esperado (não um bug) — ex: pedido duplicado, fatura não
    localizada. `motivo` é um código curto pra quem chamou decidir o que
    fazer; `detalhe` é a mensagem pra registrar na planilha."""
    def __init__(self, motivo, detalhe):
        super().__init__(detalhe)
        self.motivo = motivo
        self.detalhe = detalhe

def descrever_falha(driver, e):
    """Texto de diagnóstico pra gravar na planilha: tipo do erro, linha de
    abrir_reclamacao.py onde parou, URL atual e um trecho do texto da tela
    (o print salvo no servidor do Streamlit Cloud não dá pra abrir)."""
    import traceback
    partes = [type(e).__name__]
    try:
        quadros = [f for f in traceback.extract_tb(e.__traceback__) if f.filename.endswith("abrir_reclamacao.py")]
        if quadros:
            partes.append(f"em abrir_reclamacao.py:{quadros[-1].lineno} ({(quadros[-1].line or '').strip()[:90]})")
    except Exception:
        pass
    try:
        partes.append(f"URL: {driver.current_url}")
        texto = driver.find_element(By.TAG_NAME, "body").text
        texto = " ".join(texto.split())
        # Pula o cabeçalho (menu) e mostra o conteúdo útil, a partir da busca.
        pos = texto.find("Pesquisar")
        partes.append("TELA: " + (texto[pos:pos + 600] if pos >= 0 else texto[:600]))
        try:
            campo = driver.find_element(
                By.XPATH, "//input[contains(@placeholder,'Código do Cliente') or contains(@placeholder,'Unidade Consumidora')]"
            )
            partes.append(f"CAMPO BUSCA='{campo.get_attribute('value')}'")
        except Exception:
            pass
    except Exception:
        pass
    return " | ".join(partes)



# --- Navegação até a UC -----------------------------------------------------

def navegar_ate_uc(driver, uc_codigo, timeout=30, exigir_ligada=False):
    """A partir do login feito, entra no contexto de uma UC específica
    (necessário pra qualquer um dos dois fluxos). Elementos confirmados ao
    vivo (19/11): cada UC é um <app-uc-list-item> com um badge
    span.btn-status-imovel (LIGADA/DESLIGADA) e um mat-icon de seta
    (data-mat-icon-name='arrow_forward') que abre a UC.
    exigir_ligada=True (Falta de Energia): recusa UC DESLIGADA."""
    driver.get(URL_HOME)
    time.sleep(2)
    WebDriverWait(driver, timeout).until(lambda d: d.execute_script("return document.readyState") == "complete")

    # Se cair em "selecionar-estado" (sessão nova), escolhe Bahia.
    try:
        bahia = WebDriverWait(driver, 6).until(
            EC.element_to_be_clickable((By.XPATH, "//*[normalize-space(text())='Bahia']"))
        )
        _clicar(driver, bahia)
        time.sleep(3)
    except Exception:
        pass

    # A planilha perde os zeros à esquerda (Sheets trata como número); a Coelba
    # usa 12 dígitos (ex: 007087003102). Completa antes de buscar.
    uc_codigo = str(uc_codigo).strip()
    if uc_codigo.isdigit() and len(uc_codigo) < 12:
        uc_codigo = uc_codigo.zfill(12)

    xpath_campo = "//input[contains(@placeholder,'Código do Cliente') or contains(@placeholder,'Unidade Consumidora')]"
    xpath_item = f"//app-uc-list-item[contains(normalize-space(.),'{uc_codigo}')]"

    item = None
    for tentativa in (1, 2):
        # A Coelba às vezes não devolve a lista de UCs (bug de sessão): se a
        # lista não aparece, recarrega uma vez antes de desistir.
        try:
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "app-uc-list-item")))
        except Exception:
            if tentativa == 1:
                driver.refresh()
                time.sleep(4)
                continue
            raise ErroAbrirReclamacao(
                "sessao_sem_ucs",
                "A Coelba não devolveu a lista de Unidades Consumidoras (bug de sessão). "
                "Tente de novo em alguns minutos (a sessão nova costuma resolver).",
            )
        campo = WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.XPATH, xpath_campo)))
        campo.clear()
        campo.send_keys(uc_codigo)
        campo.send_keys(Keys.ENTER)
        try:
            item = WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH, xpath_item)))
            break
        except Exception:
            if tentativa == 2:
                raise ErroAbrirReclamacao(
                    "uc_nao_encontrada", f"A UC {uc_codigo} não apareceu na busca da Coelba com esse login."
                )
            driver.refresh()
            time.sleep(4)

    status = (item.get_attribute("textContent") or "").upper()
    if exigir_ligada and "DESLIGADA" in status:
        raise ErroAbrirReclamacao(
            "uc_desligada",
            f"A UC {uc_codigo} está DESLIGADA na Coelba — não faz sentido abrir Falta de Energia. "
            "Escolha uma UC ligada.",
        )

    try:
        seta = item.find_element(By.CSS_SELECTOR, "mat-icon[data-mat-icon-name='arrow_forward']")
    except Exception:
        icones = item.find_elements(By.TAG_NAME, "mat-icon")
        if not icones:
            raise ErroAbrirReclamacao("uc_nao_clicavel", f"Não achei a seta pra abrir a UC {uc_codigo}.")
        seta = icones[-1]
    _clicar(driver, seta)
    time.sleep(3)


# --- Fluxo: Ouvidoria (Reclamação) ------------------------------------------

def abrir_reclamacao_ouvidoria(driver, motivo_principal, detalhes_motivo, relato, telefone,
                                 email_resposta, protocolo_anterior=""):
    """Dirige o fluxo Fale Conosco > Ouvidoria > Reclamação até o envio real.
    Precisa já estar no contexto da UC certa (ver navegar_ate_uc). Levanta
    ErroAbrirReclamacao pra casos esperados (duplicado, motivo sem suporte).
    Devolve dict: {protocolo, numero_manifestacao, payload_capturado}."""
    if motivo_principal in MOTIVOS_SEM_SUPORTE_AUTOMATICO:
        raise ErroAbrirReclamacao(
            "motivo_sem_suporte",
            f"Motivo '{motivo_principal}' exige dados reais de fatura (passo 'Localizar fatura') — "
            "sem automação ainda, abrir manualmente.",
        )

    relato = _sem_acento_e_seguro(relato)

    # Fale Conosco > Ouvidoria
    menu = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable((By.XPATH, "//button[contains(.,'Fale Conosco')]"))
    )
    _clicar(driver, menu)
    time.sleep(0.5)
    clicar_texto_exato(driver, "Ouvidoria")
    time.sleep(2)

    # Passo 1: "Reclamação" + Continuar
    clicar_mat_radio_por_texto(driver, "Reclamação")
    clicar_botao(driver, "Continuar")
    time.sleep(1.5)

    # Passo Protocolo (opcional)
    if protocolo_anterior:
        preencher_input_por_label(driver, "", protocolo_anterior)  # campo sem mat-label fixo nesse passo
    clicar_botao(driver, "Continuar")
    time.sleep(1)
    fechar_modal_se_existir(driver, "Continuar", timeout=4)  # aviso de "não tenho protocolo"
    time.sleep(1.5)

    # Passo Manifestação
    selecionar_mat_select(driver, "MOTIVO PRINCIPAL", motivo_principal)
    selecionar_mat_select(driver, "DETALHES DO MOTIVO", detalhes_motivo)
    preencher_textarea_por_label(driver, "RELATO DA MANIFESTAÇÃO", relato)
    preencher_input_por_label(driver, "Telefone para Contato", telefone)
    clicar_botao(driver, "Continuar")
    time.sleep(1.5)

    if _texto_da_pagina_contem(driver, "já possui um pedido em andamento"):
        numero = ""
        try:
            numero = driver.find_element(By.XPATH, "//*[contains(text(),'Matifestação') or contains(text(),'Manifestação')]").text
        except Exception:
            pass
        fechar_modal_se_existir(driver, "Fechar", timeout=3)
        fechar_modal_se_existir(driver, "NÃO", timeout=3)  # mantém os dados preenchidos, não cancela
        raise ErroAbrirReclamacao(
            "pedido_em_andamento",
            f"Já existe uma manifestação em andamento pra essa UC+motivo. {numero}".strip(),
        )

    # Passo E-mail
    opcoes_email_conhecidas = ["LETICIASOLONLINE@GMAIL.COM", "financeiro@bluesolutionsgroup.com"]
    email_alvo = next((o for o in opcoes_email_conhecidas if o.lower() == (email_resposta or "").lower()), None)
    if email_alvo:
        clicar_mat_radio_por_texto(driver, email_alvo)
    else:
        clicar_mat_radio_por_texto(driver, "Quero receber em outro email")
        preencher_input_por_label(driver, "", email_resposta)
    clicar_botao(driver, "Continuar")
    time.sleep(1.5)

    # Passo Dados Adicionais -> Continuar AQUI é o envio real, sem confirmação depois.
    clicar_botao(driver, "Continuar")
    time.sleep(3)

    payloads = pegar_payloads_capturados(driver)

    protocolo, numero_manifestacao = "", ""
    try:
        protocolo = driver.find_element(By.XPATH, "//*[contains(text(),'PROTOCOLO')]/following::*[1]").text
    except Exception:
        pass
    try:
        numero_manifestacao = driver.find_element(
            By.XPATH, "//*[contains(text(),'Manifestação de Ouvidoria')]/following::*[1]"
        ).text
    except Exception:
        pass

    return {"protocolo": protocolo, "numero_manifestacao": numero_manifestacao, "payload_capturado": payloads}


# --- Fluxo: Falta de Energia -------------------------------------------------

def abrir_falta_energia(driver, telefone, ponto_referencia):
    """Dirige Para a Unidade Consumidora > Energia > Falta de Energia ou
    Iluminação Pública, usando sempre as duas respostas padrão da equipe
    (ver protocolos.PERGUNTAS_PADRAO_FALTA_ENERGIA) até o envio real.
    Precisa já estar no contexto da UC certa (ver navegar_ate_uc)."""
    # Rota confirmada ao vivo (19/11): #/home/servicos/falta-de-energia. Muda só
    # o hash (SPA), mantendo a UC selecionada; se a tela não abrir, cai no menu.
    driver.execute_script("window.location.hash = '#/home/servicos/falta-de-energia';")
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, "//*[contains(.,'Apenas na minha unidade consumidora')]"))
        )
    except Exception:
        menu = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, "//*[self::button or self::a][contains(.,'Para a Unidade Consumidora')]"))
        )
        _clicar(driver, menu)
        time.sleep(0.5)
        clicar_texto_exato(driver, "Falta de Energia ou Iluminação Pública")
        time.sleep(2)

    # "Onde é a falta de energia?" -> resposta padrão
    selecionar_opcao_simples(driver, "Não sei se é apenas na minha unidade consumidora.")
    clicar_botao(driver, "Continuar")
    time.sleep(1.5)

    # "Pode dizer o que você sabe sobre o problema?" -> resposta padrão
    selecionar_opcao_simples(driver, "Não sei.")
    clicar_botao(driver, "Continuar")
    time.sleep(1.5)

    # Dados de contato
    preencher_input_por_label(driver, "NUMERO DE CELULAR", telefone)
    preencher_input_por_label(driver, "PONTO DE REFERENCIA", ponto_referencia)
    clicar_botao(driver, "Continuar")
    time.sleep(1.5)

    # Confirmação -> Continuar AQUI é o envio real.
    clicar_botao(driver, "Continuar")
    time.sleep(3)

    payloads = pegar_payloads_capturados(driver)

    protocolo, ocorrencia, data_combinada = "", "", ""
    try:
        protocolo = driver.find_element(By.XPATH, "//*[contains(text(),'PROTOCOLO')]/following::*[1]").text
    except Exception:
        pass
    for payload in payloads:
        corpo_resposta = payload.get("body") or {}
        if isinstance(corpo_resposta, dict) and "ocorrencia" in corpo_resposta:
            ocorrencia = corpo_resposta.get("ocorrencia", "")
            data_combinada = corpo_resposta.get("dataCombinada", "")

    return {
        "protocolo": protocolo, "ocorrencia": ocorrencia, "data_combinada": data_combinada,
        "payload_capturado": payloads,
    }


# --- Orquestração: login + navegação até a UC -------------------------------

def logar_e_entrar_na_uc(login_user, login_password, cliente, uc_codigo, exigir_ligada=False):
    """Monta o driver, instala o interceptor, loga (reaproveitando a mesma
    rotina usada pelos módulos de Faturas/Protocolos) e entra no contexto da
    UC. Devolve o driver pronto pra chamar abrir_reclamacao_ouvidoria ou
    abrir_falta_energia; quem chamar é responsável por driver.quit()."""
    driver = configurar_driver()
    try:
        instalar_interceptor(driver)
        driver.get("https://agenciavirtual.neoenergia.com/#/login")
        time.sleep(4)
        WebDriverWait(driver, 30).until(lambda d: d.execute_script("return document.readyState") == "complete")

        token = realizar_login_selenium_original(driver, login_user, login_password, f"reclamacao_{cliente}")
        if not token:
            raise ErroAbrirReclamacao("login_falhou", f"Não consegui logar em {cliente}.")

        navegar_ate_uc(driver, uc_codigo, exigir_ligada=exigir_ligada)
        return driver
    except ErroAbrirReclamacao:
        driver.quit()
        raise
    except Exception as e:
        detalhe = descrever_falha(driver, e)
        try:
            driver.quit()
        except Exception:
            pass
        raise ErroAbrirReclamacao("falha_login_ou_navegacao", detalhe)
