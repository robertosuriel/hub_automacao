"""Tela do Streamlit: Auditoria de Geração (Target × Inversor × Coelba)."""
import os
from datetime import date, datetime

import pandas as pd
import streamlit as st

from .cadastro import USINAS
from .calculo import AMARELO, CINZA, NEUTRO, VERDE, VERMELHO, Linha, montar_linha, observacoes, periodo
from .faturas import carregar_controle, obter_fatura
from .inversores import coletar, montar_conectores
from .inversores.manual import SerieManual
from .pdftexto import pdf_para_texto
from .relatorio_coelba import parse_texto
from .saidas_png import gerar_png
from .saidas_xlsx import gerar_xlsx
from .targets import Targets

MESES = ["janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho", "agosto", "setembro", "outubro",
         "novembro", "dezembro"]
EMOJI = {VERMELHO: "🔴", AMARELO: "🟡", VERDE: "🟢", NEUTRO: "🔹", CINZA: "⚪"}

CRITERIOS = [
    "Coelba ÷ Inversor: quanto mais perto de 100%, mais consistente. Verde 95–105% • Vermelho < 90% • Amarelo > 110%.",
    "Inversor = soma diária do dia seguinte à leitura anterior até o dia da leitura atual (mesmo nº de dias da fatura).",
    "Coelba = energia injetada na fatura da geradora (se faltar, relatório da Coelba, marcado como relatório).",
    "Inversor × Target e Coelba × Target = (valor ÷ target − 1). Riachão: faixa Min–Max (fatura do mês seguinte).",
]


def _segredo(nome):
    try:
        if nome in st.secrets:
            return str(st.secrets[nome])
    except Exception:
        pass
    return os.getenv(nome, "")


def _relatorios(arquivos, ano, mes):
    """{codigo_cliente: injetado do mês} a partir dos PDFs de relatório da Coelba enviados."""
    out, avisos = {}, []
    for f in arquivos or []:
        try:
            d = parse_texto(pdf_para_texto(f.getvalue()), f.name)
            g = next((g for g in d["gens"] if (g["ano"], g["mes"]) == (ano, mes)), None)
            if d.get("conta") and g:
                out[d["conta"]] = g["inj"]
            else:
                avisos.append(f"Relatório {f.name}: sem linha do gerador em {mes:02d}/{ano}")
        except Exception as e:
            avisos.append(f"Relatório {f.name}: não consegui ler ({type(e).__name__})")
    return out, avisos


def _executar(ano, mes, arq_target, rels, csv_manual):
    from extrator import autenticar_drive
    from protocolos import autenticar_google_sheets

    avisos = []
    targets = Targets(arq_target, ano) if arq_target else None
    if not targets:
        avisos.append("Planilha de targets não enviada: colunas de target ficam vazias")
    rel, av = _relatorios(rels, ano, mes)
    avisos += av
    controle = carregar_controle(autenticar_google_sheets())
    drive = autenticar_drive()
    faturas, avisos_fat = {}, {}
    barra = st.progress(0, text="Lendo faturas…")
    for i, u in enumerate(USINAS):
        try:
            faturas[u.nome], avisos_fat[u.nome] = obter_fatura(u, ano, mes, controle, drive)
        except Exception as e:
            faturas[u.nome], avisos_fat[u.nome] = None, [f"{u.nome}: erro ao ler a fatura ({type(e).__name__}: {str(e)[:100]})"]
        barra.progress((i + 1) / len(USINAS), text=f"Fatura: {u.nome}")
    barra.empty()
    ini_fim = {n: periodo(f["leit_ant"], f["leit_atu"]) for n, f in faturas.items()
               if f and f["leit_ant"] and f["leit_ant"].year >= 2000}
    with st.spinner("Buscando geração nos inversores…"):
        series, av = coletar(USINAS, montar_conectores(_segredo), ini_fim, SerieManual(csv_manual) if csv_manual else None)
    avisos += av
    linhas = []
    for u in USINAS:
        av_u = avisos_fat.get(u.nome, [])
        linhas.append(montar_linha(u, faturas.get(u.nome), series.get(u.nome, {}), targets, rel.get(u.codigo_cliente), []))
        avisos += av_u
    return linhas, avisos


def _gerar_arquivos(linhas, ano, mes):
    titulo = f"{MESES[mes - 1]} {ano}"
    hoje = datetime.now().strftime("%d/%m/%Y")
    rodape = "\n".join(CRITERIOS + [f"Gerado pelo Hub de Automação em {hoje}."])
    xlsx = gerar_xlsx(linhas, f"{MESES[mes - 1].capitalize()} {ano}", rodape)
    png = gerar_png(linhas, titulo, f"Ciclo de leitura da fatura de {titulo} de cada geradora • gerado em {hoje}", CRITERIOS)
    return xlsx, png


def render():
    st.markdown("Gera o comparativo **Target × Inversor × Coelba** por usina: fatura (Drive) + inversores (API) + targets do Gersio.")
    hoje = date.today()
    ano_ref, mes_ref = (hoje.year - 1, 12) if hoje.month == 1 else (hoje.year, hoje.month - 1)
    c1, c2, c3 = st.columns([1, 1, 2])
    mes = c1.selectbox("Mês", range(1, 13), index=mes_ref - 1, format_func=lambda m: MESES[m - 1].capitalize())
    ano = c2.number_input("Ano", 2025, 2037, ano_ref)
    with c3:
        st.caption("Credenciais de inversor encontradas: " + (", ".join(montar_conectores(_segredo)) or "nenhuma (use o CSV manual)"))
    arq_target = st.file_uploader("Planilha de targets (Targets das Usinas…xlsx)", type="xlsx")
    rels = st.file_uploader("Relatórios de micro e minigeração da Coelba (PDF, opcional)", type="pdf", accept_multiple_files=True)
    csv_manual = st.file_uploader("Geração diária manual (CSV usina,data,kwh — opcional)", type="csv")

    if st.button("▶️ Gerar auditoria", type="primary", use_container_width=True):
        try:
            linhas, avisos = _executar(int(ano), int(mes), arq_target, rels, csv_manual.getvalue() if csv_manual else None)
            xlsx, png = _gerar_arquivos(linhas, int(ano), int(mes))
            st.session_state["aud_geracao"] = dict(linhas=linhas, avisos=avisos, xlsx=xlsx, png=png, ano=int(ano), mes=int(mes))
        except Exception as e:
            st.error(f"❌ Falha ao gerar a auditoria: {type(e).__name__}: {e}")

    r = st.session_state.get("aud_geracao")
    if not r:
        return
    linhas = r["linhas"]
    nome = f"Target_Inversor_Coelba_{MESES[r['mes'] - 1][:3]}{r['ano']}"
    d1, d2 = st.columns(2)
    d1.download_button("⬇️ Baixar planilha (xlsx)", r["xlsx"], f"{nome}.xlsx", use_container_width=True,
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    d2.download_button("⬇️ Baixar imagem (PNG)", r["png"], f"{nome}.png", mime="image/png", use_container_width=True)
    verm = [l for l in linhas if l.cor == VERMELHO]
    if verm:
        st.error("🔴 Possível perda de crédito: " + ", ".join(f"{l.usina.nome} ({l.razao:.0%})" for l in verm))
    if r["avisos"]:
        with st.expander(f"⚠️ {len(r['avisos'])} aviso(s) de dados", expanded=True):
            for a in r["avisos"]:
                st.write("• " + a)
    tabela = pd.DataFrame([{
        "": EMOJI.get(l.cor, "⚪"), "Usina": l.usina.nome, "Ref.": l.mes_ref, "Dias": l.dias,
        "Target": l.target, "Inversor": l.inversor, "Coelba": l.coelba, "Origem": l.origem_coelba,
        "Coelba ÷ Inversor": l.razao, "Inversor × Target": l.inv_x_target, "Coelba × Target": l.coelba_x_target,
        "Obs.": observacoes(l)} for l in linhas])
    st.dataframe(tabela, use_container_width=True, hide_index=True, column_config={
        "Target": st.column_config.NumberColumn(format="%.0f"), "Inversor": st.column_config.NumberColumn(format="%.0f"),
        "Coelba": st.column_config.NumberColumn(format="%.0f"),
        "Coelba ÷ Inversor": st.column_config.NumberColumn(format="percent"),
        "Inversor × Target": st.column_config.NumberColumn(format="percent"),
        "Coelba × Target": st.column_config.NumberColumn(format="percent")})
    st.image(r["png"], use_container_width=True)
