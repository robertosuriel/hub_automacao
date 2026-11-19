# Handoff — Auditoria de geração (Target × Inversor × Coelba) para o Hub de Automação

Documento para a sessão do Claude Code que mantém o **hub_automacao** (Streamlit: https://solonline-automacao.streamlit.app/, código: `robertosuriel/hub_automacao/app.py`).

**Objetivo:** criar no hub uma função que gera, todo mês, o relatório **Target × Inversor × Coelba por usina**: uma imagem PNG e uma planilha editável no mesmo padrão dos arquivos anexos.

Até agora esse relatório foi feito à mão numa sessão do Claude, lendo PDFs e entrando nos portais dos inversores pelo navegador. No hub, os dados dos inversores devem vir **por API**. O resultado final tem que ser o mesmo.

Os arquivos de exemplo e o código usado estão junto deste documento, no mesmo zip (ver a seção 9).

---

## 1. Contexto do negócio

- **Quem é a Sol Online:** aluga placas solares para clientes, que são as unidades consumidoras. A Sol Online paga a conta da Coelba (Neoenergia) de cada cliente com a assinatura que recebe dele.
- **De onde vem a energia:** as usinas (unidades geradoras) injetam energia na rede. A Coelba transforma essa energia em créditos e reparte entre as beneficiárias, conforme o % de rateio.
- **O que a auditoria quer descobrir:** se a Coelba está "comendo" créditos. Ou seja, se a energia injetada que ela registra para a usina, na fatura ou no relatório, é menor do que a usina realmente gerou, segundo o inversor.
- **Quem usa:** a Letícia (Sol Online) leva os casos para a Coelba. Há uma reunião quinzenal com o Magno, da Coelba, numa associação baiana de energia solar.

---

## 2. Fontes de dados

| Fonte | O que contém | Onde está |
|---|---|---|
| **Fatura da geradora** (PDF) | Período de leitura (leitura anterior e atual, nº de dias, próxima leitura), leituras do medidor e **energia injetada no mês** (bloco MMGD/SCEE) | Planilha "Controle de Pagamentos de Clientes" (id `1Ut5Y0LstIP7nhv7Jzyywc7SS7ObIPlO-3yEg-J8Pp5o`), abas `Controle_<EMPRESA>_Automação`, coluna K `Link_Fatura` (link do Drive). Filtrar `codigo_cliente` = conta da geradora e `mesReferencia` = `AAAA/MM`. Os PDFs ficam no Drive com o nome `AAAA/MM_00<codigo_cliente>_<numeroFatura>.pdf` |
| **Relatório de micro e minigeração da Coelba** (PDF) | Gerador (medidor, leitura de/até, injetado, saldo, acumulado), histórico de 13 meses, consumo das beneficiárias, resumo de saldo por UC, rateio | E-mail com assunto **"Relatórios Micro e Mini Geração"**, de `energiasolonline@gmail.com`, um PDF por geradora (`<EMPRESA> <codigo_cliente>.pdf`). O de outubro chegou em 11/11/2026 |
| **Target de geração** (xlsx) | Target diário de 2026 a 2037, por perfil de usina | E-mail do Gersio Misawa, "Target por usinas" (16/06/2026), anexo `Targets das Usinas_add 132.xlsx`. Colunas: C = ano, D = mês, E a AI = dias 1 a 31, AK = total do mês. Uma linha por (ano, mês), a partir da linha 5 |
| **Inversores** | Geração diária por usina | **iSolarCloud** (Sungrow), **SolisCloud** e **Fronius Solar.web**. Ver a seção 5 |

**Abas de controle das geradoras:** `Controle_BlueSolutions_Automação`, `_Criatech_`, `_NCA_`, `_GM_`, `_RM_`, `_DNA_` e `_SoftDados_`. A aba `unidadesGeradoras` lista codigo_cliente, usina e empresa.

---

## 3. Cadastro das usinas

| Usina | codigo_cliente | NUC (UC) | Portal | ID no portal | Aba de target (planilha do Gersio) | Grupo |
|---|---|---|---|---|---|---|
| Riachão | 7084200845 | 9.752.662.028-11 | Fronius Solar.web | pvSystemId `60ce51d8-0630-420d-a7ba-c2f54d2a4b91` | `Riachão_Min` e `Riachão_Max` (faixa) | 75/110 Riachão |
| Itaguaçu 1 | 7087961308 | 9.119.409.028-77 | iSolarCloud | Usina01 | `Ita 1_2_3` | 83/110 |
| Itaguaçu 2 | 7086422110 | 9.087.755.028-49 | iSolarCloud | Usina02 | `Ita 1_2_3` | 83/110 |
| Itaguaçu 3 | 7086396470 | 9.430.860.028-04 | iSolarCloud | Usina03 | `Ita 1_2_3` | 83/110 |
| Itaguaçu 4 | 7089712445 | 8.123.318.028-25 | iSolarCloud | Usina04 | `Ita 4_5_6` | 75/110 Itaguaçu |
| Itaguaçu 5 | 7090822300 | 9.784.044.028-91 | iSolarCloud | Usina05 | `Ita 4_5_6` | 75/110 Itaguaçu |
| Itaguaçu 6 | 7090898080 | 9.450.649.028-25 | iSolarCloud | Usina06 | `Ita 4_5_6` | 75/110 Itaguaçu |
| Itaguaçu 7 (nova) | 7098720140 | 8.789.620.028-84 | iSolarCloud | Usina07 | `Ita 7_8_9` | 75/110 Itaguaçu |
| Itaguaçu 8 | 7093708452 | 7.473.336.028-80 | iSolarCloud | Usina08 | `Ita 7_8_9` | 75/110 Itaguaçu |
| Itaguaçu 9 | 7093114668 | 9.130.142.028-82 | iSolarCloud | Usina09 | `Ita 7_8_9` | 75/110 Itaguaçu |
| Itaguaçu 10 | 7093802912 | 9.805.966.028-64 | iSolarCloud | Usina10 | `Ita 10` | 83/132 |
| Itaguaçu 11 | 7093807787 | 7.487.928.028-28 | iSolarCloud | Usina11 | `Ita 11` | 75/110 Itaguaçu |
| Itaguaçu 12 | 7093810311 | 7.157.945.028-10 | iSolarCloud | Usina12 | `Ita 12` | 83/110 |
| Itaguaçu 13 | 7095207043 | 9.474.729.028-02 | iSolarCloud | Usina13 | `Ita 13` | 83/132 |
| Itaguaçu 14 | 7095208228 | 7.808.050.028-05 | iSolarCloud | Usina14 | `Ita 14_15` | 83/110 |
| Itaguaçu 15 | 7095209127 | 8.157.393.028-43 | iSolarCloud | Usina15 | `Ita 14_15` | 83/110 |
| Itaguaçu 16 | 7099408493 | 7.177.027.028-96 | SolisCloud | station id `1298491919450633675` | `Ita 16_17_18` | 108/132 |
| Itaguaçu 17 | 7099472370 | 7.846.231.028-65 | SolisCloud | `1298491919450704148` | `Ita 16_17_18` | 108/132 |
| Itaguaçu 18 | 7099477835 | 7.847.121.028-70 | SolisCloud | `1298491919450704520` | `Ita 16_17_18` | 108/132 |
| Itaguaçu 19 | 7099508218 | 9.492.368.028-21 | iSolarCloud | Usina19 | `Xique-xique_Max 83kW 132kWp` (geral) | 83/132 |
| Itaguaçu 20 | 7102983682 | 9.895.736.028-60 | iSolarCloud | Usina20 | — (pendente) | 100/132 |
| Itaguaçu 21 | 7102413746 | 9.874.316.028-38 | iSolarCloud | Usina21 | `Xique-xique_Max 100kW 132kWp` (geral) | 100/132 |
| Itaguaçu 22 | 7101856725 | 9.854.858.028-08 | iSolarCloud | "Usina 22" (com espaço) | **não há target 110/132** | 110/132 |

Observações sobre o cadastro:
- No iSolarCloud existe também uma "Usina25", nova (conexão em 05/11/2026), ainda fora do relatório.
- Existe ainda a Itaguaçu 7 antiga (conta 7092251262), que não entra no relatório.
- Os IDs do iSolarCloud acima são os nomes das usinas na conta B2522210740. Na API, use o `ps_id` correspondente.

---

## 4. Regras de cálculo (validadas em setembro e outubro de 2026)

1. **Mês de referência:** usar a fatura `mesReferencia = AAAA/MM` de cada geradora.
   - **Exceção, Riachão:** o ciclo dela começa no início do mês, então usamos a fatura do mês seguinte. Em outubro foi a 2026/11, com ciclo de 06/10 a 05/11.
   - O relatório deve mostrar a coluna `mesReferencia`.
2. **Período somado** (inversor e target): do **dia seguinte à leitura anterior** até o **dia da leitura atual**, inclusive. Assim o número de dias somados é igual ao `N° DE DIAS` da fatura.
   - Exemplo: leitura anterior 24/09 e leitura atual 26/10 → soma de 25/09 a 26/10, 32 dias.
3. **Coelba:** é a energia injetada da **fatura da geradora** (`MMGD.Energia injetada no mes N kWh`).
   - Se a fatura não trouxer esse dado, usar o injetado do **relatório da Coelba** e marcar a origem como "relatório". Em outubro isso aconteceu com a Itaguaçu 5.
   - Nas usinas em que vieram os dois, fatura e relatório deram exatamente o mesmo valor em outubro.
4. **Target:** soma dos targets diários de 2026 da aba correspondente (seção 3), no mesmo período do item 2.
   - **Riachão:** mostrar a faixa entre `Riachão_Min` e `Riachão_Max`.
   - Recalculando setembro por essa regra, os 12 valores conferidos bateram 100% com a imagem de setembro.
5. **Indicadores:**
   - `Coelba ÷ Inversor` = injetado ÷ inversor. Quanto mais perto de 100%, mais consistente.
   - `Inversor × Target` = inversor ÷ target − 1.
   - `Coelba × Target` = injetado ÷ target − 1. Na Riachão vira uma faixa.
6. **Cores, sempre pelo `Coelba ÷ Inversor`:**
   - 🔴 vermelho, menos de 90%: possível perda de crédito ou erro de medição;
   - 🟡 amarelo, mais de 110%: quase sempre é o portal que perdeu dias de dado;
   - 🟢 verde, de 95% a 105%: normal;
   - sem cor nas faixas intermediárias;
   - cinza quando não há como comparar (falta injetado ou falta inversor).
7. **Dias sem dado no portal:** listar os dias faltando ou zerados no período e citar na observação. Isso explica a maioria dos amarelos.

---

## 5. Dados dos inversores: o que foi feito pelo navegador e o que fazer por API

**Como foi feito pelo navegador** (só como referência; é isso que deve ser trocado):
- **iSolarCloud** (`web3.isolarcloud.com.hk`): Relatório → "Relatório de geração de energia" → Personalizado, com um intervalo de datas. A tabela traz uma linha por usina com a geração do período.
  - Pegamos o total de 24/09 a 26/10 e ajustamos as bordas com consultas de um dia só (24/09, 24/10, 25/10, 26/10 e 27/10).
  - A API web interna é assinada e criptografada, então não foi usada.
- **SolisCloud:** página da usina → Dados operacionais → Mês. O app chama `POST /api/chart/station/month` com o corpo `{"id": <stationId>, "month": "AAAA-MM", ...}`, que devolve `data[].dateStr` e `data[].energy` em kWh por dia.
  - A chamada é assinada pelo app. Capturamos a resposta interceptando o XHR.
- **Fronius Solar.web:** `GET /Chart/GetChartNew?pvSystemId=<id>&year=AAAA&month=M&day=1&interval=month&view=production`, que funciona com o cookie da sessão. A série "Produção" vem como `[timestamp_ms, kWh]` por dia.

**Como fazer por API** (confirmar os detalhes na documentação de cada fabricante antes de implementar):
- **iSolarCloud:** a Sungrow tem uma OpenAPI oficial, com portal de desenvolvedor, `appkey` e chave de acesso, login do usuário e token. Ela tem endpoints para listar usinas (`ps_id`) e consultar a energia diária de cada usina num intervalo.
- **SolisCloud:** API oficial "SolisCloud Platform API". É preciso pedir `KeyID` e `KeySecret` ao suporte da Solis. As requisições são assinadas com HMAC-SHA1 nos cabeçalhos Content-MD5, Date e Authorization. Há endpoint de energia diária da estação por mês.
- **Fronius:** **Solar.web Query API (SWQAPI)**, com `AccessKeyId` e `AccessKeyValue` gerados no Solar.web; pode exigir plano Premium. Usar os dados agregados diários do pvSystem, canal de energia produzida.
- **Segurança:** credenciais em `st.secrets` ou variáveis de ambiente, nunca no código.
- **Saída esperada do módulo:** `{usina: {date: kWh}}`, ou seja, a série diária, e não só o total. Com ela dá para aplicar a regra do período (item 4.2) e detectar dias faltando (item 4.7).

---

## 6. Leitura dos PDFs

Os scripts estão no zip:
- **`extrai_fatura.py`:** recebe o texto da fatura e devolve um dicionário. Use `pdftotext -layout`, porque o `read_file_content` do Drive embaralha o texto.
  - Campos usados aqui: `uc`, `ref`, `leit_ant_data`, `leit_atu_data`, `dias`, `prox_leitura`, `medidor`, `leit_ant_kwh`, `leit_atu_kwh`, `injetado_kwh`. Também extrai compensado, saldo, excedente, TUSD GD II, multas e créditos, que servem para a auditoria das beneficiárias.
  - A regex principal do período é: `LEITURA ANTERIOR (dd/mm/aaaa) LEITURA ATUAL (dd/mm/aaaa) N° DE DIAS (n) PRÓXIMA LEITURA (dd/mm/aaaa)`.
- **`parse_relatorio_coelba.py`:** lê o relatório de micro e minigeração (texto do `pdftotext -layout`).
  - Linha do gerador: `Gerador <medidor> [UC] <de> <até> MM/AAAA <injetado> <saldo_ant> <acumulado>`.
  - Também lê o histórico de 13 meses, o rodapé de totais, o resumo de saldo/rateio e o consumo das beneficiárias.
- **`targets_ciclo.py`:** soma os targets diários da planilha do Gersio num período (`cyc(aba, leitura_anterior, leitura_atual)`), com o mapeamento usina → aba.

---

## 7. Saídas esperadas (ver os exemplos no zip)

1. **Planilha editável** (`Target_Inversor_Coelba_out2026.xlsx`), uma linha por usina, nesta ordem de colunas:
   - Usina, codigo_cliente, mesReferencia, NUC;
   - Leitura anterior, Leitura atual, Dias, Próxima leitura;
   - Injetado na fatura (kWh), Inversor no período (kWh), Portal do inversor;
   - Target (kWh), Relatório Coelba (kWh);
   - Coelba ÷ Inversor, Inversor × Target, Coelba × Target, Observação.

   Detalhes:
   - Os indicadores são **fórmulas**, para a Letícia poder editar.
   - As cores são **fixas** (não usar formatação condicional), porque o visualizador do WhatsApp e do Claude não mostra formatação condicional.
   - Os valores calculados das fórmulas precisam ficar salvos no arquivo (por exemplo, recalculando com LibreOffice headless).
   - Quando falta um dado, escrever um texto, como "Não veio na fatura" ou "Sem dado no portal", em vez de deixar a célula vazia.
2. **Imagem PNG** (`Target_Inversor_Coelba_out2026.png`) para o WhatsApp. Foi feita com HTML renderizado pelo Playwright.
   - Cabeçalho azul-escuro "SOL ONLINE"; tabela agrupada por grupo (seção 3).
   - Colunas: Usina, Portal, Leituras, Dias, Target, Inversor, Coelba, Inversor × Target, Coelba × Target e Coelba ÷ Inversor (como pílula colorida).
   - Embaixo: 3 cartões de resumo (vermelho, amarelo, verde), notas numeradas por usina e os critérios.
3. **Dossiê de uma usina problemática** (`Itaguacu16_Resumo_Coelba.pdf`), de uma página, para levar à Coelba:
   - comparação de vários meses;
   - comparação com usinas irmãs do mesmo grupo;
   - argumentos e pedido de aferição do medidor e revisão dos créditos.

   Seria bom o hub gerar esse dossiê automaticamente quando uma usina ficar vermelha em dois meses seguidos.

---

## 8. Resultados e achados até agora

**Setembro/2026** (imagem de 09/10, critério igual):
- Riachão: 22% a 31% abaixo do target.
- Itaguaçu 16: Coelba 16.964 kWh contra inversor 23.624 kWh (72%).
- Itaguaçu 21: −37% contra o target, no primeiro ciclo.
- Itaguaçu 20: sem dados.
- Itaguaçu 22: sem target.

**Outubro/2026** (Coelba ÷ Inversor):
- 🔴 **Itaguaçu 16: 70,7%.** Inversor 21.368 kWh, Coelba 15.101 kWh. O inversor ficou 4,8% acima do target e a Coelba 25,9% abaixo, e as irmãs 17 e 18 ficaram em 102% e 105%. É o segundo mês seguido, com cerca de 12.900 kWh de diferença no acumulado. Medidor 5254934625.
- 🔴 **Riachão: 83,0%.** Inversor 12.856 kWh, Coelba 10.671 kWh.
- 🟡 **Itaguaçu 22 (137%), 3 (122%) e 7 (118%):** falha do portal iSolarCloud, com dias perdidos.
- 🟢 **Treze usinas entre 95% e 105%.** Itaguaçu 5 em 94,5% (pelo relatório); Itaguaçu 8, 10 e 18 entre 105% e 107%.
- **Itaguaçu 15:** 21% a 23% abaixo do target pelos dois lados (inversor e Coelba), ou seja, é geração baixa e não medição.

**Problemas de dados encontrados** (o hub deve detectar e avisar):
- **Itaguaçu 6:** o `Link_Fatura` da 2026/10 na planilha de controle aponta para a fatura de **outra conta** (7099570690). Validar sempre que o `codigo_cliente` do nome do PDF bate com o da linha.
- **Itaguaçu 5:** a fatura de 2026/10 veio **sem** o bloco MMGD de energia injetada.
- **Itaguaçu 20:** primeiro ciclo, com leitura anterior 0. A fatura não mostra injeção (ainda não aparece como geradora na Coelba) e o portal só tem dados a partir de 29/09.
- **Relatórios que faltaram no e-mail de 11/11:** Itaguaçu 11, 12, 14, 17 e 20.
- **Portais sem dados em alguns dias:** Solis sem 20/10 (16, 17 e 18), 01/10 zerado (17) e sem 28/09 (18).
- **Problemas internos dos relatórios da Coelba**, vistos na auditoria de setembro:
  - histórico de 13 meses sem a linha da UC geradora;
  - injeção lançada na linha de uma beneficiária;
  - consumidores sem número de UC;
  - leitura anterior zerada;
  - usinas faltando na seção de consumo.

---

## 9. Outros trabalhos do mesmo período (contexto)

- **Set/out — auditoria dos 20 relatórios de geração da Coelba de setembro** contra os dados das faturas (PDF `Conferencia_relatorios_Coelba_set2026.pdf`). A injeção bateu, mas foram encontradas várias falhas internas, listadas no item 8.
- **Out — extração dos PDFs das faturas** para a planilha de controle, feita numa **cópia** "Controle de Pagamentos de Clientes (teste auditoria)" (id `1s6Zj4Q_W5dS5VFxwLAit5RqSLlx0_gO55gN8-b_x1a8`).
  - Novas colunas T:AS nas abas PRIMO e DNA: datas de leitura, medidor, consumo, compensado, excedente, saldo, injetado, TUSD GD II, multas e créditos.
- **Out — piloto de conciliação de créditos da DNA** (Itaguaçu 8 e 7 beneficiárias): abas "Relatórios Coelba", "Rateio Coelba" e "Conferência Itaguaçu 8".
  - Regra: crédito recebido no mês m = saldo(m) − saldo(m−1) + compensado(m), que deve ser igual a % rateio × injeção da geradora em m−1.
  - Bateu para 6 das 7 unidades. Paulo e MC tiveram divergências no relatório de agosto.
  - A aba de conferência ficou incompleta: só a primeira linha das beneficiárias tem fórmula.

**Arquivos no zip:** este documento, `extrai_fatura.py`, `parse_relatorio_coelba.py`, `targets_ciclo.py` e os exemplos de saída `Target_Inversor_Coelba_out2026.xlsx`, `Target_Inversor_Coelba_out2026.png` e `Itaguacu16_Resumo_Coelba.pdf`.

---

## 10. Sugestão de implementação no hub

1. **Cadastro de usinas** (tabela ou YAML) com os campos da seção 3: usina, codigo_cliente, NUC, empresa/aba, portal, id no portal, aba de target, grupo e regra de mês de referência.
2. **Conectores de inversor** (`inverters/sungrow.py`, `solis.py`, `fronius.py`) com a mesma interface `daily_energy(plant_id, start, end) -> dict[date, kWh]`, com cache.
3. **Coelba:**
   - buscar a fatura pela planilha de controle e pelo Drive (validando o codigo_cliente do arquivo) e extrair o período e o injetado;
   - buscar o relatório no Gmail, pelo assunto, como segunda fonte e para conferência.
4. **Target:** ler a planilha do Gersio uma vez e somar no período.
5. **Montagem:** aplicar as regras da seção 4 e gerar o xlsx (fórmulas, cores fixas e valores recalculados) e o PNG.
6. **Tela no Streamlit:**
   - escolher o mês, rodar, ver a tabela colorida e baixar o xlsx e o PNG;
   - mostrar alertas de dados faltando (seção 8) e de usinas vermelhas, com botão para gerar o dossiê.
7. **Depois:**
   - incluir usinas de terceiros (por exemplo, a do "Primo": o relatório dela repete 34.000 kWh há 4 meses);
   - expor o resultado para o "robozinho" de protocolos e para o app do cliente, por um endpoint próprio, já que a plataforma Horizon não aceita outras APIs de inversor.
