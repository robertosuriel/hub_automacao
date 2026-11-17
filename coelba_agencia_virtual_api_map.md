# Mapa de APIs e Estrutura — Agência Virtual Coelba / Neoenergia

> Referência técnica para o módulo de acompanhamento de protocolos/reclamações do `hub_automacao`.
> Levantado por engenharia reversa dos bundles Angular do portal (`runtime.*.js` → mapa de chunks → 81 chunks descompilados) + testes empíricos ao vivo com o token de sessão.
> Perfil usado nos testes: **multilogin PJ, `tipoPerfil=1`, distribuidora COELBA (Bahia)**. Data: 2026-10-02.

---

## 1. Fundamentos

| Item | Valor |
|---|---|
| **Host das APIs** | `https://apineprd.neoenergia.com` |
| **Portal (SPA Angular)** | `https://agenciavirtual.neoenergia.com` |
| **Gateway** | WSO2 (erros retornam `{"retorno":{"tipo":"WSO2",...}}` ou `{"fault":{"code":...}}`) |
| **Auth** | Header `Authorization: Bearer <accessToken>` + `Accept: application/json` (só isso) |
| **Fonte do token no portal** | `localStorage.token` (região NE) / `localStorage.tokenNeSe` (`.ne`/`.se`) |

### Parâmetros globais do perfil (capturados de chamadas reais 200)
```
canalSolicitante   = AGC
distribuidora      = COELBA        (exceto obterProtocolo → ver abaixo)
regiao             = NE
documentoSolicitante = {cnpj}      (44582015000180 na conta de teste)
tipoPerfil         = 1
usuario            = WSO2_CONEXAO  (constante do ambiente; NÃO é o CNPJ)
```
> ⚠️ `obterProtocolo` usa **`distribuidora=COEL`** (código legado diferente) e **não** aceita `usuario=WSO2_CONEXAO`.

---

## 2. Autenticação & sessão (dois tempos distintos)

### Token Bearer — **~1 hora**
- `iat` e `exp` com 60 min de diferença (confirmado; expirou durante o teste).
- **É o real limitante.** Renovar a cada execução / a cada ~50 min.

### `protocolo` de sessão — registro Salesforce, cacheável
- Criado por `GET /protocolo/1.1.0/obterProtocolo`. Resposta:
  `{ protocoloSalesforceStr, protocoloLegadoStr, protocoloSalesforce, protocoloLegado, retorno }`
- **Validado server-side**: protocolo inventado → `500 "Foreign key external ID ... not found"`.
- Gerado em **nível de conta** (sem precisar de UC) e **reutilizável em todas as UCs e por toda a sessão** (reusado em dezenas de chamadas 200).
- O número **codifica a data** (`20261002...`) → estável pelo menos no dia.
- **Recomendação:** cachear por dia; renovar só o token por hora.

---

## 3. Legenda de status de acesso

| Símbolo | Significado |
|---|---|
| ✅ | **Testado ao vivo → 200.** Acessível com o token web atual. |
| 🟡 | **Testado → 400** (acessível, mas faltou/errou parâmetro — não é bloqueio). |
| ⛔ | **Testado → 401/403.** Bloqueado para este token (escopo/subscription diferente). |
| ⚪ | **Mapeado só no código**, não testado. Mesma família de auth — provavelmente acessível, não confirmado. |

---

## 4. Endpoints núcleo do projeto (detalhado)

Base `multilogin/2.0.0/` abreviada como `{ML}` = `/multilogin/2.0.0/`.

### 4.1 Protocolo de sessão
```
✅ GET /protocolo/1.1.0/obterProtocolo
   params: distribuidora=COEL, canalSolicitante=AGC, documento={cnpj},
           regiao=NE, recaptchaAnl=false   (usuario e codCliente vazios p/ NE)
```

### 4.2 Listar UCs da conta (CNPJ)
```
✅ GET {ML}servicos/minha-conta
   params: canalSolicitante=AGC, distribuidora=COELBA, usuario={cnpj},
           tipoPerfil=1, documentoSolicitante={cnpj}
   → retorna todas as UCs do CNPJ (é a chamada que a tela "Unidades Consumidoras" faz)

⚪ GET /imoveis/1.1.0/clientes/{cnpj}/ucs      (variante alternativa, não testada)
⚪ GET {ML}servicos/imoveis/ucs/{uc}           (detalhe de 1 UC)
```

### 4.3 Acompanhamento de solicitações  ⭐ (coração do módulo)
Service: `core/services/acompanhamento-solicitacoes/acompanhamentosolicitacoes.service.ts`

```
✅ GET {ML}servicos/acompanhamento-solicitacoes/{cnpj}/pedidos-header
   OBRIGATÓRIOS: codigo={uc}, codCliente={uc}, protocolo={protocolo},
                 tipificacao=1011601, documentoSolicitante={cnpj}, usuario=WSO2_CONEXAO,
                 distribuidora=COELBA, canalSolicitante=AGC, regiao=NE,
                 tipoPerfil=1, geraSsOs=N, byPassActiv=(vazio)
   → { solicitacoes:[ {numSolicitacao, tipoSolicitacao, datSolicitacao,
                       statusSolicitacao, estrutura, protocolo, ...} ], retorno }
   estrutura "N" = solicitação real | "P" = log de "Contato" (ruído)

⛔ GET {ML}servicos/acompanhamento-solicitacoes/ucs/{uc}/pedidos-header   → 401
   (variante do fluxo de login por UC única; NÃO funciona no perfil multilogin)

✅ GET {ML}servicos/acompanhamento-solicitacoes/{numSolicitacao}/pedidos-detalhe
   OBRIGATÓRIOS: protocoloSolicitacao={num}, codigo={uc}, documento={cnpj},
                 documentoSolicitante={cnpj}, tipoPerfil=1, tipificacao=1010602,
                 distribuidora=COELBA, canalSolicitante=AGC, regiao=NE,
                 protocolo={protocolo}, usuario=WSO2_CONEXAO, tipoEstrutura=N
   → { situacao, numProtocolo, acompanhamentoDetalhado:[...], informacoes,
       backoffice, descricaoNota, tipologiaAtendimento, protocoloFilho, retorno }

⚪ GET {ML}servicos/acompanhamento-solicitacoes/ucs/{num}/pedidos-detalhes  (plural; não testado)
```

**Achados empíricos importantes (acompanhamento):**
1. O `{cnpj}` no path é **enganoso**: `codigo` (UC) é **obrigatório** — sem ele → `400 "O parâmetro 'codigo' é obrigatório!"`. O endpoint retorna **só as solicitações daquela UC**. (UC 7099002640 → 30 solicitações; outras UCs → 0.) **Não existe endpoint nível conta** → continuar iterando UCs.
2. **Atalho p/ o `pedidos-detalhe`:** o `codigo` **não precisa ser a UC "dona"** da solicitação — **qualquer UC válida da conta** funciona (provado: mesma `300002355207` retornou idêntica com UC 7099002640 e 7099424626). O backend valida pela conta/protocolo, não pelo par UC↔solicitação. → **Guarde a UC junto do `numSolicitacao` e atualize status chamando o detalhe direto, sem rerodar o header de todas as UCs.** Só não dá pra omitir o `codigo`.
3. **Duplicatas** (mesmo `numSolicitacao` em UCs diferentes): são solicitações nível-conta (ex.: "Reclamação Ouvidoria") que o backend associa a +1 UC. Não há endpoint de conta p/ eliminar na raiz → **manter a trava de dedup por `numSolicitacao`** (está correto).

### 4.4 Falta de energia / emergencial
Service: `core/services/falta-de-energia/falta-de-energia.service.ts` — classe `TipologiaFaltaEnergiaService`
Model: `core/models/falta-de-energia/falta-de-energia.ts`

```
⛔ GET  {ML}servicos/falta-energia/ucs/{uc}/falta-energia              (consultarFaltaEnergiaNE)
⛔ GET  {ML}servicos/falta-energia/ucs/{uc}/falta-energia-interrupcoes (consultarInterrupcoes)
⛔ GET  {ML}servicos/falta-energia/ucs/{uc}/falta-energia-ocorrencia   (consultarOcorrenciaNE)
✅ POST {ML}servicos/falta-energia/ucs/falta-energia                   (registrarFaltaEnergia)
```

> **Correção de 14/11**: a suspeita antiga de "produto WSO2 separado, sem
> acesso" era sobre os GETs com o token da conta BLUE — continua valendo
> pros GETs. Mas o **POST de registrar** foi confirmado funcionando ao vivo
> (200 OK) com o token da conta **NCA** (CNPJ 59186682000111, "NCA ENERGIA
> LTDA"), via DevTools durante um registro real. Então o POST de registrar
> falta de energia É acessível — pelo menos pra essa conta/escopo.

Params que o app monta (GET consulta/interrupções/ocorrência, ainda ⛔ não
confirmados):
```
canalSolicitante=AGC, codigo={uc}, distribuidora=COELBA, documentoSolicitante={cnpj},
regiao=NE, protocolo={protocolo}, usuario=WSO2_CONEXAO, tipoPerfil=1,
tipificacao={resolvida em runtime pelo serviço de parâmetros — valor não hardcoded}
```

**Corpo real do POST `registrarFaltaEnergia`** (capturado ao vivo via
DevTools, 14/11 — UC 007093810311, relato "Vizinhança e eu"):
```json
{
  "uc": "{uc}",
  "codigo": "",
  "documento": "{cnpj}",
  "documentoSolicitante": "{cnpj}",
  "nomeSolicitante": "{razão social da conta}",
  "observacoes": "{relato}",
  "telefoneContato": "{telefone}",
  "endereco": "{endereço, sem número}",
  "numeroEndereco": "{número do endereço}",
  "pontoReferencia": "{texto livre — nesse exemplo veio \"usina\"}",
  "motivoId": "10",
  "tipificacao": 1020901,
  "diaSemana": "6",
  "horaOscilacao": "",
  "atividade": "",
  "matriculaPlataforma": 18250,
  "canalSolicitante": "AGC",
  "distribuidora": "COELBA",
  "regiao": "NE",
  "protocolo": "{protocolo de sessão}",
  "usuario": "WSO2_CONEXAO",
  "tipoPerfil": 1
}
```
Resposta (200 OK):
```json
{"dataCombinada": "14/11/2026 08:44", "ocorrencia": "83343"}
```
Terminologia própria desse domínio: não devolve "protocolo/manifestação"
como a Ouvidoria — devolve **ocorrencia** (número do caso) e
**dataCombinada** (data/hora agendada, aparentemente pra verificação/visita).

⚠️ **Falta descobrir**: `motivoId` ("10") e `tipificacao` (1020901, formato
numérico aqui — diferente da Ouvidoria que usa string "1020199") são os
códigos do motivo de falta de energia escolhido na tela; não sei ainda o
texto correspondente nem quantas opções de motivo existem pra Falta de
Energia (o módulo do Hub só tem o Motivo Principal/Detalhe da Ouvidoria
mapeado — Falta de Energia usa outra taxonomia própria, "motivoId", não os
18 Motivos da Ouvidoria). `diaSemana`, `horaOscilacao`, `atividade` e
`matriculaPlataforma` também são campos que o Hub ainda não coleta nesse
fluxo — `atividade` e `horaOscilacao` vieram vazios nesse exemplo (talvez
opcionais ou condicionais a outro campo). Não tem campo de e-mail nesse
payload (diferente da Ouvidoria) — sugere que Falta de Energia não tem o
passo "E-mail" no wizard.

### 4.5 Histórico de consumo
```
⚪ GET {ML}servicos/historicos/ucs/{uc}/consumos
⚪ GET {ML}servicos/historicos/ucs/{uc}/consumos/grupo-a
```

### 4.6 Ouvidoria / Fale Conosco / Reclamações (relacionados a protocolos)
```
✅ GET  /ouvidoria/1.0.0/consultar-ouvidoria
   params: distribuidora=COELBA, canalSolicitante=AGC, usuario=WSO2_CONEXAO,
           numCliente={id interno, != codigo da UC — ver nota abaixo},
           manifestacao=05 (código fixo observado p/ "Reclamação"),
           byPassActiv=X, codigo={uc}, tipoPerfil=1, documentoSolicitante={cnpj}
   → confirmado em 13/11 (DevTools ao vivo, fluxo real de abrir reclamação).
     É essa chamada (rodada ao abrir a tela "Manifestação") que detecta
     duplicata: se já existe uma manifestação em andamento pro mesmo
     UC+motivo, o front mostra "Identificamos que já possui um pedido em
     andamento" com o número da manifestação — a Coelba BLOQUEIA abrir
     outra pro mesmo caso enquanto a anterior não for concluída.

✅ POST {ML}servicos/ouvidoria/cadastrar-ouvidoria
   → CONFIRMADO em 13/11: é o endpoint real que registra a reclamação
     (200 OK, devolve protocolo/manifestação novos). Vem do bundle
     `main.*.js` (não é chunk lazy separado).

   Corpo da requisição (capturado ao vivo via DevTools, 13/11, time abrindo
   um chamado real na conta NCA — CNPJ 59186682000111, "NCA ENERGIA LTDA" —
   Motivo Principal "Outros", Detalhe provavelmente "Compensação
   descumprimento prazo ñ real" — não confirmado ainda; relato: "compensação
   de créditos que já havia na unidade"):
   ```json
   {
     "canalAtendimento": "WEB",
     "canalSolicitante": "AGC",
     "codificacao": "0190",
     "codigo": "{uc}",
     "anexos": [],
     "camposAdicionais": [
       {"textoManifestacao": "***** Protocolo Informado: {protocolo_anterior} *****"},
       {"textoManifestacao": "{relato da manifestação}"}
     ],
     "distribuidora": "COELBA",
     "documentoSolicitante": "{cnpj}",
     "email": "{email_resposta}",
     "grupoCodificacao": "OROUTROS",
     "manifestacao": "05",
     "numCliente": "{numCliente, id interno != uc}",
     "operacao": "CAD",
     "protocolo": "{protocolo de sessão — obterProtocolo}",
     "telefone": "{telefone}",
     "tipificacao": "1020199",
     "tipoPerfil": 1,
     "usuario": "WSO2_CONEXAO"
   }
   ```
   Resposta (200 OK):
   ```json
   {
     "numeroNota": "300002374944",
     "e_resultado": "X",
     "retorno": {"tipo": "S", "id": "ZATCWS", "numero": "029", "mensagem": "Executado com sucesso."}
   }
   ```
   `numeroNota` é o número da manifestação mostrado na tela de confirmação.

   ⚠️ **Falta descobrir**: `codificacao` ("0190"), `tipificacao` ("1020199")
   e `grupoCodificacao` ("OROUTROS") são os códigos internos que codificam
   Motivo Principal + Detalhes do Motivo — essa captura confirma 1 par
   (Motivo "Outros", Detalhe provavelmente "Compensação descumprimento prazo
   ñ real" — ainda não confirmado qual exatamente). Faltam os códigos dos
   outros 17 Motivos (e seus Detalhes) pra montar a automação completa.
   **Hipótese a testar, SEM precisar abrir mais chamados reais**: os
   endpoints `GET /imoveis/1.1.0/motivos` e `GET /imoveis/1.1.0/detalhe-motivos`
   (catalogados no §5.1, nunca testados) podem devolver essa tabela completa
   de texto↔código de uma vez só — são leituras, não têm o risco de abrir
   reclamação nenhuma. Próximo passo natural antes de tentar montar o POST
   de verdade.
   > `camposAdicionais[0]` só aparece quando "Protocolo anterior" é
   > preenchido no formulário (formato fixo "***** Protocolo Informado: X
   > *****"); sem protocolo anterior, provavelmente só tem o item do relato
   > — não confirmado ainda.

⚪ GET {ML}servicos/fale-conosco/ucs/{uc}/buscar-fale-conosco
⚪ GET {ML}servicos/fale-conosco/ucs/{uc}/verifica-protocolo
⚪ GET {ML}servicos/fale-conosco/consultar-status-nota
```
> A UI de "Reclamações" (módulo `servicos/reclamacoes`) usa os endpoints de acompanhamento acima; reclamações concluídas aparecem no `pedidos-header` como `tipoSolicitacao: "Reclamação Ouvidoria"`.
>
> **Fluxo completo da tela mapeado ao vivo em 13/11** (ver protocolos.py,
> comentário acima de `ABA_FILA_RECLAMACOES`): Protocolo (opcional) ->
> Manifestação (Motivo/Detalhe/Relato/Telefone — Motivo "Faturas" abre dois
> passos extras, Faturas + Solicitar Leitura, com validação de fatura real)
> -> E-mail (campo obrigatório não mapeado antes) -> Dados Adicionais (anexo
> opcional) -> Envio. **O clique de "Continuar" em Dados Adicionais É O
> ENVIO FINAL** — não existe uma tela de revisar-antes-de-enviar depois
> dele. Os 18 Motivos Principais e seus Detalhes do Motivo estão todos
> mapeados em `MOTIVOS_PRINCIPAIS_RECLAMACAO`/`DETALHES_MOTIVO_RECLAMACAO`
> (protocolos.py) — essas listas são estáticas no bundle, não vêm de uma
> chamada de API (confirmado: nenhuma chamada de rede nova acontece ao
> trocar Motivo/Detalhe).

---

## 5. Catálogo completo de endpoints (apêndice)

`{ML}` = `/multilogin/2.0.0/`. Verbos detectados automaticamente do bundle (podem ter imprecisão em casos isolados). `{}` = parâmetro de path (geralmente UC ou documento).

### 5.1 Autenticados — `{ML}servicos/*` e `{ML}agv/*`

**Acompanhamento** ✅/⛔ (ver §4.3)
```
GET  acompanhamento-solicitacoes/{cnpj}/pedidos-header       ✅
GET  acompanhamento-solicitacoes/ucs/{uc}/pedidos-header     ⛔
GET  acompanhamento-solicitacoes/{num}/pedidos-detalhe       ✅
GET  acompanhamento-solicitacoes/ucs/{num}/pedidos-detalhes  ⚪
```

**Multilogin / gestão de acessos (agv)** ⚪
```
GET  agv/{}/representantes                          GET  agv/mlogin-obter-vinculos-concedidos
POST agv/adiciona-vinculo-perfis                    GET  agv/mlogin-obter-vinculos-recebidos
POST agv/adiciona-vinculo-uc                        GET  agv/obter-servicos
GET  agv/cliente/{}/grupo-de-cliente               POST agv/remove-vinculo-perfis
GET  agv/consulta/{}                               POST agv/remove-vinculo-uc
GET  agv/corretor/{}                                GET  agv/representante/{}
GET  agv/corretor/{}/imobiliaria/{}/cliente/{}/empresa/{}   POST agv/representante/atribuir
GET  agv/grupo/corretor/{}                         POST agv/representante/atualiza-representados
GET  agv/grupo/imobiliaria/{}                       GET  agv/representante/representado/{}/empresa/{}
GET  agv/imobiliaria/{}/cliente/{}/empresa/{}      POST agv/usuarios/busca-tipo-cliente
GET  agv/imobiliaria/{}/empresa/{}/corretores      POST agv/imobiliaria/vincular/uc
GET  agv/imobiliaria/{}/representanteCliente        POST agv/imobiliaria/vincular/{}/corretor
GET  agv/imobiliaria/{}/uc/{}/empresa/{}           POST agv/imobiliaria/desvincular/uc
                                                    POST agv/imobiliaria/desvincular/{}/corretor
```

**Conta / cadastro / perfil** ✅/⚪
```
GET  servicos/minha-conta                                   ✅
GET  servicos/minha-conta/minha-conta-legado               ⚪
GET  servicos/imoveis/ucs/{uc}                             ⚪
GET  servicos/atualizacao-cadastral/{}/dados-cadastrais    ⚪
GET  servicos/atualizacao-cadastral/consultar-cliente      ⚪
GET  servicos/atualizacaocadastral/{}/dados-cadastrais     ⚪
PUT  servicos/atualizacaocadastral/atualizar-pf            ⚪
POST servicos/atualizacaocadastral/cadastro-pf             ⚪
POST servicos/log-atividade/registra-log-atividade         ⚪
```

**Faturas / pagamentos / débitos** ⚪
```
GET  servicos/faturas/ucs/faturas                   GET  servicos/faturas/{}/dados-pagamento
GET  servicos/faturas/faturas-simplificada          POST servicos/faturas/{}/pdf-email
POST servicos/faturas/pdfs                           PUT servicos/fatura-cumulativa/permitir-pagamento-fatura-cumulativa
GET  servicos/fatura-digital/ucs/fatura-digital     POST servicos/fatura-digital/ucs/{}/fatura-email-cadastra
GET  servicos/fatura-digital/ucs/{}/fatura-digital-valida   POST servicos/fatura-digital/ucs/{}/fatura-email-descadastra
POST servicos/fatura-digital/ucs/{}/fatura-braile-cadastra
POST servicos/boleto-unico/gerar-boleto-unico       POST servicos/boleto-unico/listar-boleto-unico
POST servicos/boleto-unico/buscar-linha-digitavel   POST servicos/boleto-unico/imprimir-boleto-unico
POST servicos/boleto-unico/listar-boletos-vinculados POST servicos/boletounico/gerar-pdf-boleto-unico
POST servicos/boletounico/gerar-boleto-unico
POST servicos/pagamentos/v1/negocie                 POST servicos/pixne/gerarPix  /  pixse/gerarPix
GET  servicos/debito-automatico/bancos              GET  servicos/debito-automatico/bancos/{}/agencias
GET  servicos/debito-automatico/conta-cadastrada-debito
POST servicos/debito-automatico/ucs/{}/debito-automatico
POST servicos/debito-automatico/ucs/{}/cancela-debito-automatico
POST servicos/icms/solicitar-pagamento-credito-icms
```

**Consumo / entenda sua conta** ⚪
```
GET servicos/historicos/ucs/{}/consumos            GET servicos/entendasuaconta/ucs/{}/entenda-conta
GET servicos/historicos/ucs/{}/consumos/grupo-a    GET servicos/entendasuaconta/ucs/{}/entenda-conta-qualidade
```

**Atendimento / solicitações de serviço** ⚪ (exceto falta-energia ⛔)
```
GET  servicos/fale-conosco/ucs/{}/buscar-fale-conosco   GET servicos/fale-conosco/ucs/{}/verifica-protocolo
GET  servicos/fale-conosco/consultar-status-nota        GET servicos/fale-conosco/verifica-protocolo
GET  servicos/falta-energia/ucs/{}/falta-energia              ⛔
GET  servicos/falta-energia/ucs/{}/falta-energia-interrupcoes ⛔
GET  servicos/falta-energia/ucs/{}/falta-energia-ocorrencia   ⛔
POST servicos/falta-energia/ucs/falta-energia                 ⚪(POST registrar)
POST servicos/religacao/ucs/{}/religacao-imediata       GET  servicos/religacao/ucs/{}/valida-religacao
POST servicos/religacao/cadastrarDadosSolicitante       POST servicos/religacao/inserir-registro-atendimento
POST servicos/desligamentodefinitivo/ucs/{}/verifica-solicitacao
POST servicos/desligamentodefinitivo/ucs/verifica-solicitacao
DELETE servicos/desligamento/ucs/desligamento-programado/{}/pes
POST servicos/autoleitura/ucs/{}/autoleitura            GET servicos/autoleitura/ucs/{}/valida-autoleitura
DELETE servicos/autoleitura/ucs/{}/simula-autoleitura
GET  servicos/datacerta/ucs/{}/datacerta                PUT  servicos/datacerta/ucs/{}/datacerta
GET  servicos/datacerta/ucs/{}/datacerta-valida         POST servicos/datacerta/ucs/{}/datacerta-descadastra
GET  servicos/tarifasocial/valida  /  POST servicos/tarifasocial/valida
POST servicos/troca-titularidade/troca-titularidade     GET  servicos/troca-titularidade/consultar-cliente
POST servicos/troca-titularidade/executa-troca-titularidade  POST servicos/troca-titularidade/servicos-adicionais
GET  servicos/entrega-fatura/buscar-cep                 POST servicos/entrega-fatura/ucs/{}/entrega-alternativa
GET  servicos/entrega-fatura/endereco-alternativo-valida POST servicos/entrega-fatura/ucs/{}/cancela-entrega-alternativa
POST servicos/entrega-fatura/clientes/cadastrar-endereco
POST servicos/contrato-de-adesao/gerar                  POST servicos/reforma-padrao/reforma-padrao
POST servicos/particulares/enviar-projeto-particular
POST servicos/ressarcimento-danos/verifica-solicitacao (…)
POST servicos/termo-aceite/cadastrarTermoAdesao         POST servicos/termo-aceite/descadastrarTermoAdesao
```

**Ligação nova (34 endpoints)** ⚪ — consultas de endereço/carga/equipamentos + submissão:
```
GET  servicos/ligacao-nova/listar-municipios / listar-bairro / listar-logradouro / listar-uf /
     listar-equipamentos / listar-processos / listar-subtipos-aparelhos / buscar-endereco /
     consulta-data-certa / verifica-data-certa / validacao-inicial / verifica-uc-cliente ...
POST servicos/ligacao-nova/gerar-pedido-ligacao / calculo-demanda / valida-carga /
     criar-ss-informacao / inserir-anexo / integrar-pedido-sgd / listar-setor-industrial ...
PUT  servicos/ligacao-nova/solicita-ligacao-nova / atualizar-cliente
```

**Mensageria / OCR / outros** ⚪
```
GET  servicos/ocr-rec/ocr/receita/cnpj   /  cpfavancado
POST servicos/salesforce-marketing-automation/mensageria/envio-mensagens / optin-altera
GET  servicos/salesforce-marketing-automation/mensageria/optin-consulta
POST servicos/send-email/generic
GET  servicos/perfil ...
```

### 5.2 Não autenticados / outros namespaces ⚪
```
GET  /protocolo/1.1.0/obterProtocolo                        ✅ (ver §4.1)
GET  /imoveis/1.1.0/clientes/{cnpj}/ucs                     ⚪  /  /imoveis/1.1.0/motivos, /detalhe-motivos
PUT  /minha-conta/2.0.0/atualizar-minha-conta / atualizar-senha
GET  /ouvidoria/1.0.0/consultar-ouvidoria
POST /consumidores/1.0.0/termo-adesao-cadastro / -consulta / -descadastro
GET  /datacerta/1.1.1/ucs/{}/datacerta-dias
GET  /demonstrativos-mmgd/1.0.0/{}/consultar-demonstrativos / buscar-opbel
GET  /contrato-de-adesao/1.0.0/recupera-pdf
GET  /cms-faq/1.0.0/lista-conteudo-servico
DELETE /login/2.0.0/usuarios/{}
POST /pixne/1.0.0/secure/rest/api/v1/consultaPix
POST /anexos/
Área NÃO logada (/areaNaoLogada/*): autentica, usuarios (esqueci-senha, valida, codigo-valida),
     faturas-simplificada, buscar-linha-digitavel, boleto-unico, negocie, get-url,
     ocr/receita/cnpj-situacao, lista-conteudo-servico ...  (+ /areaNaoLogadaSE/obterProtocolo)
```

### 5.3 Base de serviços (46 chaves de `environment.endpoints`)
`atualizacaoCadastral, atualizacaoCadastralMultilogin, autoleitura, bandeiraTarifaria, boletoUnico, contaMinima, contratoAdesao, dataCerta, demonstrativoGeracao, desligamentoDefinitivo, faceMatch, faleConosco, faturaDigital, faturaImpressa, icms, ligacaoNova, ligacaoNovaMultilogin, listaCompensacao, marketingAutomation, minhaConta, multilogin(agv), multiloginServicos, orcRec, ouvidoria, pagamento, particulares, pix, historico, protocolo, ressarcimentoDanos, selecaoImovel(imoveis), tarifaSocial, termoDeAdesao, token, trocaTitularidade, validaCPFReceitaFederal(+Multilogin), faq, pesquisaSatisfacao, reformaPadrao, login, cms, demonstrativoMmgd, anexos, perfil, faturaCumulativa`

---

## 6. Estrutura do site (SPA Angular — módulos → páginas)

```
home/           → home, meus-imoveis (lista de UCs do CNPJ)
login/          → pagina-inicial, new-register
multilogin-acesso/  (perfis PJ, compartilhamento de acesso)
minha-conta/
recuperar-senha/
cadastro/
ligacao-nova/   (fluxo próprio extenso)
ressarcimento-danos/
servicos/       → menu de serviços:
    acompanhe-seu-pedido ⭐  reclamacoes ⭐  falta-energia  fale-conosco
    historico-consumo    consultar-debitos      segunda-via-login
    fatura-digital       escolha-tipo-fatura    entrega-da-fatura
    data-certa           debito-automatico      boleto-unico
    tarifa-social        autoleitura            conta-minima
    atualizacao-cadastral entenda-sua-conta     perfil-consumo
    modificacao-contratual troca-de-titularidade religacao
    micro-mini-geracao-distribuida  reforma-de-padrao  projeto-particular
    pagamento-icms-fatura  multilogin-cadastro  multilogin-compartilhar-acesso
    vincular-unidade-consumidora   cadastrar-pessoa-juridica
    criar-relacao-rl-pj  cadastrar-representante-legal
```

### Rotas de 1º nível (hash routing `#/...`)
`selecionar-estado → selecionar-perfil-de-acesso → home/meus-imoveis → servicos/<serviço>`
Outras: `login, recuperar-senha, minha-conta, configuracao-de-notificacoes, debitos, faq/perguntas-frequentes, compartilhar-acesso, dashboard, avisos, protocolo-texto`.

### Fluxo de entrada (necessário p/ a UI; irrelevante se chamar API direto)
`selecionar-estado` (Bahia=COELBA) → `selecionar-perfil-de-acesso` → perfil multilogin PJ → `meus-imoveis` (lista UCs). As telas de serviço só renderizam com uma UC em contexto; por isso, no scraping por API, pular a UI e chamar os endpoints direto é mais robusto.

---

## 7. Resumo: o que temos vs. o que falta

### ✅ Confirmado acessível (token web AGC, multilogin PJ)
- `obterProtocolo` (nível conta)
- `minha-conta` (lista UCs)
- `acompanhamento-solicitacoes/{cnpj}/pedidos-header` (por UC)
- `acompanhamento-solicitacoes/{num}/pedidos-detalhe` (com qualquer UC válida)

### ⛔ Confirmado SEM acesso (mesmo token → 401/escopo diferente)
- Todo `/falta-energia/*` (consulta, interrupções, ocorrência)
- Variante `/acompanhamento-solicitacoes/ucs/{uc}/pedidos-header`

### ⚪ Mapeado, não testado (provável acesso, confirmar sob demanda)
- Todo o restante do catálogo (§5): faturas, histórico-consumo, débitos, fale-conosco/ouvidoria, religação, etc.

### Próximos passos sugeridos
1. **Falta de energia:** investigar o fluxo de login/subscription que emite um token com escopo p/ a API `falta-energia` (produto WSO2 distinto). Sem isso, não há como puxar ocorrências de falta por API.
2. **Otimizar o módulo atual:** guardar `{numSolicitacao → uc}` na planilha e atualizar status via `pedidos-detalhe` direto (sem rerodar `pedidos-header` de todas as UCs). Manter a trava de dedup.
3. **Cachear o `protocolo` por dia**; renovar só o token (~1h).
4. Se precisar de histórico de consumo/faturas no hub, `historicos/ucs/{uc}/consumos` e `faturas/ucs/faturas` são os candidatos (testar com 1 UC primeiro).

---
*Levantado via DevTools/engenharia reversa da própria Agência Virtual na conta do usuário. Verbos auto-detectados podem ter imprecisões pontuais; confirmar com 1 chamada antes de integrar cada endpoint novo.*
