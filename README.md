# Ranking de Fundos

Site estático que compara fundos de ações brasileiros por CAGR, risco, consistência e alpha. Dados buscados diariamente da CVM e publicados automaticamente via GitHub Pages.

## O que o site faz

**Aba Ranking**
- Tabela ordenável por CAGR 12M / 36M / 60M / Início
- Coluna α: alpha do fundo contra o benchmark relevante (IBOV para equity, CDI para multimercados) na janela de tempo selecionada
- Filtros por tipo (Long Only / Long Biased / Multimercado), exposição (Brasil / Internacional) e tributação (RV / TR)
- Período customizado: selecione qualquer janela dentro dos 3 anos do histórico e o ranking recalcula
- Detalhe expandido por fundo: CAGR por janela, drawdown atual, tipo, tributação, banco distribuidor

**Aba Portfólio**
- Monte carteiras com pesos ajustáveis, calcule CAGR real, volatilidade, drawdown, Sharpe e retorno alvo
- Otimizador: máximo Sharpe, mínima volatilidade ou máximo retorno, para 3, 5 ou 10 fundos
- Simulador de IR: tributação correta por fundo (RV vs. tabela regressiva com come-cotas)

**Aba Gráficos**
- Cotas normalizadas (base 100) com seletor de janela e toggles por fundo
- Modo underwater: drawdown contínuo em relação ao pico

**Aba Análise**
- Radar risco × retorno: volatilidade anualizada vs. retorno alvo com Sharpe no hover
- Heatmap de retornos mensais: cores por magnitude, 3 anos de histórico
- Score de consistência: % de meses positivos, acima do CDI, acima do IBOV, pior mês
- Drawdown histórico (underwater) por fundo com máximo e tempo médio de recuperação
- Stress test automático: identifica os 3 piores períodos de 20 pregões e mostra impacto por fundo
- Simulador de aportes periódicos com IR: aporte inicial + recorrente mensal/anual, resultado em XIRR real

---

## Estrutura do projeto

```
.
├── .github/
│   └── workflows/
│       └── update.yml        # Roda seg–sex às 21h UTC (18h BRT)
├── docs/
│   ├── index.html            # Site completo (HTML + CSS + JS em um arquivo)
│   ├── data.json             # Gerado automaticamente — CAGRs, benchmarks
│   └── history.json          # Gerado automaticamente — série histórica 3 anos
└── scripts/
    └── fetch_data.py         # Coleta CVM, Yahoo Finance, BCB
```

---

## Como funciona por baixo

```
scripts/fetch_data.py   ← GitHub Actions roda todo dia útil às 18h BRT
        │
        ├─ Baixa cotas diárias da CVM (arquivos ZIP mensais/anuais)
        ├─ Calcula CAGR 12M / 36M / 60M / desde início para cada fundo
        ├─ Âncora por mediana (robusto a gaps em fundos individuais)
        ├─ Benchmarks: IBOV via Yahoo Finance, CDI via Banco Central (série 12)
        ├─ Escreve docs/data.json
        └─ Escreve docs/history.json (série de cotas, 80% de presença mínima,
                                       interpolação geométrica para gaps)
                │
        docs/index.html lê os dois JSONs e renderiza tudo no browser
        (sem backend, sem servidor, sem banco de dados)
```

---

## Setup completo (primeira vez)

### Pré-requisitos

- Conta no [GitHub](https://github.com) (gratuita)
- Git instalado no computador ([download](https://git-scm.com/downloads))

### Passo 1 — Criar o repositório no GitHub

1. Acesse [github.com/new](https://github.com/new)
2. Dê um nome ao repositório (ex: `fundos`)
3. Deixe em **Public** (necessário para GitHub Pages gratuito)
4. **Não** marque nenhuma opção de inicialização (README, .gitignore etc.)
5. Clique em **Create repository**

### Passo 2 — Enviar os arquivos para o GitHub

Abra o terminal (no Mac: Terminal; no Windows: Git Bash ou PowerShell) na pasta onde você descompactou o zip e execute:

```bash
git init
git add .
git commit -m "primeiro commit"
git branch -M main
git remote add origin https://github.com/SEU_USUARIO/SEU_REPO.git
git push -u origin main
```

> Substitua `SEU_USUARIO` pelo seu nome de usuário do GitHub e `SEU_REPO` pelo nome que você deu ao repositório.

### Passo 3 — Ativar GitHub Pages

1. No repositório, clique em **Settings** (engrenagem no menu superior)
2. No menu lateral esquerdo, clique em **Pages**
3. Em **Source**, selecione **Deploy from a branch**
4. Em **Branch**, selecione `main` e a pasta `/docs`
5. Clique em **Save**

Após salvar, o GitHub exibirá o endereço do site (ex: `https://seu_usuario.github.io/fundos/`). Pode levar 1–2 minutos para o site aparecer.

### Passo 4 — Dar permissão de escrita ao workflow

1. No repositório, clique em **Settings**
2. No menu lateral, clique em **Actions → General**
3. Role até **Workflow permissions**
4. Selecione **Read and write permissions**
5. Clique em **Save**

### Passo 5 — Rodar o script pela primeira vez

O script roda automaticamente todo dia útil às 18h BRT. Para rodar agora:

1. Clique em **Actions** no menu superior do repositório
2. No painel esquerdo, clique em **Update fund data & deploy**
3. Clique no botão **Run workflow** → **Run workflow**
4. Aguarde 3–8 minutos (a CVM serve arquivos grandes)

Quando o workflow ficar verde (✓), acesse o endereço do seu site.

### Passo 6 — Verificar se funcionou

Acesse `https://seu_usuario.github.io/fundos/`. Você deve ver o ranking com os fundos e os CAGRs. O rodapé mostra a data de referência e o horário de geração.

---

## Atualização automática

O workflow `.github/workflows/update.yml` roda automaticamente segunda a sexta às 21h UTC (18h BRT). A cada execução ele:

1. Baixa as cotas mais recentes da CVM
2. Recalcula todos os CAGRs e métricas
3. Faz commit do `data.json` e `history.json` atualizados
4. Faz o deploy automático no GitHub Pages

Você não precisa fazer nada — o site se atualiza sozinho.

---

## Adicionar ou remover fundos

### No `scripts/fetch_data.py`

Edite a lista `FUNDS` perto do topo do arquivo:

```python
FUNDS = [
    {"name": "Nome do Fundo",  "cnpj": "00000000000000", "cnpjFmt": "00.000.000/0001-00"},
    # ... outros fundos
]
```

O CNPJ do fundo está disponível no site da CVM ou no extrato do fundo.

### No `docs/index.html`

Edite o objeto `FUND_META` no bloco `<script>`:

```javascript
const FUND_META = {
  "00.000.000/0001-00": {
    nome: "Nome Curto",
    inception: "AAAA-MM-DD",    // data de início do fundo
    initialQuota: 1.0,           // cota no dia de início (geralmente 1.0)
    maxQuota: 1.0,               // deixe 1.0 — será atualizado automaticamente
    tipo: "Long Only",           // Long Only | Long Biased | Multimercado
    trib: "RV",                  // RV (Renda Variável) | TR (Tabela Regressiva)
    expo: "Brasil",              // Brasil | Internacional | Majoritariamente Brasil
    banco: "BTG",                // banco distribuidor principal
    obs: ""                      // observação opcional
  },
};
```

Após editar, faça commit e push. O próximo agendamento usará os dados novos.

---

## Fontes de dados

| Dado | Fonte | Frequência |
|---|---|---|
| Cotas diárias dos fundos | [CVM — inf_diario_fi](https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/) | Diária |
| IBOV | Yahoo Finance (`^BVSP`) | Diária |
| CDI | Banco Central (série 12) | Diária |

---

## Metodologia resumida

**CAGR:** `(cota_fim / cota_inicio) ^ (1 / anos) − 1`, com `anos = dias / 365.25`.

**Âncora:** mediana das últimas datas de cota disponíveis entre todos os fundos (quorum de 50%). Garante que todos os fundos, CDI e IBOV usam exatamente a mesma janela de tempo.

**Alpha:** `CAGR_fundo − CAGR_benchmark` no mesmo período. Benchmark: IBOV para fundos de equity, CDI para multimercados.

**Retorno alvo:** média ponderada dos CAGRs com pesos `√T`, com penalização por dispersão entre períodos e pull de 25% em direção ao CAGR 60M.

**Volatilidade:** desvio padrão dos retornos diários × `√252`, anualizado.

**Sharpe:** `(retorno_alvo − CDI) / volatilidade`.

**Correlação:** Pearson sobre retornos diários, janela deslizante de 3 anos.

**XIRR (aportes periódicos):** Newton-Raphson sobre os fluxos de caixa reais.

**Drawdown:** `(cota_atual − cota_pico) / cota_pico`.

**Stress test:** 3 piores janelas de 20 pregões do portfólio igual-ponderado, impacto por fundo e tempo de recuperação.

---

## Problemas comuns

**O site abre mas mostra "Erro ao carregar data.json"**
O workflow ainda não rodou. Vá em Actions e execute manualmente (Passo 5).

**O workflow falha com erro de permissão**
Vá em Settings → Actions → General → Workflow permissions → selecione "Read and write permissions" → Save.

**Os dados não atualizam depois de dias úteis**
GitHub desativa Actions em repositórios sem atividade por 60 dias. Faça qualquer commit para reativar.

**Aba Análise / Gráficos carrega vazia**
O `history.json` é gerado no primeiro run do workflow. Execute manualmente se ainda não rodou.
