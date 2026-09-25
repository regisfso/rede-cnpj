# Sobre este fork

Este repositório é um fork de [rictom/rede-cnpj](https://github.com/rictom/rede-cnpj),
mantido sincronizado com o projeto original (`git merge upstream/master`).

A filosofia deste fork é **intervir o mínimo possível nos arquivos originais**:
as customizações ficam em arquivos/pastas novos, para facilitar merges futuros
do upstream. Este documento reúne o que existe aqui e não existe no projeto
original.

## Scripts de carga (`rede_cria_tabelas/`)

- **`dados_cnpj_para_sqlite.py`**: uso de memória RAM reduzido — os CSVs
  grandes são lidos em blocos com `dask` (`blocksize=128MB`) e processados
  partição a partição, com `gc.collect()` liberando a memória a cada etapa,
  em vez de carregar o arquivo inteiro de uma vez; a ordem de carga das
  tabelas foi ajustada (estabelecimento primeiro) para reduzir o pico de uso
  de RAM.
- **`dados_cnpj_baixa_resiliente.py`** *(novo)*: alternativa ao
  `dados_cnpj_baixa.py` com download concorrente de vários arquivos ao mesmo
  tempo, retomada automática de downloads incompletos, validação de
  integridade dos zips baixados e contador de quantos arquivos ainda faltam.
- **`dados_cnpj_para_sqlite_progresso.py`** *(novo)*: alternativa ao
  `dados_cnpj_para_sqlite.py` que grava o progresso em uma tabela de controle
  dentro do próprio banco sqlite, permitindo retomar a geração da base do
  ponto onde parou (sem refazer o que já foi concluído) caso o processo seja
  interrompido.
- `requirements.txt` (em `rede_cria_tabelas`): adicionadas as dependências
  `pyarrow`, `psutil` e `tqdm` usadas pelos scripts acima.

## Deploy em produção (`deploy/`)

Dockerfile, docker-compose e configuração de nginx para rodar esta instalação
em produção com Docker. Veja [`deploy/README.md`](deploy/README.md).

O ponto de entrada de produção é `deploy/wsgi.py`, que carrega `rede/rede.py`
dinamicamente (via `importlib`) e é também onde a API descrita abaixo é
registrada — sem precisar editar `rede/rede.py`.

## API de consulta (`rede/modulos/api_ext/`)

Além das rotas `/api/<tipo>/<cnpj>` e `/api/caminhos` que já existem no
projeto original (gated por `api_cnpj`/`api_caminhos` em `rede.ini`), este
fork adiciona um blueprint com rotas `/api/ext/...` para busca de dados de
empresas, pensadas para consumo programático (scripts, integrações).

**Importante:** essas rotas só ficam disponíveis quando a aplicação roda via
`deploy/wsgi.py` (produção/Docker, com `gunicorn wsgi:application`). Rodando
localmente com `python rede.py`, apenas as rotas originais estão disponíveis.

### Endpoints

Todos aceitam apenas `GET` e retornam JSON (`Content-Type: application/json`).
O prefixo de rota segue o `subdomain` configurado em `rede.ini`
(`[LOGIN] subdomain`, padrão `/rede/`).

| Rota | Descrição | Parâmetros |
|---|---|---|
| `GET /rede/api/ext/busca/nome` | Busca ids por nome/razão social (full text) | `q` (obrigatório), `limite` (padrão 10, máx. 100) |
| `GET /rede/api/ext/busca/cnpj_raiz/<cnpj_basico>` | Busca ids de filiais a partir da raiz do CNPJ (8 dígitos) | `limite` (padrão 10, máx. 200) |
| `GET /rede/api/ext/busca/cnae/<codigo>` | Busca ids de empresas pelo CNAE fiscal principal (7 dígitos) | `limite` (padrão 10, máx. 200) |
| `GET /rede/api/ext/busca/cpf/<cpf_parcial>` | Busca ids de sócios PF pelo miolo do CPF (mín. 9 dígitos) | `limite` (padrão 10, máx. 100) |
| `GET /rede/api/ext/dados` | Retorna dados completos de uma lista de ids (CNPJ/CPF) | `ids` (obrigatório, separados por vírgula, ex. `PJ_12345678000199,PJ_...`), `socios` (`1` para incluir sócios) |

As rotas de busca (`/busca/...`) retornam apenas os ids encontrados
(`{"ids": [...]}`) — use `/dados?ids=...` em seguida para obter os dados
completos desses ids, já que essa separação é a mesma usada internamente
pelas telas da própria RedeCNPJ.

### Limite de resultados (parâmetro `limite`)

Nas rotas de busca, `limite` é enviado como parâmetro de query string na
própria URL do GET (não existe em `/dados`, que não tem esse parâmetro):

```bash
curl "http://localhost/rede/api/ext/busca/nome?q=BANCO%20DO%20BRASIL&limite=50"

curl "http://localhost/rede/api/ext/busca/cnpj_raiz/00000000?limite=150"

curl "http://localhost/rede/api/ext/busca/cnae/6810202?limite=50"

curl "http://localhost/rede/api/ext/busca/cpf/123456789?limite=30"
```

O `limite` enviado pelo cliente é só um teto sugerido: o valor efetivamente
usado na consulta é sempre `min(limite_pedido, teto_do_servidor)`, e o teto é
fixo no código (não é configurável via `rede.ini`). Se `limite` vier omitido
ou como `0`, cai no padrão de 10.

| Rota | Teto do servidor | Onde está no código |
|---|---|---|
| `/busca/nome` | 100 | `rede_sqlite_cnpj.buscaPorNome` |
| `/busca/cnpj_raiz/<cnpj_basico>` | 200 | `rede_sqlite_cnpj.busca_cnpj` |
| `/busca/cnae/<codigo>` | 200 | `rede_sqlite_cnpj.busca_cnae` |
| `/busca/cpf/<cpf_parcial>` | 100 | `rede_sqlite_cnpj.busca_cpf` |
| `/dados` | **sem limite** — processa todos os `ids` enviados | `rede_sqlite_cnpj.jsonDados` |

Como `/dados` não limita a quantidade de `ids` por requisição, o único freio
para um payload muito grande nessa rota hoje é o rate limit de requisições
(`limiter_dados`, abaixo) — não há cap de tamanho de lista.

### Exemplos

```bash
curl "http://localhost/rede/api/ext/busca/nome?q=BANCO%20DO%20BRASIL&limite=5"

curl "http://localhost/rede/api/ext/busca/cnpj_raiz/00000000"

curl "http://localhost/rede/api/ext/dados?ids=PJ_00000000000000&socios=1"
```

### Habilitando/desabilitando (`rede.ini.local`)

Em produção, `rede/rede.ini` é o exemplo rastreado pelo git e não deve ser
editado no servidor. As chaves `api_ext_*` e `api_keys` vão em
`rede/rede.ini.local` — arquivo **não versionado** (está no `.gitignore`) que
`rede_config.py` lê por último, sobrepondo o `rede.ini` padrão. Veja
[`deploy/README.md`](deploy/README.md#configuração-específica-de-ambiente-redeinilocal)
para como criá-lo a partir de `rede/rede.ini.local.example`.

```ini
[API]
api_ext_busca=1          # habilita /busca/nome, /busca/cnpj_raiz, /busca/cpf
api_ext_dados=1           # habilita /dados
api_ext_requer_chave=0    # 1 = exige api_key válida (ver abaixo) em todas as rotas /api/ext
```

Se `rede.ini.local` não tiver essas chaves, os endpoints `/api/ext/...` ficam
desativados (`getboolean` cai no `False` padrão) em vez de dar erro.

Como `rede/bases`, `rede/rede.ini` e `rede/rede.ini.local` são montados como
volume no docker-compose (não fazem parte da imagem), basta editar
`rede.ini.local` e rodar `docker-compose restart app` — não precisa rebuild.
(Em ambiente local sem Docker, sem a variável `CONFIG_PATH_LOCAL` que o
`deploy/wsgi.py` define, `rede_config.py` procura `rede.ini.local` na pasta
de trabalho atual.)

### Autenticação por chave (opcional)

Com `api_ext_requer_chave=1`, toda requisição às rotas `/api/ext/...` precisa
enviar uma chave válida, via query string ou header:

```bash
curl "http://localhost/rede/api/ext/dados?ids=PJ_...&api_key=SUACHAVE"
# ou
curl -H "X-API-Key: SUACHAVE" "http://localhost/rede/api/ext/dados?ids=PJ_..."
```

As chaves válidas são as mesmas configuradas em `[API] api_keys` em
`rede.ini.local` (usadas também pela rota original `/api/caminhos`),
separadas por vírgula:

```ini
[API]
api_keys=chave-do-time-a,chave-do-time-b
```

Para gerar uma chave aleatória, use o módulo `secrets` do Python (já é uma
dependência do projeto):

```bash
python3 -c "import secrets; print(secrets.token_hex(24))"
```

### Limites de requisição (rate limit)

As rotas `/api/ext/...` usam o mesmo `flask_limiter` e o mesmo parâmetro
`limiter_dados` (`[ETC] limiter_dados` em `rede.ini`) das rotas
`/api/<tipo>/<cnpj>` originais — o limite é compartilhado entre elas.

### Detalhes de implementação

O blueprint fica em `rede/modulos/api_ext/rede_api_ext.py` e reaproveita as
funções de acesso a dados já existentes em `rede/rede_sqlite_cnpj.py`
(`buscaPorNome`, `busca_cnpj`, `busca_cpf`, `jsonDados`) — nenhuma lógica de
consulta ao banco foi duplicada. O registro do blueprint acontece só em
`deploy/wsgi.py`; `rede/rede.py` permanece inalterado.

`busca_cnae` (usada por `/busca/cnae/<codigo>`) é a exceção: diferente das
demais buscas, que usam a tabela virtual FTS5 `id_search` (pensada para busca
textual livre por nome/descrição), CNAE fiscal é um código de igualdade
exata, então a consulta é direta em `estabelecimento.cnae_fiscal`, usando o
índice `idx_estabelecimento_cnae_fiscal` (criado junto com os demais índices
de `estabelecimento` em `rede_cria_tabelas/dados_cnpj_para_sqlite.py`).
Bases já geradas antes dessa mudança precisam rodar
`CREATE INDEX idx_estabelecimento_cnae_fiscal ON estabelecimento (cnae_fiscal);`
manualmente para não cair em table scan.

**Zero à esquerda:** parte dos registros de `estabelecimento.cnae_fiscal` tem
o zero à esquerda do código oficial suprimido (ex.: `151201` em vez de
`0151201`) — afeta só as seções A e B da CNAE (agropecuária, pesca e
indústrias extrativas, os únicos códigos que começam com `0`); a tabela
`cnae` (referência oficial) sempre tem os 7 dígitos corretos. `busca_cnae`
já busca as duas variantes (`cnae_fiscal in (:codigo, :codigo_sem_zero)`)
para não perder esses registros — confirmado batendo o código de 7 dígitos
contra a tabela `cnae` para todos os 122 códigos de 6 dígitos existentes na
base local de teste.
