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
| `GET /rede/api/ext/busca/cpf/<cpf_parcial>` | Busca ids de sócios PF pelo miolo do CPF (mín. 9 dígitos) | `limite` (padrão 10, máx. 100) |
| `GET /rede/api/ext/dados` | Retorna dados completos de uma lista de ids (CNPJ/CPF) | `ids` (obrigatório, separados por vírgula, ex. `PJ_12345678000199,PJ_...`), `socios` (`1` para incluir sócios) |

As rotas de busca (`/busca/...`) retornam apenas os ids encontrados
(`{"ids": [...]}`) — use `/dados?ids=...` em seguida para obter os dados
completos desses ids, já que essa separação é a mesma usada internamente
pelas telas da própria RedeCNPJ.

### Exemplos

```bash
curl "http://localhost/rede/api/ext/busca/nome?q=BANCO%20DO%20BRASIL&limite=5"

curl "http://localhost/rede/api/ext/busca/cnpj_raiz/00000000"

curl "http://localhost/rede/api/ext/dados?ids=PJ_00000000000000&socios=1"
```

### Habilitando/desabilitando (`rede.ini`)

```ini
[API]
api_ext_busca=1          # habilita /busca/nome, /busca/cnpj_raiz, /busca/cpf
api_ext_dados=1           # habilita /dados
api_ext_requer_chave=0    # 1 = exige api_key válida (ver abaixo) em todas as rotas /api/ext
```

Como `rede/bases` e `rede/rede.ini` são montados como volume no
docker-compose (não fazem parte da imagem), basta editar `rede.ini` e rodar
`docker-compose restart app` — não precisa rebuild.

### Autenticação por chave (opcional)

Com `api_ext_requer_chave=1`, toda requisição às rotas `/api/ext/...` precisa
enviar uma chave válida, via query string ou header:

```bash
curl "http://localhost/rede/api/ext/dados?ids=PJ_...&api_key=SUACHAVE"
# ou
curl -H "X-API-Key: SUACHAVE" "http://localhost/rede/api/ext/dados?ids=PJ_..."
```

As chaves válidas são as mesmas configuradas em `[API] api_keys` no
`rede.ini` (usadas também pela rota original `/api/caminhos`), separadas por
vírgula:

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
`deploy/wsgi.py`; `rede/rede.py` e `rede/rede_sqlite_cnpj.py` permanecem
inalterados.
