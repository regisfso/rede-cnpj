# Deploy com Docker

Conteineriza com Docker a instalação de produção deste fork do rede-cnpj (rictom/rede-cnpj).

O código da aplicação (incluindo o `wsgi.py`) é incorporado à imagem no build. Só a pasta `rede/bases` (bases sqlite), o `rede/rede.ini` e o override de ambiente `rede/rede.ini.local` (ver seção "Configuração específica de ambiente" abaixo) são montados como volume, para poderem ser atualizados sem reconstruir a imagem.

Ou seja: depois de um `git pull` com mudança de código, rode `docker-compose up --build`; depois de gerar uma base nova (veja abaixo), basta `docker-compose restart app`.

# Requisitos Mínimos

2 vCPU

16 GB de RAM (no momento da criação do banco o script consome por volta de 14 GB RAM)

150 GB de espaço em disco

# Estrutura do Projeto

```
rede-cnpj/                     # este repositório
├── deploy/
│   ├── Dockerfile
│   ├── wsgi.py
│   ├── docker-compose.yaml
│   ├── atualiza_producao.sh   # atualização de código em produção (git pull + rebuild, com health gate/rollback)
│   ├── nginx/
│   │   ├── nginx.conf
│   │   ├── gerar_certificado_autoassinado.sh
│   │   └── certs/             # fullchain.pem e privkey.pem (não versionados, veja HTTPS abaixo)
│   └── README.md              # este arquivo
├── rede/                      # aplicação (código original + pasta bases com os .db)
├── rede_cria_tabelas/         # scripts para baixar e gerar os bancos sqlite
└── ...
```

---

# Instalação do projeto em produção

### Instalação de dependências no servidor

Atualiza o sistema e instala o git:

```bash
sudo apt update &&
sudo apt upgrade &&
sudo apt install git
```

> Se o repositório for privado, configure antes uma chave SSH ou `gh auth login` para conseguir cloná-lo.

##### Instalação do docker e docker-compose

```bash
# dependencias
sudo apt install -y curl gnupg ca-certificates software-properties-common apt-transport-https

# instalar docker
https://github.com/inova-dtip-pcrs/instalador-docker

#habilitar inicialização automática
sudo systemctl enable docker.service
sudo systemctl enable containerd.service
```

---

### Clone do projeto

Na pasta raiz do usuário do servidor Linux:

```bash
cd && git clone https://github.com/regisfso/rede-cnpj
# Ajusta permissões de arquivos da pasta static do flask
chmod -R 755 ~/rede-cnpj/rede/static/
```

---

### HTTPS

O nginx expõe as portas 80 e 443, mas o bloco HTTPS (443) exige um certificado em `deploy/nginx/certs/fullchain.pem` e `privkey.pem` — **sem isso o container do nginx não sobe**. Antes do primeiro `docker-compose up`, escolha uma das opções abaixo:

**Opção 1 — certificado autoassinado** (uso interno, teste, sem domínio público). Rode o script, informando o domínio ou IP do servidor (o padrão é `localhost`):

```bash
cd ~/rede-cnpj/deploy/nginx
./gerar_certificado_autoassinado.sh meuservidor.exemplo.com
```

O navegador vai exibir um aviso de segurança ao acessar por `https://` — normal para um certificado autoassinado.

**Opção 2 — certificado real** (produção com domínio público, ex.: [Let's Encrypt](https://letsencrypt.org/)/certbot). Copie os arquivos do certificado para `deploy/nginx/certs/`, com estes nomes exatos:

```bash
cp /caminho/do/seu/fullchain.pem ~/rede-cnpj/deploy/nginx/certs/
cp /caminho/do/seu/privkey.pem   ~/rede-cnpj/deploy/nginx/certs/
```

Escolhida uma das duas opções, suba os containers (e não esqueça de liberar a porta 443 no firewall do servidor, se houver um):

```bash
cd ~/rede-cnpj/deploy && docker-compose up --build
# Acessível em http://ip/rede e https://ip/rede
```

Se quiser trocar o certificado depois (autoassinado por um real, ou gerar outro autoassinado), apague os dois arquivos em `deploy/nginx/certs/` e repita uma das opções acima, depois reinicie o container: `docker-compose restart nginx`.

---

### Configuração específica de ambiente (rede.ini.local)

`rede/rede.ini` é o exemplo rastreado pelo git e nunca deve ser editado em
produção -- qualquer alteração local nele volta a causar o mesmo conflito de
`git pull` que já aconteceu antes (um commit que toque em `rede.ini` sempre vai
colidir com uma cópia que diverge do git, `skip-worktree` ou não). Em vez
disso, valores específicos deste ambiente (e-mail de contato, segredos como
`api_keys`, chaves que `atualiza_base.py` mantém sozinho como `referencia_bd`
e `[RFB]`, e as chaves `api_ext_*` deste fork) vão em `rede/rede.ini.local`,
um arquivo **não versionado** (está no `.gitignore`) que `rede_config.py` lê
por último, sobrepondo o `rede.ini` padrão.

Crie-o a partir do exemplo rastreado, **antes do primeiro `docker-compose
up`** (um bind mount de arquivo que não existe no host faz o Docker criar um
diretório vazio nesse caminho dentro do container):

```bash
cp ~/rede-cnpj/rede/rede.ini.local.example ~/rede-cnpj/rede/rede.ini.local
nano ~/rede-cnpj/rede/rede.ini.local   # ajuste email e mensagem_advertencia
```

`rede/rede.ini.local.example` documenta cada chave (por que existe, quem a
mantém, o que quebra se faltar) -- inclusive a pegadinha de
`exibe_mensagem_advertencia` (lida com `configparser.getboolean()`, dá erro em
string vazia) e o fato de que é `mensagem_advertencia` vazio, não
`exibe_mensagem_advertencia`, quem realmente desliga o aviso de "base de
teste" (tanto no alerta de abertura da página quanto no tooltip do botão
"RedeCNPJ") -- `rede.py` lê esse texto direto, sem checar
`exibe_mensagem_advertencia` em lugar nenhum do fluxo web.

As chaves `api_ext_*` (endpoints `/api/ext/...` deste fork) e `api_keys`
(segredo, chaves válidas separadas por vírgula) não existem mais em
`rede.ini` -- se `rede.ini.local` não tiver essas chaves, os endpoints
`/api/ext/...` ficam desativados (`getboolean` cai no `False` padrão) em vez
de dar erro.

---

### Criar os bancos de dados de produção

```bash
# instalar python venv
sudo apt install python3.10-venv
# entrar na pasta
cd ~/rede-cnpj/rede_cria_tabelas
# criar virtual env
python3 -m venv .venv

# Cria sessão screen (recomendado para conexões instáveis, mantem uma sessão independente da conexão SSH)
screen -S minha_sessao
# Ativar o ambiente virtual do python
source .venv/bin/activate
# instalar requirements
pip install -r requirements.txt
#conecta na sessão screen caso a conexão caia
screen -x minha_sessao

# para baixar os arquivos zip do site de Dados Abertos, rode o comando, caso haja falha rode o comando novamente até que todos os arquivos sejam baixados:
# o script original é python dados_cnpj_baixa.py, pode ser usado em conexões estáveis e rápidas
python dados_cnpj_baixa_resiliente.py

# cria a base de empresas cnpj.db: (1:30 h)
python dados_cnpj_para_sqlite.py

# cria a tabela de vínculos rede.db utilizada na redeCNPJ: (25 min)
python rede_cria_tabela_rede.db.py

# cria a tabela de vínculos cnpj_links_ete.db de endereços, de emails e de telefones utilizada na redeCNPJ: (1:45 h)
python rede_cria_tabela_cnpj_links_ete.py

# cnpj.db, rede.db, rede_search.db e cnpj_links_ete.db em rede/bases são exemplos
# rastreados pelo git. Rode isto uma única vez, antes de sobrescrevê-los pela
# primeira vez, para o git parar de rastrear o conteúdo (senão toda base de produção
# nova aparecerá como "modified" no git status, arriscando ser commitada por engano):
cd ~/rede-cnpj && git update-index --skip-worktree rede/bases/cnpj.db rede/bases/rede.db rede/bases/rede_search.db rede/bases/cnpj_links_ete.db

# Ao final, mova os arquivos de rede_cria_tabelas/dados-publicos para a rede/bases
cd ~/rede-cnpj/rede/bases && rm cnpj.db rede.db rede_search.db cnpj_links_ete.db

mv $HOME/rede-cnpj/rede_cria_tabelas/dados-publicos/cnpj.db $HOME/rede-cnpj/rede/bases/

mv $HOME/rede-cnpj/rede_cria_tabelas/dados-publicos/rede.db $HOME/rede-cnpj/rede/bases/

mv $HOME/rede-cnpj/rede_cria_tabelas/dados-publicos/rede_search.db $HOME/rede-cnpj/rede/bases/

mv $HOME/rede-cnpj/rede_cria_tabelas/dados-publicos/cnpj_links_ete.db $HOME/rede-cnpj/rede/bases/

# Ajustar o rede.ini.local (arquivo NÃO versionado -- ver seção "Configuração
# específica de ambiente (rede.ini.local)" acima; só é necessário rodando estes
# passos manualmente. rede_cria_tabelas/atualiza_base.py, descrito na próxima
# seção, mantém referencia_bd/exibe_mensagem_advertencia/[RFB] sozinho depois disso)
nano $HOME/rede-cnpj/rede/rede.ini.local

referencia_bd = Abril/2025
exibe_mensagem_advertencia = 0
```

Depois de gerar uma base nova, basta reiniciar o container `app` (`cd ~/rede-cnpj/deploy && docker-compose restart app`) — como `rede/bases` é montada como volume (veja `docker-compose.yaml`), não é necessário reconstruir a imagem. Mudança de **código** (`git pull` com commits na pasta `rede/`) exige `docker-compose up --build`.

### Atualização de código em produção (`atualiza_producao.sh`)

Em vez de rodar manualmente `git pull` + `docker-compose up --build`, use `deploy/atualiza_producao.sh`:

```bash
cd ~/rede-cnpj/deploy
./atualiza_producao.sh              # interativo, pergunta em cada etapa
./atualiza_producao.sh --rapido     # sem prompts, para automação (ex. chamado por outro script)
```

Ele confere conectividade e autenticação com o GitHub, compara o commit local com `origin/master` (encerra sem fazer nada se já estiver atualizado), avisa se houver alterações locais não commitadas antes do `git pull`, e então reconstrói e troca o container `app`. A troca tem **health gate com rollback automático**: se o container novo não responder dentro de 60s, o script volta sozinho para a imagem anterior (e reverte o workspace git para o commit correspondente). Se `rede-cnpj.service` (ver seção de systemd abaixo) estiver ativo, a troca usa `sudo systemctl restart rede-cnpj.service` em vez de mexer no container diretamente — exige uma regra sudoers NOPASSWD para esse comando (senão o script trava pedindo senha no modo `--rapido`). Só atualiza código — a atualização das bases de CNPJ continua por conta do `atualiza_base.py` (próxima seção).

### Atualização automática diária (cron)

Em vez de repetir os passos manuais acima a cada mês, `rede_cria_tabelas/atualiza_base.py` automatiza todo o processo — pensado para rodar via cron **todo dia**, não só uma vez por mês: a cada execução, primeiro consulta (PROPFIND, bem barato) qual é a referência (`anoMes`) mais recente disponível na Receita e compara com a que já está em produção (`rede/rede.ini.local`). Se for a mesma, o script só loga isso e termina sem fazer nada. Se for uma referência nova, baixa os zips, gera as 4 bases numa pasta de staging, valida cada uma (tamanho mínimo e contagem de linhas numa tabela-chave) e só então troca os arquivos em `rede/bases` de forma atômica, reiniciando o container.

A Receita normalmente publica a base do mês perto do **2º domingo**, mas às vezes atrasa (já aconteceu de atrasar 1-3 semanas) — por isso um cron diário funciona melhor do que fixar um dia do mês: a base nova é detectada e processada no dia seguinte à publicação, seja lá quando ela ocorrer, sem precisar reajustar o cron a cada vez que a Receita mudar a cadência.

Se a execução for interrompida (queda de rede, falha do servidor da Receita, reinício da máquina etc.) antes de terminar, a próxima chamada do cron **retoma de onde parou** em vez de recomeçar do zero: etapas cujo arquivo de saída já existe e passa a checagem de sanidade são puladas. Se qualquer etapa falhar de verdade (não só "ainda não terminou"), a base em produção não é alterada (ou é restaurada a partir do backup, se a falha ocorrer durante a própria troca). Ele também mantém `rede/rede.ini.local` em dia: limpa `referencia_bd` e `exibe_mensagem_advertencia` (o rótulo/aviso de base de teste) e sincroniza a seção `[RFB]` com o mês mais recente disponível na Receita — os ajustes manuais de `rede.ini.local` do passo anterior não precisam ser repetidos.

Nos dias em que há base nova para processar, exige pelo menos ~70GB livres no início (o conjunto novo de bases fica perto do tamanho do atual, mais a folga para os zips/csvs temporários da etapa de geração do cnpj.db) — o script aborta antes de começar se não houver espaço suficiente. Nos demais dias essa checagem nem roda.

```bash
# usa o mesmo venv criado na seção anterior
crontab -e
```

Adicione a linha (ajuste os caminhos para o seu servidor; roda toda meia-noite — na maioria dos dias só faz a consulta rápida e sai):

```
0 0 * * * $HOME/rede-cnpj/rede_cria_tabelas/.venv/bin/python $HOME/rede-cnpj/rede_cria_tabelas/atualiza_base.py
```

Os logs de cada execução ficam em `rede_cria_tabelas/logs/` (um arquivo por dia; nos dias sem base nova o log tem só uma ou duas linhas). O script resolve o caminho do `docker-compose` sozinho (via `PATH` ou `/usr/local/bin/docker-compose`), já que o `PATH` do cron costuma ser mais restrito que o do shell interativo.

#### Monitoramento de erros (Sentry)

`atualiza_base.py` reporta falhas do pipeline à Sentry (mesmo projeto/conta usado no argos) se a variável `SENTRY_DSN` estiver definida. Como o `crontab` não lê `.bashrc`/`.profile`, defina-a diretamente no próprio crontab, numa linha antes da tarefa:

```
SENTRY_DSN=https://sua-chave@seu-host-sentry/id-do-projeto
0 0 * * * $HOME/rede-cnpj/rede_cria_tabelas/.venv/bin/python $HOME/rede-cnpj/rede_cria_tabelas/atualiza_base.py
```

Sem `SENTRY_DSN` definido, o script segue funcionando normalmente — só fica sem alerta remoto em caso de falha (o log em `rede_cria_tabelas/logs/` continua sendo a fonte primária). Opcional: `SENTRY_ENVIRONMENT` (padrão `production`).

### Cria serviço no Linux para iniciar automaticamente o docker-compose

Criar o arquivo do serviço

```bash
sudo nano /etc/systemd/system/rede-cnpj.service
```

Conteúdo (ajuste `WorkingDirectory` e `User` para o usuário/caminho do seu servidor):

```
[Unit]
Description=Rede-CNPJ Docker
Requires=docker.service
After=docker.service

[Service]
WorkingDirectory=/home/inovacao/rede-cnpj/deploy
ExecStart=/usr/local/bin/docker-compose up
ExecStop=/usr/local/bin/docker-compose down
Restart=always
User=inovacao

[Install]
WantedBy=multi-user.target
```

Ativar o serviço e inicia-lo:

```bash
sudo systemctl enable rede-cnpj.service
sudo systemctl start rede-cnpj.service

# para verificar o status do serviço
sudo systemctl status rede-cnpj.service
```
