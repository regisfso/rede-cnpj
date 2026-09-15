# Deploy com Docker

Conteineriza com Docker a instalação de produção deste fork do rede-cnpj (rictom/rede-cnpj).

O código da aplicação (incluindo o `wsgi.py`) é incorporado à imagem no build. Só a pasta `rede/bases` (bases sqlite) e o `rede/rede.ini` são montados como volume, para poderem ser atualizados sem reconstruir a imagem.

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

# cnpj.db, rede.db, rede_search.db, cnpj_links_ete.db e rede.ini em rede/bases são
# exemplos rastreados pelo git. Rode isto uma única vez, antes de sobrescrevê-los pela
# primeira vez, para o git parar de rastrear o conteúdo (senão toda base de produção
# nova, ou toda troca automática de rede.ini pelo atualiza_base.py, aparecerá como
# "modified" no git status, arriscando ser commitada por engano):
cd ~/rede-cnpj && git update-index --skip-worktree rede/bases/cnpj.db rede/bases/rede.db rede/bases/rede_search.db rede/bases/cnpj_links_ete.db rede/rede.ini

# Ao final, mova os arquivos de rede_cria_tabelas/dados-publicos para a rede/bases
cd ~/rede-cnpj/rede/bases && rm cnpj.db rede.db rede_search.db cnpj_links_ete.db

mv $HOME/rede-cnpj/rede_cria_tabelas/dados-publicos/cnpj.db $HOME/rede-cnpj/rede/bases/

mv $HOME/rede-cnpj/rede_cria_tabelas/dados-publicos/rede.db $HOME/rede-cnpj/rede/bases/

mv $HOME/rede-cnpj/rede_cria_tabelas/dados-publicos/rede_search.db $HOME/rede-cnpj/rede/bases/

mv $HOME/rede-cnpj/rede_cria_tabelas/dados-publicos/cnpj_links_ete.db $HOME/rede-cnpj/rede/bases/

# Ajustar o arquivo rede.ini (só é necessário rodando estes passos manualmente;
# rede_cria_tabelas/atualiza_base.py, descrito na próxima seção, faz isso sozinho)
nano $HOME/rede-cnpj/rede/rede.ini

referencia_bd = Abril/2025
exibe_mensagem_advertencia = 0
```

Depois de gerar uma base nova, basta reiniciar o container `app` (`cd ~/rede-cnpj/deploy && docker-compose restart app`) — como `rede/bases` é montada como volume (veja `docker-compose.yaml`), não é necessário reconstruir a imagem. Mudança de **código** (`git pull` com commits na pasta `rede/`) exige `docker-compose up --build`.

### Atualização automática mensal (cron)

Em vez de repetir os passos manuais acima a cada mês, `rede_cria_tabelas/atualiza_base.py` automatiza todo o processo: baixa os zips, gera as 4 bases numa pasta de staging, valida cada uma (tamanho mínimo e contagem de linhas numa tabela-chave) e só então troca os arquivos em `rede/bases` de forma atômica, reiniciando o container. Se qualquer etapa falhar, a base em produção não é alterada (ou é restaurada a partir do backup, se a falha ocorrer durante a própria troca). Ele também mantém `rede/rede.ini` em dia: limpa `referencia_bd` e `exibe_mensagem_advertencia` (o rótulo/aviso de base de teste) e sincroniza a seção `[RFB]` com o mês mais recente disponível na Receita — os ajustes manuais de `rede.ini` do passo anterior não precisam ser repetidos.

Exige pelo menos ~70GB livres no início (o conjunto novo de bases fica perto do tamanho do atual, mais a folga para os zips/csvs temporários da etapa de geração do cnpj.db) — o script aborta antes de começar se não houver espaço suficiente.

```bash
# usa o mesmo venv criado na seção anterior
crontab -e
```

Adicione a linha (ajuste os caminhos para o seu servidor; roda todo dia 1 às 3h):

```
0 3 1 * * $HOME/rede-cnpj/rede_cria_tabelas/.venv/bin/python $HOME/rede-cnpj/rede_cria_tabelas/atualiza_base.py
```

Os logs de cada execução ficam em `rede_cria_tabelas/logs/`. O script resolve o caminho do `docker-compose` sozinho (via `PATH` ou `/usr/local/bin/docker-compose`), já que o `PATH` do cron costuma ser mais restrito que o do shell interativo.

#### Monitoramento de erros (Sentry)

`atualiza_base.py` reporta falhas do pipeline à Sentry (mesmo projeto/conta usado no argos) se a variável `SENTRY_DSN` estiver definida. Como o `crontab` não lê `.bashrc`/`.profile`, defina-a diretamente no próprio crontab, numa linha antes da tarefa:

```
SENTRY_DSN=https://sua-chave@seu-host-sentry/id-do-projeto
0 3 1 * * $HOME/rede-cnpj/rede_cria_tabelas/.venv/bin/python $HOME/rede-cnpj/rede_cria_tabelas/atualiza_base.py
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
WorkingDirectory=/home/regis/rede-cnpj/deploy
ExecStart=/usr/local/bin/docker-compose up
ExecStop=/usr/local/bin/docker-compose down
Restart=always
User=regis

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
