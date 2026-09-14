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
│   │   └── nginx.conf
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

# chave GPG
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

# repo do docker
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# instala docker engine
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io

# adiciona usuario ao grupo docker
sudo usermod -aG docker $USER
newgrp docker  # Atualiza as permissões sem precisar relogar

# baixa e instala docker-compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose

# permissão de execução
sudo chmod +x /usr/local/bin/docker-compose

# teste de instalação
docker-compose --version

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
# Rodar o projeto para testes, acessível em http://ip/rede
cd ~/rede-cnpj/deploy && docker-compose up --build
```

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
# rastreados pelo git. Rode isto uma única vez, antes de sobrescrevê-los pela primeira
# vez, para o git parar de rastrear o conteúdo (senão toda base de produção nova
# aparecerá como "modified" no git status, arriscando ser commitada por engano):
cd ~/rede-cnpj && git update-index --skip-worktree rede/bases/cnpj.db rede/bases/rede.db rede/bases/rede_search.db rede/bases/cnpj_links_ete.db

# Ao final, mova os arquivos de rede_cria_tabelas/dados-publicos para a rede/bases
cd ~/rede-cnpj/rede/bases && rm cnpj.db rede.db rede_search.db cnpj_links_ete.db

mv $HOME/rede-cnpj/rede_cria_tabelas/dados-publicos/cnpj.db $HOME/rede-cnpj/rede/bases/

mv $HOME/rede-cnpj/rede_cria_tabelas/dados-publicos/rede.db $HOME/rede-cnpj/rede/bases/

mv $HOME/rede-cnpj/rede_cria_tabelas/dados-publicos/rede_search.db $HOME/rede-cnpj/rede/bases/

mv $HOME/rede-cnpj/rede_cria_tabelas/dados-publicos/cnpj_links_ete.db $HOME/rede-cnpj/rede/bases/

# Ajustar o arquivo rede.ini
nano $HOME/rede-cnpj/rede/rede.ini

referencia_bd = Abril/2025
exibe_mensagem_advertencia = 0
```

Depois de gerar uma base nova, basta reiniciar o container `app` (`cd ~/rede-cnpj/deploy && docker-compose restart app`) — como `rede/bases` é montada como volume (veja `docker-compose.yaml`), não é necessário reconstruir a imagem. Mudança de **código** (`git pull` com commits na pasta `rede/`) exige `docker-compose up --build`.

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
