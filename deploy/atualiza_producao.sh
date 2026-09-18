#!/bin/bash

######################
#### Documentação ####
######################
#
# Uso: ./atualiza_producao.sh [--rapido]
#
# Fluxo de atualização do rede-cnpj (app baseado em Docker):
#   1. Verificação de conectividade com GitHub, autenticação e de versão
#      (local vs origin/master)
#   2. Atualização do código (git pull)
#   3. Rebuild e troca do container "app" (deploy/docker-compose.yaml), com
#      health gate e rollback automático se o container novo não subir
#      saudável
#
# Este script só atualiza CÓDIGO. A atualização de DADOS (bases de CNPJ em
# rede/bases) é feita à parte, todo dia via cron, por
# rede_cria_tabelas/atualiza_base.py (ver deploy/README.md) — não é
# responsabilidade deste script e não é disparada por ele.
#
# Rollback automático (sem blue-green literal):
#   Este projeto não mantém dois containers simultâneos como o
#   dashboard-operacional (atlas). Em vez disso, a imagem que estava em uso
#   antes do build é identificada e retida localmente. Se o container novo
#   não responder dentro do tempo limite (health gate), o script volta
#   sozinho para essa imagem anterior e reinicia o container — mesma garantia
#   de "nunca fica travado numa versão quebrada" que o esquema de releases
#   atômicas do argos (fluxo_atualizacao.sh) entrega via symlink + health
#   gate + rollback, só que aqui usando a tag da imagem Docker em vez de um
#   symlink de release. Depois de um rollback, o workspace git também é
#   revertido para o commit que estava em produção (git reset --hard), para
#   a próxima execução não achar "já está atualizado" e pular o deploy de
#   verdade.
#
#   Downtime esperado: breve (poucos segundos), durante a troca do container
#   pelo `docker-compose up -d` (ou `systemctl restart rede-cnpj.service`,
#   veja abaixo). Não há socket-activation (como no argos) nem dois
#   containers no ar (como no atlas) para evitar esse blip — é o mesmo
#   comportamento que já existe hoje ao rodar `docker-compose up --build`
#   manualmente, só que agora com verificação automática de saúde e reversão
#   automática se algo quebrar.
#
# Compatibilidade com o serviço systemd (rede-cnpj.service, ver
# deploy/README.md):
#   Se esse serviço estiver ativo, ele mantém um `docker-compose up` em
#   foreground como processo principal da unidade, com Restart=always.
#   Recriar o container "app" por baixo dele com `docker-compose up -d`
#   poderia fazer esse processo cair e o systemd reagir com
#   `ExecStop=docker-compose down`, derrubando também o nginx. Por isso, se
#   `rede-cnpj.service` estiver ativo, a troca do container é feita via
#   `sudo systemctl restart rede-cnpj.service` em vez de `docker-compose up
#   -d app` diretamente. Isso exige uma regra sudoers NOPASSWD para esse
#   comando específico (ex.: `inovacao ALL=(ALL:ALL) NOPASSWD: /usr/bin/
#   systemctl restart rede-cnpj.service`), senão o script trava pedindo
#   senha em modo --rapido.
#
# Tags aceitas:
#
#   --rapido
#       Executa o fluxo sem parar para perguntas s/n, usando respostas
#       padronizadas para cada etapa:
#         - Já está na versão mais recente? ...... encerra direto (nada a fazer)
#         - Há alterações locais não commitadas? .. descarta e continua
#
#       Sem --rapido, o fluxo é interativo e pergunta em cada etapa, como
#       antes.
#
#   Sem tags
#       Comportamento interativo padrão (todas as perguntas aparecem
#       normalmente).
#
# Verificação de versão:
#   Antes de qualquer outra etapa, o script testa a conectividade de rede
#   (conexão TCP com o host do remote), verifica a autenticação com o GitHub
#   e compara o commit local com origin/master. Um bloqueio de
#   firewall/proxy é reportado como erro de rede, e não mais como "token
#   expirado". Se já estiver na versão mais recente: no modo --rapido
#   encerra direto (nada a fazer); no modo normal, pergunta se deseja
#   continuar mesmo assim (útil para forçar um rebuild/restart do container
#   sem código novo, ex. após alterar o docker-compose.yaml ou o nginx.conf
#   manualmente).
#
# Alterações locais não commitadas:
#   Antes do git pull, o script verifica se há alterações locais em arquivos
#   versionados (ex.: alguém editou um arquivo direto no servidor, sem
#   commitar) que poderiam ser sobrescritas pelo merge. Se houver, o diff é
#   exibido e: no modo normal, pergunta se deseja descartar essas alterações
#   e continuar; no modo --rapido, descarta automaticamente (git reset --hard)
#   e segue. Isso NÃO afeta arquivos não versionados (untracked) nem
#   rede/bases/*.db e rede/rede.ini, marcados com `git update-index
#   --skip-worktree` (ver deploy/README.md) — o git nem os enxerga como
#   modificados.
#
# Qualquer falha em qualquer etapa (conectividade de rede, autenticação,
# verificação de versão, atualização de código, build/troca do container)
# interrompe o script imediatamente (exit 1).

################
#### Funcoes ###
################

# Função para perguntar e obter a resposta
pergunta() {
    local mensagem=${1:-"Você deseja continuar? [s/n]"}
    local resposta

    if [[ $RAPIDO -eq 1 ]]; then
        echo "$mensagem [modo rápido: assumindo 'sim']"
        return 0
    fi

    while true; do
        read -p "$mensagem [s/n]" resposta

        if [[ -z $resposta ]]; then
            echo "Entrada inválida. Responda com 'S' para sim ou 'N' para não."
            continue
        fi

        if [[ $resposta =~ ^[SsNn]$ ]]; then
            [[ $resposta =~ ^[Ss]$ ]]
            return
        else
            echo "Entrada inválida. Responda com 'S' para sim ou 'N' para não."
        fi
    done
}

# Imprime um separador visual para marcar o início de cada etapa do fluxo
etapa() {
    local titulo="$1"
    echo ""
    echo "=========================================================="
    echo ">> $titulo"
    echo "=========================================================="
}

# Resolve o binário do docker-compose: o PATH de sessões não interativas
# (cron, alguns logins via su) costuma ser mais restrito que o do shell
# interativo -- mesmo problema já tratado em rede_cria_tabelas/atualiza_base.py.
resolve_docker_compose() {
    command -v docker-compose 2>/dev/null || echo /usr/local/bin/docker-compose
}

# Espera até HEALTH_TIMEOUT segundos o container "app" responder a uma
# requisição HTTP real. Roda DENTRO do container via python -c/urllib (a
# imagem python:3.13-slim não tem curl instalado). Sem endpoint /health
# dedicado: bate na raiz ("/"), que faz um redirect -- qualquer resposta HTTP
# (mesmo um 404 do destino do redirect, ou um 429 do rate limiter em rotas
# protegidas por @limiter.limit) já prova que o gunicorn/Flask subiu e está
# processando requisições. Só falha em erro de CONEXÃO (URLError: recusada,
# timeout) -- um HTTPError é tratado como "app respondeu" e não derruba o gate.
health_gate() {
    local fim=$((SECONDS + HEALTH_TIMEOUT))
    while [ "$SECONDS" -lt "$fim" ]; do
        if "$DOCKER_COMPOSE_BIN" exec -T app python -c '
import urllib.request, urllib.error
try:
    urllib.request.urlopen("http://localhost:8000/", timeout=3)
except urllib.error.HTTPError:
    pass
' >/dev/null 2>&1; then
            return 0
        fi
        sleep 2
    done
    return 1
}


#############
#### Main ###
#############

# Modo rápido: pula prompts e assume respostas padronizadas
RAPIDO=0
for arg in "$@"; do
    case "$arg" in
        --rapido) RAPIDO=1 ;;
    esac
done

# Variáveis globais
APP_DIR="$HOME/rede-cnpj"
COMPOSE_DIR="$APP_DIR/deploy"
HEALTH_TIMEOUT=60   # segundos de tolerância no health gate

etapa "Verificação de versão"
cd "$APP_DIR" || { echo "Erro: Não foi possível acessar $APP_DIR"; exit 1; }

# Verificação de conectividade de rede (ANTES da autenticação)
# Um bloqueio de firewall/proxy de saída faz o 'git ls-remote' travar e, sem
# este teste, o erro aparece enganosamente como "falha de autenticação". Aqui
# extraímos host/porta do remote configurado e testamos só a conexão TCP.
echo "Verificando conectividade de rede com o GitHub..."
REMOTE_URL=$(git remote get-url origin 2>/dev/null)
if [[ "$REMOTE_URL" =~ ^https?://([^@/]+@)?([^/:]+)(:([0-9]+))? ]]; then
    GIT_HOST="${BASH_REMATCH[2]}"
    GIT_PORT="${BASH_REMATCH[4]:-443}"
elif [[ "$REMOTE_URL" =~ ^(ssh://)?git@([^/:]+) ]]; then
    GIT_HOST="${BASH_REMATCH[2]}"
    GIT_PORT=22
else
    GIT_HOST="github.com"
    GIT_PORT=443
fi

if ! timeout 10 bash -c "cat < /dev/null > /dev/tcp/${GIT_HOST}/${GIT_PORT}" 2>/dev/null; then
    echo "Erro: não foi possível abrir conexão TCP com ${GIT_HOST}:${GIT_PORT}."
    echo "Isso indica BLOQUEIO DE REDE/FIREWALL ou PROXY - NÃO é problema de token."
    echo ""
    echo "Para diagnosticar:"
    echo "  getent hosts ${GIT_HOST}                                    # o DNS resolve?"
    echo "  curl -v -m 15 https://${GIT_HOST}                           # a conexão HTTPS completa?"
    echo "  timeout 10 bash -c 'cat </dev/null >/dev/tcp/${GIT_HOST}/${GIT_PORT}'   # a porta está liberada?"
    echo ""
    echo "Se houver proxy corporativo, configure o git:"
    echo "  git config --global http.proxy http://USUARIO:SENHA@PROXY:PORTA"
    exit 1
fi
echo "Conexão TCP com ${GIT_HOST}:${GIT_PORT} OK."

# Verificação de autenticação com GitHub
echo "Verificando autenticação com GitHub..."
GIT_TERMINAL_PROMPT=0 timeout 20 git ls-remote origin > /dev/null 2>&1
RC=$?
if [ $RC -eq 124 ]; then
    echo "Erro: 'git ls-remote' expirou (timeout) mesmo com a porta ${GIT_PORT} acessível."
    echo "Possíveis causas: proxy exigido para o git, inspeção/MITM de TLS ou rede instável."
    exit 1
elif [ $RC -ne 0 ]; then
    echo "Erro: Falha na autenticação com GitHub. O token pode estar expirado."
    echo "Abortando antes de continuar."
    echo ''
    echo 'gh auth login'
    echo '# github.com'
    echo '# HTTPS'
    echo '# Authenticate git with your github credentials = yes'
    echo '# paste a authentication token'
    echo '# criar novo Fine-grained personal access tokens'
    echo 'liberar content'
    exit 1
fi

echo "Verificando se há uma nova versão disponível..."
git fetch origin master --quiet
if [[ $? -ne 0 ]]; then
    echo "Erro: Falha ao verificar atualizações no GitHub (git fetch). O script será interrompido."
    exit 1
fi

LOCAL_HASH=$(git rev-parse HEAD)
REMOTE_HASH=$(git rev-parse origin/master)

if [[ "$LOCAL_HASH" == "$REMOTE_HASH" ]]; then
    echo "O código já está na versão mais recente (commit ${LOCAL_HASH:0:7}). Não há atualização disponível."
    if [[ $RAPIDO -eq 1 ]]; then
        echo "Modo rápido: nada a atualizar. Encerrando."
        exit 0
    elif ! pergunta "Não há atualização de código disponível. Deseja continuar o fluxo mesmo assim (rebuild/restart do container)?"; then
        echo "Fluxo interrompido pelo usuário."
        exit 0
    fi
else
    echo "Nova versão disponível: ${LOCAL_HASH:0:7} -> ${REMOTE_HASH:0:7}."
fi

etapa "1/2 - Atualização do código"
cd "$APP_DIR" || { echo "Erro: Não foi possível acessar $APP_DIR"; exit 1; }

# Alterações locais em arquivos versionados travam o git pull (merge
# abortado). Detecta isso ANTES de tentar o pull, mostra o problema e deixa
# o usuário decidir (ou descarta automaticamente no modo --rapido), em vez
# de deixar o git pull falhar com uma mensagem de erro crua.
if ! git diff --quiet HEAD --; then
    echo "Foram encontradas alterações locais não commitadas que podem ser sobrescritas pela atualização:"
    echo ""
    git status --short
    echo ""
    git diff HEAD
    echo ""
    if [[ $RAPIDO -eq 1 ]]; then
        echo "Modo rápido: descartando alterações locais automaticamente."
        git reset --hard HEAD
    elif pergunta "Deseja descartar essas alterações locais e continuar a atualização?"; then
        git reset --hard HEAD
    else
        echo "Fluxo interrompido pelo usuário. Nenhuma alteração local foi descartada."
        exit 0
    fi

    if [[ $? -ne 0 ]]; then
        echo "Erro: Falha ao descartar as alterações locais (git reset --hard). O script será interrompido."
        exit 1
    fi
fi

echo 'Realizando atualização do rede-cnpj...'
git pull
if [[ $? -ne 0 ]]; then
    echo "Erro: git pull falhou. O script será interrompido."
    exit 1
fi

etapa "2/2 - Rebuild e troca do container (health gate + rollback automático)"
cd "$COMPOSE_DIR" || { echo "Erro: Não foi possível acessar $COMPOSE_DIR"; exit 1; }

DOCKER_COMPOSE_BIN=$(resolve_docker_compose)
if ! "$DOCKER_COMPOSE_BIN" version >/dev/null 2>&1; then
    echo "Erro: docker-compose não encontrado (procurado no PATH e em /usr/local/bin/docker-compose)."
    exit 1
fi

# Se rede-cnpj.service estiver ativo, ele já mantém um `docker-compose up`
# em foreground com Restart=always (ver deploy/README.md) -- recriar o
# container "app" por baixo dele com `up -d app` arrisca derrubar também o
# nginx (ver documentação no topo deste arquivo). Nesse caso a troca é feita
# reiniciando o serviço; sem o serviço, `up -d app` direto é suficiente.
USA_SYSTEMD=0
if systemctl is-active --quiet rede-cnpj.service 2>/dev/null; then
    USA_SYSTEMD=1
    echo "rede-cnpj.service está ativo: a troca do container será feita via 'systemctl restart'."
fi

sobe_app() {
    if [[ $USA_SYSTEMD -eq 1 ]]; then
        sudo systemctl restart rede-cnpj.service
    else
        "$DOCKER_COMPOSE_BIN" up -d app
    fi
}

# Captura a imagem atualmente em produção ANTES do build, para poder voltar
# a ela se a imagem nova não ficar saudável.
CONTAINER_ANTERIOR=$("$DOCKER_COMPOSE_BIN" ps -q app)
IMAGEM_ANTERIOR=""
REPO_TAG_ANTERIOR=""
if [[ -n "$CONTAINER_ANTERIOR" ]]; then
    IMAGEM_ANTERIOR=$(docker inspect --format='{{.Image}}' "$CONTAINER_ANTERIOR" 2>/dev/null)
    REPO_TAG_ANTERIOR=$(docker inspect --format='{{index .RepoTags 0}}' "$IMAGEM_ANTERIOR" 2>/dev/null)
fi

echo "Construindo a nova imagem (o container atual continua no ar)..."
"$DOCKER_COMPOSE_BIN" build app
if [[ $? -ne 0 ]]; then
    echo "Erro: Falha ao construir a imagem com docker-compose. O script será interrompido."
    exit 1
fi

echo "Subindo o container novo..."
sobe_app
if [[ $? -ne 0 ]]; then
    echo "Erro: Falha ao subir o container novo. O script será interrompido."
    exit 1
fi

echo "Aguardando health gate (até ${HEALTH_TIMEOUT}s)..."
if ! health_gate; then
    echo ""
    echo "Erro: o container novo não respondeu dentro de ${HEALTH_TIMEOUT}s."
    if [[ -n "$IMAGEM_ANTERIOR" && -n "$REPO_TAG_ANTERIOR" ]]; then
        echo "Rollback: voltando para a imagem anterior ($REPO_TAG_ANTERIOR)..."
        docker tag "$IMAGEM_ANTERIOR" "$REPO_TAG_ANTERIOR"
        sobe_app
        if health_gate; then
            echo "Rollback OK: imagem anterior de volta no ar."
            # Mantém o workspace git alinhado com o que está em produção --
            # senão a próxima execução (--rapido) compara HEAD com
            # origin/master, já vê "atualizado" (o pull já rodou) e sai sem
            # tentar de novo, apesar de produção ter voltado para a versão
            # anterior.
            echo "Revertendo o workspace para ${LOCAL_HASH:0:7}, para ficar de acordo com a imagem em produção..."
            git -C "$APP_DIR" reset --hard "$LOCAL_HASH"
        else
            echo "!! Rollback também não respondeu. Investigue manualmente:"
            echo "   $DOCKER_COMPOSE_BIN logs app | tail -n 80"
        fi
    else
        echo "!! Sem imagem anterior identificada para rollback (primeiro deploy?)."
        echo "   Investigue manualmente: $DOCKER_COMPOSE_BIN logs app | tail -n 80"
    fi
    exit 1
fi
echo "Container novo respondendo normalmente."

etapa "Concluído"
echo "Script concluído com sucesso!"
exit 0
