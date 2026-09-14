#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Orquestra a atualização mensal da base pública de CNPJ para rodar sem intervenção
manual (ex.: via cron): baixa os arquivos da Receita, gera cnpj.db, rede.db,
rede_search.db e cnpj_links_ete.db, valida cada um e só então troca os arquivos em
produção (rede/bases) de forma atômica, reiniciando o container da aplicação.

Se qualquer etapa falhar, a base em produção não é tocada (ou é restaurada a partir
do backup, se a falha ocorrer durante a própria troca) e o processo termina com
código de saída != 0 -- a falha é reportada à Sentry (ver configura_sentry) se
SENTRY_DSN estiver definido no ambiente do cron.

Uso: python atualiza_base.py
Pasta de trabalho: os scripts chamados usam caminhos relativos (dados-publicos,
dados-publicos-zip), por isso este script sempre roda com cwd = sua própria pasta.
"""
import fcntl
import glob
import json
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time

import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))  # rede_cria_tabelas/
REPO_ROOT = os.path.dirname(SCRIPT_DIR)  # rede-cnpj/
BASES_DIR = os.path.join(REPO_ROOT, "rede", "bases")
REDE_INI = os.path.join(REPO_ROOT, "rede", "rede.ini")
STAGING_DIR = os.path.join(SCRIPT_DIR, "dados-publicos")
ZIP_DIR = os.path.join(SCRIPT_DIR, "dados-publicos-zip")
DEPLOY_DIR = os.path.join(REPO_ROOT, "deploy")
LOG_DIR = os.path.join(SCRIPT_DIR, "logs")
LOCK_PATH = os.path.join(SCRIPT_DIR, ".atualiza_base.lock")
METADADOS_RFB_PATH = os.path.join(STAGING_DIR, "_ultima_referencia_rfb.json")

QTDE_ZIPS_ESPERADA = 37

# espaço livre mínimo exigido no início, em GB. O conjunto novo de bases fica perto
# do tamanho do atual (hoje ~60GB); a folga cobre os zips + csvs temporários que
# coexistem com ele durante a geração do cnpj.db.
MIN_FREE_GB_START = 70

PYTHON = sys.executable

# arquivo final -> (tamanho mínimo em bytes, tabela p/ checagem de sanidade, linhas mínimas)
ARQUIVOS_FINAIS = {
    "cnpj.db": (15 * 2**30, "estabelecimento", 1_000_000),
    "rede.db": (2 * 2**30, "ligacao", 1),
    "rede_search.db": (3 * 2**30, "id_search", 1),
    "cnpj_links_ete.db": (2 * 2**30, "link_ete", 1),
}

# caminho do binário do docker-compose. Resolvido explicitamente porque o PATH do
# cron costuma ser bem mais curto que o do shell interativo (ex.: sem /usr/local/bin,
# onde o deploy/README.md manda instalá-lo) -- descobrir isso só na hora do restart,
# depois de já ter trocado os arquivos em produção, seria o pior momento possível.
DOCKER_COMPOSE_BIN = shutil.which("docker-compose") or "/usr/local/bin/docker-compose"

logger = logging.getLogger("atualiza_base")


class PipelineError(RuntimeError):
    """Erro em alguma etapa do pipeline de atualização da base."""


def configura_log():
    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = os.path.join(LOG_DIR, f"atualiza_base_{time.strftime('%Y%m%d_%H%M%S')}.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(log_path, encoding="utf-8"), logging.StreamHandler(sys.stdout)],
    )
    logger.info("Log desta execução em %s", log_path)


def _obter_sentry_release():
    """Versão reportada à Sentry: SENTRY_RELEASE do ambiente se definido, senão o SHA
    curto do commit atual (mesma lógica usada no projeto argos). Retorna None se nada
    estiver disponível -- a Sentry aceita release=None (fica sem agrupamento por
    versão)."""
    env_release = os.getenv("SENTRY_RELEASE", "").strip()
    if env_release:
        return env_release
    try:
        sha = subprocess.check_output(
            # -c safe.directory=*: evita "dubious ownership" quando o cron roda com um
            # usuário diferente do dono do checkout.
            ["git", "-c", "safe.directory=*", "rev-parse", "--short", "HEAD"],
            cwd=REPO_ROOT,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=2,
        ).strip()
    except (subprocess.SubprocessError, OSError):
        return None
    return f"rede-cnpj@{sha}" if sha else None


def configura_sentry():
    """Sem SENTRY_DSN no ambiente (ex.: variável não exportada no crontab -- crontab
    não lê .bashrc/.profile), o monitoramento fica desativado e o cron segue
    funcionando normalmente, só sem alerta remoto em caso de falha."""
    dsn = os.getenv("SENTRY_DSN", "").strip()
    if not dsn:
        logger.info("SENTRY_DSN não definido: monitoramento de erros via Sentry desativado.")
        return
    environment = os.getenv("SENTRY_ENVIRONMENT", "production").strip()
    sentry_sdk.init(
        dsn=dsn,
        environment=environment,
        release=_obter_sentry_release(),
        integrations=[
            # event_level=None: os logger.info/warning/error de cada etapa (roda_etapa
            # já loga a saída inteira dos subprocessos) viram breadcrumb, não issue
            # própria -- quem reporta o evento é o capture_exception explícito em
            # main(), com o traceback real e o histórico completo do pipeline até ali.
            LoggingIntegration(level=logging.INFO, event_level=None),
        ],
        traces_sample_rate=0,
        send_default_pii=False,
    )
    logger.info("Sentry inicializado (environment=%s).", environment)


def adquire_lock():
    """Evita duas execuções concorrentes (ex.: uma rodada anterior ainda não terminou
    quando o cron dispara de novo)."""
    lock_file = open(LOCK_PATH, "w")
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock_file.close()
        raise PipelineError(
            f"Já existe uma execução em andamento (lock {LOCK_PATH}). Abortando para não rodar duas em paralelo."
        )
    lock_file.write(str(os.getpid()))
    lock_file.flush()
    return lock_file


def verifica_espaco_livre(caminho, minimo_gb):
    livre_gb = shutil.disk_usage(caminho).free / 2**30
    logger.info("Espaço livre em %s: %.1f GB", caminho, livre_gb)
    if livre_gb < minimo_gb:
        raise PipelineError(
            f"Espaço livre insuficiente ({livre_gb:.1f} GB) em {caminho}. Mínimo exigido: {minimo_gb} GB."
        )


def limpa_staging():
    """Garante início limpo: remove qualquer cnpj.db/rede.db/etc. de uma execução
    anterior incompleta. Os scripts de geração recusam rodar se o arquivo de saída já
    existe (ou, no caso do cnpj.db, retomariam de onde pararam) -- e retomar um
    cnpj.db parcialmente gerado a partir de zips de um período de referência
    diferente corromperia a base nova silenciosamente."""
    if os.path.exists(STAGING_DIR):
        logger.info("Limpando pasta de staging %s", STAGING_DIR)
        shutil.rmtree(STAGING_DIR)
    os.makedirs(STAGING_DIR)


def roda_etapa(nome, script, entrada_stdin=""):
    """Roda um dos scripts do pipeline como subprocesso, transmitindo a saída linha a
    linha para o log (algumas etapas levam horas; sem isso não haveria nenhuma
    visibilidade até o fim, o que importa quando o Sentry entrar para monitorar isso).

    entrada_stdin fornece as respostas para os `input()` do script (ver levantamento
    na conversa: alguns têm confirmações incondicionais tipo 'Deseja prosseguir?
    Pressione Enter'). Passar stdin vazio faz qualquer prompt inesperado (ex.:
    quantidade de zips diferente do esperado) travar por EOF imediatamente -- o que é
    o comportamento desejado: falhar alto em vez de prosseguir com dados incompletos.
    """
    logger.info("=== Iniciando etapa: %s ===", nome)
    inicio = time.time()
    processo = subprocess.Popen(
        [PYTHON, script],
        cwd=SCRIPT_DIR,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    processo.stdin.write(entrada_stdin)
    processo.stdin.close()
    for linha in processo.stdout:
        logger.info("[%s] %s", nome, linha.rstrip())
    processo.wait()
    duracao_min = (time.time() - inicio) / 60
    if processo.returncode != 0:
        raise PipelineError(f"Etapa '{nome}' falhou (código {processo.returncode}) após {duracao_min:.1f} min.")
    logger.info("=== Etapa concluída: %s (%.1f min) ===", nome, duracao_min)


def verifica_zips_completos():
    qtde = len(glob.glob(os.path.join(ZIP_DIR, "*.zip")))
    if qtde != QTDE_ZIPS_ESPERADA:
        raise PipelineError(
            f"Download incompleto: {qtde}/{QTDE_ZIPS_ESPERADA} arquivos zip em {ZIP_DIR}."
        )
    logger.info("Download completo: %d/%d arquivos zip.", qtde, QTDE_ZIPS_ESPERADA)


def checa_sanidade(caminho, tamanho_minimo, tabela, linhas_minimas):
    if not os.path.exists(caminho):
        raise PipelineError(f"Arquivo esperado não foi gerado: {caminho}")
    tamanho = os.path.getsize(caminho)
    if tamanho < tamanho_minimo:
        raise PipelineError(
            f"{caminho} tem {tamanho / 2**30:.1f} GB, abaixo do mínimo esperado "
            f"({tamanho_minimo / 2**30:.1f} GB). Pode estar incompleto/corrompido."
        )
    try:
        con = sqlite3.connect(f"file:{os.path.abspath(caminho)}?mode=ro", uri=True)
        (linhas,) = con.execute(f"SELECT COUNT(*) FROM {tabela}").fetchone()
        con.close()
    except sqlite3.Error as e:
        raise PipelineError(f"Não foi possível abrir/consultar {caminho} (tabela {tabela}): {e}")
    if linhas < linhas_minimas:
        raise PipelineError(f"Tabela '{tabela}' em {caminho} tem só {linhas} linha(s), esperado >= {linhas_minimas}.")
    logger.info("Sanidade OK: %s (%.1f GB, %s linhas em %s)", os.path.basename(caminho), tamanho / 2**30, linhas, tabela)


def troca_arquivo(nome_arquivo):
    """Troca atômica (rename, mesmo filesystem) do arquivo em staging pelo de
    produção. O antigo vira .bak e só é removido depois que TODOS os arquivos
    trocarem e o container reiniciar com sucesso."""
    origem = os.path.join(STAGING_DIR, nome_arquivo)
    destino = os.path.join(BASES_DIR, nome_arquivo)
    backup = destino + ".bak"
    if os.path.exists(destino):
        os.replace(destino, backup)
    os.replace(origem, destino)
    logger.info("Trocado em produção: %s", destino)


def remove_backups():
    for bak in glob.glob(os.path.join(BASES_DIR, "*.bak")):
        os.remove(bak)
        logger.info("Backup removido: %s", bak)


def restaura_backups():
    """Rollback: se algo falhar depois que a troca já começou, restaura os .bak.
    Não sabemos, ao entrar aqui, quais arquivos já foram trocados com sucesso antes da
    falha -- então nunca sobrescreve um `destino` existente direto: se ele já é o
    arquivo novo (troca bem-sucedida), preserva-o em .novo_falhou em vez de descartar
    silenciosamente horas de geração por causa da falha em *outro* arquivo."""
    for nome_arquivo in ARQUIVOS_FINAIS:
        backup = os.path.join(BASES_DIR, nome_arquivo + ".bak")
        destino = os.path.join(BASES_DIR, nome_arquivo)
        if not os.path.exists(backup):
            continue
        if os.path.exists(destino):
            preservado = os.path.join(BASES_DIR, nome_arquivo + ".novo_falhou")
            os.replace(destino, preservado)
            logger.warning("Arquivo novo preservado para inspeção manual em %s", preservado)
        os.replace(backup, destino)
        logger.warning("Rollback: restaurado %s a partir do backup", destino)


def verifica_docker_compose():
    if not os.path.exists(DOCKER_COMPOSE_BIN):
        raise PipelineError(
            f"docker-compose não encontrado em '{DOCKER_COMPOSE_BIN}'. Sem ele não dá para reiniciar "
            "o container no final -- corrija o PATH do cron ou o caminho em DOCKER_COMPOSE_BIN antes de rodar."
        )


def verifica_rede_ini():
    if not os.access(REDE_INI, os.W_OK):
        raise PipelineError(f"{REDE_INI} não existe ou não é gravável.")


def le_metadados_rfb():
    """Lê o anoMes/urls que dados_cnpj_baixa_resiliente.py descobriu junto ao WebDAV
    da Receita, para replicar em rede.ini -- evita consultar o site de novo."""
    if not os.path.exists(METADADOS_RFB_PATH):
        raise PipelineError(
            f"Metadados do download não encontrados em {METADADOS_RFB_PATH} "
            "(dados_cnpj_baixa_resiliente.py não gravou o arquivo esperado)."
        )
    with open(METADADOS_RFB_PATH, encoding="utf-8") as f:
        return json.load(f)


def atualiza_ini_valor(caminho_ini, secao, chave, valor):
    """Substitui o valor de uma chave já existente em rede.ini editando o texto linha
    a linha, em vez de usar configparser -- que regravaria o arquivo sem os
    comentários que documentam cada parâmetro."""
    with open(caminho_ini, encoding="utf-8") as f:
        linhas = f.readlines()

    padrao_secao = re.compile(r"^\[" + re.escape(secao) + r"\]\s*$")
    padrao_chave = re.compile(r"^" + re.escape(chave) + r"\s*=")

    dentro_da_secao = False
    for i, linha in enumerate(linhas):
        if re.match(r"^\[.+\]\s*$", linha):
            dentro_da_secao = bool(padrao_secao.match(linha))
            continue
        if dentro_da_secao and padrao_chave.match(linha):
            linhas[i] = f"{chave} = {valor}\n"
            with open(caminho_ini, "w", encoding="utf-8") as f:
                f.writelines(linhas)
            return

    raise PipelineError(f"Chave '{chave}' não encontrada na seção [{secao}] de {caminho_ini}")


def atualiza_rede_ini(metadados_rfb):
    """Aplica em rede.ini o que antes era feito manualmente a cada carga (ver
    deploy/README.md, seção 'Criar os bancos de dados de produção'): limpa o rótulo
    de 'base de testes' e desliga o aviso de base de teste com nomes embaralhados (a
    data em si já é lida automaticamente do cnpj.db pela aplicação, essa parte não
    precisa de ajuste), e sincroniza a seção [RFB] usada pela API que informa aos
    usuários onde baixar o mês mais recente."""
    logger.info("Atualizando rede.ini...")
    atualiza_ini_valor(REDE_INI, "BASE", "referencia_bd", "")
    atualiza_ini_valor(REDE_INI, "INICIO", "exibe_mensagem_advertencia", "0")
    atualiza_ini_valor(REDE_INI, "RFB", "anoMes", metadados_rfb["anoMes"])
    atualiza_ini_valor(REDE_INI, "RFB", "urlBaseArquivosDoMes", metadados_rfb["urlBaseArquivosDoMes"])
    atualiza_ini_valor(REDE_INI, "RFB", "urlPaginaDownloadMeses", metadados_rfb["urlPaginaDownloadMeses"])
    logger.info("rede.ini atualizado (rótulos de teste limpos, [RFB] em %s).", metadados_rfb["anoMes"])


def reinicia_app():
    logger.info("Reiniciando container da aplicação...")
    resultado = subprocess.run(
        [DOCKER_COMPOSE_BIN, "restart", "app"], cwd=DEPLOY_DIR, capture_output=True, text=True
    )
    if resultado.returncode != 0:
        raise PipelineError(f"Falha ao reiniciar o container: {resultado.stderr}")
    logger.info("Container reiniciado com sucesso.")


def executa_pipeline():
    verifica_docker_compose()
    verifica_rede_ini()
    verifica_espaco_livre(REPO_ROOT, MIN_FREE_GB_START)
    limpa_staging()

    roda_etapa("download dos zips", "dados_cnpj_baixa_resiliente.py", entrada_stdin="\n")
    verifica_zips_completos()
    metadados_rfb = le_metadados_rfb()

    roda_etapa("geração do cnpj.db", "dados_cnpj_para_sqlite_progresso.py", entrada_stdin="")
    checa_sanidade(os.path.join(STAGING_DIR, "cnpj.db"), *ARQUIVOS_FINAIS["cnpj.db"])

    logger.info("Removendo zips já processados para liberar espaço...")
    shutil.rmtree(ZIP_DIR, ignore_errors=True)

    roda_etapa("geração do rede.db / rede_search.db", "rede_cria_tabela_rede.db.py", entrada_stdin="y\n\n")
    checa_sanidade(os.path.join(STAGING_DIR, "rede.db"), *ARQUIVOS_FINAIS["rede.db"])
    checa_sanidade(os.path.join(STAGING_DIR, "rede_search.db"), *ARQUIVOS_FINAIS["rede_search.db"])

    roda_etapa("geração do cnpj_links_ete.db", "rede_cria_tabela_cnpj_links_ete.py", entrada_stdin="y\n\n")
    checa_sanidade(os.path.join(STAGING_DIR, "cnpj_links_ete.db"), *ARQUIVOS_FINAIS["cnpj_links_ete.db"])

    logger.info("Todas as bases novas passaram na checagem de sanidade. Trocando em produção...")
    try:
        for nome_arquivo in ARQUIVOS_FINAIS:
            troca_arquivo(nome_arquivo)
        reinicia_app()
    except Exception:
        logger.error("Falha durante a troca em produção. Restaurando bases anteriores...")
        restaura_backups()
        raise

    # bases novas já em produção e o container já reiniciado com elas: uma falha
    # daqui pra frente não deve reverter os arquivos .db (que estão corretos), só
    # deixa o rede.ini para ser corrigido manualmente e o run reportado como falho.
    atualiza_rede_ini(metadados_rfb)

    remove_backups()
    shutil.rmtree(STAGING_DIR, ignore_errors=True)
    logger.info("Atualização concluída com sucesso.")


def main():
    configura_log()
    configura_sentry()
    try:
        lock_file = adquire_lock()
    except PipelineError as e:
        logger.warning("%s Encerrando sem erro.", e)
        return

    try:
        executa_pipeline()
    except PipelineError as e:
        logger.error("Pipeline abortado: %s", e)
        sentry_sdk.capture_exception(e)
        sys.exit(1)
    except Exception as e:
        logger.exception("Erro inesperado no pipeline: %s", e)
        sentry_sdk.capture_exception(e)
        sys.exit(1)
    finally:
        lock_file.close()
        try:
            os.remove(LOCK_PATH)
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    main()
