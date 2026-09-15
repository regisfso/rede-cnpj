# -*- coding: utf-8 -*-
"""
Script para download resiliente de dados públicos do CNPJ.
Verifica arquivos existentes, retoma downloads e valida integridade.
"""
import requests, os, sys, time, zipfile, re, json, random
from xml.etree import ElementTree
from tqdm import tqdm
from tqdm.contrib.concurrent import thread_map
from requests.adapters import HTTPAdapter


pasta_zip = r"dados-publicos-zip"
pasta_cnpj = "dados-publicos"

# Override manual: se definido, usado no lugar da descoberta automática (útil
# se descobrir_share_token() parar de funcionar). Por padrão None -- o token é
# descoberto a cada execução seguindo o redirect de arquivos.receitafederal.gov.br,
# que sempre aponta para o compartilhamento público vigente. A Receita já trocou
# esse token/layout mais de uma vez (fev/2026, set/2026) sem aviso.
SHARE_TOKEN = None

# Caminho, dentro do compartilhamento vigente, onde ficam as pastas YYYY-MM do
# CNPJ. Também já mudou (antes ficava na raiz do share). Se a Receita mudar de
# novo, consulta_base_webdap levanta um erro explicando como atualizar isto.
CAMINHO_PASTA_CNPJ = "Dados/Cadastros/CNPJ"

# Configurações
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
}
# O servidor da Receita derruba conexões com frequência sob carga; como agora as
# tentativas retomam de onde pararam (Range), múltiplas tentativas ficam baratas.
# Backoff exponencial com teto de 30s: pior caso (arquivo totalmente inacessível)
# é ~60s de espera por arquivo antes de desistir, não minutos.
max_tentativas = int(os.environ.get("CNPJ_MAX_TENTATIVAS", "6"))
BACKOFF_TETO_SEGUNDOS = 30
# O servidor limita conexões simultâneas por IP (observado: 4 simultâneos = timeouts
# em cascata). Ajustável via variável de ambiente sem editar o script.
max_concorrentes = int(os.environ.get("CNPJ_MAX_CONCORRENTES", "2"))

sessao = requests.Session()
sessao.headers.update(headers)
_adapter = HTTPAdapter(pool_connections=max_concorrentes, pool_maxsize=max_concorrentes)
sessao.mount("https://", _adapter)
sessao.mount("http://", _adapter)


class ArquivoCorrompidoError(Exception):
    """Levantada quando o arquivo, já com o tamanho esperado, falha na validação
    de integridade -- diferente de uma falha de transporte, aqui não há como saber
    quais bytes estão errados, então o único jeito seguro é recomeçar do zero."""


def requisitos():
    """Cria pastas se não existirem, sem apagar arquivos existentes."""
    os.makedirs(pasta_cnpj, exist_ok=True)
    os.makedirs(pasta_zip, exist_ok=True)


def _requisicao_com_retry(metodo, url, **kwargs):
    """Repete a requisição em caso de falha de transporte (timeout, conexão
    recusada/derrubada) ou erro 5xx do servidor, com o mesmo backoff exponencial
    usado nos downloads. Erros 4xx propagam na hora -- um 401/403 normalmente
    indica share_token inválido, que uma nova tentativa não resolve."""
    ultimo_erro = None
    for tentativa in range(1, max_tentativas + 1):
        try:
            response = sessao.request(metodo, url, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code < 500:
                raise
            ultimo_erro = e
        except requests.exceptions.RequestException as e:
            ultimo_erro = e

        print(f"⚠️  Falha ({tentativa}/{max_tentativas}) ao consultar {url}: {ultimo_erro}")
        if tentativa < max_tentativas:
            time.sleep(min(BACKOFF_TETO_SEGUNDOS, 2 ** tentativa) + random.uniform(0, 1))
    raise ultimo_erro


def descobrir_share_token():
    """Descobre o token do compartilhamento público vigente seguindo o redirect
    de https://arquivos.receitafederal.gov.br/ -- é um 302 HTTP simples (sem JS),
    não precisa de navegador/Playwright para capturar."""
    response = _requisicao_com_retry("GET", "https://arquivos.receitafederal.gov.br/", timeout=(15, 60))
    match = re.search(r"/index\.php/s/([A-Za-z0-9]+)", response.url)
    if not match:
        raise RuntimeError(f"Não encontrei o share_token na URL final ({response.url}).")
    return match.group(1)


def consulta_base_webdap(share_token=None, base_url="https://arquivos.receitafederal.gov.br/public.php/webdav"):
    """Lista o mês mais recente e os arquivos zip disponíveis via WebDAV, junto com
    o tamanho (getcontentlength) e a etag de cada um -- evita uma requisição GET
    extra por arquivo só para descobrir o tamanho, e permite detectar com segurança
    se um parcial em disco pertence à versão atual do arquivo remoto."""
    token = share_token or SHARE_TOKEN or descobrir_share_token()
    DAV_NS = {"d": "DAV:"}  # WebDAV XML namespace
    url = f"{base_url}/{CAMINHO_PASTA_CNPJ}/"
    response = _requisicao_com_retry("PROPFIND", url, auth=(token, ""), headers={"Depth": "1"}, timeout=(15, 60))
    root = ElementTree.fromstring(response.content)

    directories = []
    for resp in root.findall("d:response", DAV_NS):
        href = resp.find("d:href", DAV_NS).text
        match = re.search(r"(\d{4}-\d{2})/?$", href)  # pastas no formato YYYY-MM
        if match:
            directories.append(match.group(1))

    if not directories:
        raise RuntimeError(
            f"Nenhuma pasta YYYY-MM encontrada em {url}. A Receita deve ter mudado "
            "o layout do compartilhamento de novo -- navegue até "
            "https://arquivos.receitafederal.gov.br/ , localize a pasta atual do "
            "CNPJ e atualize CAMINHO_PASTA_CNPJ."
        )

    ultimoAnoMes = directories[-1]
    # obtem lista de arquivos do mês mais recente
    response = _requisicao_com_retry("PROPFIND", url + ultimoAnoMes + "/", auth=(token, ""), headers={"Depth": "1"}, timeout=(15, 60))
    root = ElementTree.fromstring(response.content)

    files = []
    tamanhos = {}
    etags = {}
    for resp in root.findall("d:response", DAV_NS):
        href = resp.find("d:href", DAV_NS).text
        match = re.search(r"/([^/]+\.zip)$", href, re.IGNORECASE)
        if not match:
            continue
        filename = match.group(1)
        files.append(filename)
        prop = resp.find("d:propstat/d:prop", DAV_NS)
        tamanho_el = prop.find("d:getcontentlength", DAV_NS) if prop is not None else None
        etag_el = prop.find("d:getetag", DAV_NS) if prop is not None else None
        tamanhos[filename] = int(tamanho_el.text) if tamanho_el is not None and tamanho_el.text else None
        etags[filename] = etag_el.text if etag_el is not None else None

    urlBaseArquivosDoMes = f"https://arquivos.receitafederal.gov.br/public.php/dav/files/{token}/{CAMINHO_PASTA_CNPJ}/{ultimoAnoMes}/"
    return {
        "anoMes": ultimoAnoMes,
        "urlBaseArquivosDoMes": urlBaseArquivosDoMes,
        "arquivos": files,
        "tamanhos": tamanhos,
        "etags": etags,
        "shareToken": token,
    }


def consulta_base():
    """Consulta a lista de arquivos disponíveis via webdap."""
    try:
        r = consulta_base_webdap()
        print("Consulta por webdap")
        return r
    except requests.exceptions.HTTPError as e:
        # 401/403 tipicamente indica share_token inválido/expirado -- retentar não ajuda.
        print(f"⚠️  Erro de autorização ao consultar a lista de arquivos via webdap: {e}")
        print("Navegue até https://arquivos.receitafederal.gov.br/ e localize a pasta Dados>Cadastros>CNPJ")
        print("A url da página conterá um código (share_token) após .../index.php/s/")
        print("Copie o código e informe manualmente em SHARE_TOKEN, se a auto-descoberta não bastar.")
        return None
    except Exception as e:
        print(f"⚠️  Erro ao consultar a lista de arquivos via webdap (após até {max_tentativas} tentativas): {e}")
        return None


def is_zip_valid(file_path):
    """Verifica a integridade completa do ZIP (decomprime e confere o CRC de
    cada membro). Caro para arquivos grandes -- rodar só uma vez, logo após um
    download terminar, nunca como checagem de 'já está OK' a cada execução."""
    try:
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            return zip_ref.testzip() is None
    except Exception:
        return False


def is_zip_structurally_ok(file_path):
    """Checagem barata: só confirma que o diretório central do ZIP é legível
    (arquivo não truncado). Não decomprime membros, então não detecta corrupção
    de dados -- suficiente para decidir se um arquivo já baixado pode ser pulado."""
    try:
        with zipfile.ZipFile(file_path, "r"):
            return True
    except Exception:
        return False


def etag_path_de(file_path):
    return file_path + ".etag"


def download_file(url, filename, expected_size, expected_etag):
    """Baixa o arquivo com retomada (HTTP Range) e backoff exponencial com jitter.

    Em falha de transporte (timeout, conexão derrubada) o parcial é preservado e a
    próxima tentativa retoma de onde parou. O parcial só é descartado quando o
    arquivo COMPLETO falha na validação de integridade, ou quando não há garantia
    de que ele pertence à versão atual do arquivo remoto (ver etag abaixo).
    """
    file_path = os.path.join(pasta_zip, filename)
    etag_path = etag_path_de(file_path)
    local_size = 0

    if os.path.exists(file_path):
        local_size = os.path.getsize(file_path)
        tamanho_ok = expected_size is None or local_size == expected_size
        if tamanho_ok and is_zip_structurally_ok(file_path):
            print(f"⏩ {filename} já está OK.")
            return True

        # Sem tamanho esperado não há como avaliar se um parcial é retomável com
        # segurança; e um parcial "menor que o esperado" só é confiável se a etag
        # bater com a registrada quando o download começou -- caso contrário pode
        # ser sobra de um mês anterior (dados-publicos-zip não é versionado por mês)
        # cujo tamanho por coincidência é menor que o do arquivo atual.
        etag_registrada = None
        if os.path.exists(etag_path):
            try:
                etag_registrada = open(etag_path, "r", encoding="utf-8").read().strip()
            except OSError:
                etag_registrada = None

        pode_retomar = (
            expected_size is not None
            and local_size < expected_size
            and expected_etag is not None
            and etag_registrada == expected_etag
        )
        if not pode_retomar:
            os.remove(file_path)
            local_size = 0

    if local_size == 0 and expected_etag is not None:
        with open(etag_path, "w", encoding="utf-8") as f:
            f.write(expected_etag)

    for tentativa in range(1, max_tentativas + 1):
        try:
            resumindo = local_size > 0
            req_headers = {"Range": f"bytes={local_size}-"} if resumindo else {}
            modo = "ab" if resumindo else "wb"

            with sessao.get(url, headers=req_headers, stream=True, timeout=(15, 60)) as response:
                if resumindo and response.status_code == 200:
                    # servidor ignorou o Range: trata como resposta completa e recomeça.
                    modo = "wb"
                    local_size = 0
                else:
                    response.raise_for_status()
                    content_length = response.headers.get("Content-Length")
                    total_size = (local_size + int(content_length)) if content_length else expected_size
                    with open(file_path, modo) as f, tqdm(
                        desc=filename,
                        total=total_size,
                        initial=local_size,
                        unit="B",
                        unit_scale=True,
                        unit_divisor=1024,
                    ) as bar:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                bar.update(len(chunk))

            local_size = os.path.getsize(file_path)
            tamanho_ok = expected_size is None or local_size == expected_size
            if tamanho_ok and is_zip_valid(file_path):
                if os.path.exists(etag_path):
                    os.remove(etag_path)
                return True

            raise ArquivoCorrompidoError("tamanho ou CRC não confere após download completo")

        except ArquivoCorrompidoError as e:
            print(f"⚠️  {filename}: {e} (tentativa {tentativa}/{max_tentativas})")
            if os.path.exists(file_path):
                os.remove(file_path)
            local_size = 0
            if expected_etag is not None:
                with open(etag_path, "w", encoding="utf-8") as f:
                    f.write(expected_etag)

        except Exception as e:
            print(f"⚠️  Falha na tentativa {tentativa}/{max_tentativas} de {filename}: {str(e)}")
            local_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0

        if tentativa < max_tentativas:
            espera = min(BACKOFF_TETO_SEGUNDOS, 2 ** tentativa) + random.uniform(0, 1)
            time.sleep(espera)

    print(f"❌ Falha definitiva em {filename} após {max_tentativas} tentativas.")
    return False


def baixar_com_args(args):
    url, filename, expected_size, expected_etag = args
    return download_file(url, filename, expected_size, expected_etag)


def main():
    requisitos()
    print(f"\n{time.asctime()} - Iniciando...")

    # Obtém lista de arquivos via WebDAV
    parametrosSite = consulta_base()
    if not parametrosSite:
        print("❌ Não foi possível obter a lista de arquivos disponíveis.")
        sys.exit(1)

    ultima_referencia = parametrosSite["anoMes"]
    urlBaseArquivosDoMes = parametrosSite["urlBaseArquivosDoMes"]
    tamanhos = parametrosSite["tamanhos"]
    etags = parametrosSite["etags"]
    lista = [
        (urlBaseArquivosDoMes + arq, arq, tamanhos.get(arq), etags.get(arq))
        for arq in parametrosSite["arquivos"]
    ]

    # grava para quem orquestra este script (ex.: atualiza_base.py) preencher a
    # seção [RFB] do rede.ini sem precisar repetir a consulta ao WebDAV.
    with open(os.path.join(pasta_cnpj, "_ultima_referencia_rfb.json"), "w", encoding="utf-8") as f:
        json.dump(
            {
                "anoMes": ultima_referencia,
                "urlBaseArquivosDoMes": urlBaseArquivosDoMes,
                "urlPaginaDownloadMeses": f"https://arquivos.receitafederal.gov.br/index.php/s/{parametrosSite['shareToken']}",
            },
            f,
        )

    print(f"\nÚltima base disponível: {ultima_referencia}")
    print(f"\n{len(lista)} arquivos encontrados:")
    for url, _, _, _ in lista:
        print(f"🔗 {url}")

    # Filtra arquivos já válidos (checagem barata: só tamanho + diretório central legível)
    arquivos_para_baixar = []
    for url, filename, expected_size, expected_etag in lista:
        file_path = os.path.join(pasta_zip, filename)
        if os.path.exists(file_path):
            local_size = os.path.getsize(file_path)
            tamanho_ok = expected_size is None or local_size == expected_size
            if tamanho_ok and is_zip_structurally_ok(file_path):
                print(f"⏩ {filename} já está OK. Pulando.")
                continue
        arquivos_para_baixar.append((url, filename, expected_size, expected_etag))

    # Executa downloads mesmo com falhas parciais
    total = len(arquivos_para_baixar)

    if arquivos_para_baixar:
        print(
            f"\n🚀 Iniciando download de {total} arquivos com até {max_concorrentes} simultâneos...\n"
        )

        # dispara os downloads em paralelo e coleta True/False
        resultados = thread_map(
            baixar_com_args,
            arquivos_para_baixar,
            max_workers=max_concorrentes,
            desc="Download geral",
        )

        # soma os True para contar sucessos
        success_count = sum(resultados)

        # imprime o resumo usando success_count
        print(
            f"\n✅ {success_count} arquivos baixados com sucesso | ❌ {total - success_count} falhas."
        )
        if success_count < total:
            sys.exit(1)
    else:
        print("\n🎉 Todos os arquivos já estão atualizados!")


if __name__ == "__main__":
    main()
    if sys.stdin.isatty():
        input("\nPressione Enter para sair.")
