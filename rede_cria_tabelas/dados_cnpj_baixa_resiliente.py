# -*- coding: utf-8 -*-
"""
Script para download resiliente de dados públicos do CNPJ.
Verifica arquivos existentes, retoma downloads e valida integridade.
"""
import requests, os, time, zipfile, re, json
from xml.etree import ElementTree
from tqdm import tqdm
from tqdm.contrib.concurrent import thread_map


pasta_zip = r"dados-publicos-zip"
pasta_cnpj = "dados-publicos"

# usado tanto como default de consulta_base_webdap quanto para montar a url da
# página de download (urlPaginaDownloadMeses), que a função não retorna.
SHARE_TOKEN = "YggdBLfdninEJX9"

# Configurações
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
}
max_tentativas = 3  # Número máximo de tentativas por arquivo
max_concorrentes = 4  # Número de downloads simultâneos


def requisitos():
    """Cria pastas se não existirem, sem apagar arquivos existentes."""
    os.makedirs(pasta_cnpj, exist_ok=True)
    os.makedirs(pasta_zip, exist_ok=True)


def consulta_base_webdap(share_token=SHARE_TOKEN, base_url="https://arquivos.receitafederal.gov.br/public.php/webdav"):
    """Lista o mês mais recente e os arquivos zip disponíveis via WebDAV.
    A Receita mudou o layout da página de download em fev/2026; caso o
    share_token pare de funcionar, será necessário obter um novo em
    https://arquivos.receitafederal.gov.br/ (pasta Dados>Cadastros>CNPJ)."""
    DAV_NS = {"d": "DAV:"}  # WebDAV XML namespace
    url = base_url + "/"
    response = requests.request("PROPFIND", url, auth=(share_token, ""), headers={"Depth": "1"})
    response.raise_for_status()
    root = ElementTree.fromstring(response.content)

    directories = []
    for response in root.findall("d:response", DAV_NS):
        href = response.find("d:href", DAV_NS).text
        match = re.search(r"(\d{4}-\d{2})/?$", href)  # pastas no formato YYYY-MM
        if match:
            directories.append(match.group(1))

    ultimoAnoMes = directories[-1]
    # obtem lista de arquivos do mês mais recente
    response = requests.request("PROPFIND", url + ultimoAnoMes + "/", auth=(share_token, ""), headers={"Depth": "1"})
    response.raise_for_status()
    root = ElementTree.fromstring(response.content)

    files = []
    for response in root.findall("d:response", DAV_NS):
        href = response.find("d:href", DAV_NS).text
        match = re.search(r"/([^/]+\.zip)$", href, re.IGNORECASE)
        if match:
            files.append(match.group(1))

    urlBaseArquivosDoMes = f"https://arquivos.receitafederal.gov.br/public.php/dav/files/{share_token}/{ultimoAnoMes}/"
    return {"anoMes": ultimoAnoMes, "urlBaseArquivosDoMes": urlBaseArquivosDoMes, "arquivos": files}


def consulta_base():
    """Consulta a lista de arquivos disponíveis via webdap."""
    try:
        r = consulta_base_webdap()
        print("Consulta por webdap")
        return r
    except Exception as e:
        print(f"⚠️  Erro ao consultar a lista de arquivos via webdap: {e}")
        print("Navegue até https://arquivos.receitafederal.gov.br/ e localize a pasta Dados>Cadastros>CNPJ")
        print("A url da página conterá um código (share_token) após .../index.php/s/")
        print("Copie o código e atualize o parâmetro share_token de consulta_base_webdap.")
        return None


def is_zip_valid(file_path):
    """Verifica se o arquivo ZIP é válido."""
    try:
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            return zip_ref.testzip() is None
    except:
        return False


def get_remote_file_size(url):
    """Obtém o tamanho remoto do arquivo. Retorna -1 se não for possível
    determinar (falha de conexão ou servidor não informa Content-Length),
    caso em que o tamanho não deve ser usado para validação."""
    try:
        with requests.get(url, headers=headers, stream=True, timeout=15) as response:
            if response.status_code == 200:
                content_length = response.headers.get("Content-Length")
                return int(content_length) if content_length is not None else -1
    except Exception as e:
        print(f"⚠️  Erro ao obter tamanho remoto: {str(e)}")
    return -1


def download_file(url, filename):
    """Baixa o arquivo com tratamento robusto de erros."""
    file_path = os.path.join(pasta_zip, filename)

    # remote_size == -1 significa que não foi possível determinar o tamanho
    # (falha de conexão, ou o servidor não informa Content-Length); nesse
    # caso a validação de tamanho é ignorada e usa-se apenas is_zip_valid.
    remote_size = get_remote_file_size(url)

    # Se o arquivo local existe e é válido, pula
    if os.path.exists(file_path):
        tamanho_ok = remote_size == -1 or os.path.getsize(file_path) == remote_size
        if tamanho_ok and is_zip_valid(file_path):
            print(f"⏩ {filename} já está OK.")
            return True
        else:
            print(f"⚠️  {filename} incompleto/corrompido. Reiniciando download.")
            os.remove(file_path)

    # Tenta baixar do zero
    for tentativa in range(1, max_tentativas + 1):
        try:
            # print(f"\n📥 Tentativa {tentativa}/{max_tentativas} para {filename}")
            with requests.get(
                url, headers=headers, stream=True, timeout=60
            ) as response:
                response.raise_for_status()
                content_length = response.headers.get("Content-Length")
                total_size = int(content_length) if content_length is not None else 0

                with open(file_path, "wb") as f, tqdm(
                    desc=filename,
                    total=total_size or None,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                ) as bar:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            bar.update(len(chunk))

            # Validação rigorosa
            tamanho_ok = remote_size == -1 or os.path.getsize(file_path) == remote_size
            if is_zip_valid(file_path) and tamanho_ok:
                # print(f"✅ {filename} validado com sucesso!")
                return True
            else:
                raise Exception("Arquivo corrompido após download")

        except Exception as e:
            print(f"⚠️  Falha na tentativa {tentativa}: {str(e)}")
            if os.path.exists(file_path):
                os.remove(file_path)

    print(f"❌ Falha definitiva em {filename} após {max_tentativas} tentativas.")
    return False


def baixar_com_args(args):
    url, filename = args
    return download_file(url, filename)


def main():
    requisitos()
    print(f"\n{time.asctime()} - Iniciando...")

    # Obtém lista de arquivos via WebDAV
    parametrosSite = consulta_base()
    if not parametrosSite:
        print("❌ Não foi possível obter a lista de arquivos disponíveis.")
        return

    ultima_referencia = parametrosSite["anoMes"]
    urlBaseArquivosDoMes = parametrosSite["urlBaseArquivosDoMes"]
    lista = [urlBaseArquivosDoMes + arq for arq in parametrosSite["arquivos"]]

    # grava para quem orquestra este script (ex.: atualiza_base.py) preencher a
    # seção [RFB] do rede.ini sem precisar repetir a consulta ao WebDAV.
    with open(os.path.join(pasta_cnpj, "_ultima_referencia_rfb.json"), "w", encoding="utf-8") as f:
        json.dump(
            {
                "anoMes": ultima_referencia,
                "urlBaseArquivosDoMes": urlBaseArquivosDoMes,
                "urlPaginaDownloadMeses": f"https://arquivos.receitafederal.gov.br/index.php/s/{SHARE_TOKEN}",
            },
            f,
        )

    print(f"\nÚltima base disponível: {ultima_referencia}")
    print(f"\n{len(lista)} arquivos encontrados:")
    for url in lista:
        print(f"🔗 {url}")

    # Filtra arquivos já válidos
    arquivos_para_baixar = []
    for url in lista:
        filename = os.path.basename(url)
        remote_size = get_remote_file_size(url)
        file_path = os.path.join(pasta_zip, filename)

        if os.path.exists(file_path):
            local_size = os.path.getsize(file_path)
            tamanho_ok = remote_size == -1 or local_size == remote_size
            if tamanho_ok and is_zip_valid(file_path):
                print(f"⏩ {filename} já está OK. Pulando.")
                continue
        arquivos_para_baixar.append((url, filename))

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
    else:
        print("\n🎉 Todos os arquivos já estão atualizados!")


if __name__ == "__main__":
    main()
    input("\nPressione Enter para sair.")
