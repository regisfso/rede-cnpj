# -*- coding: utf-8 -*-
"""
Script para download resiliente de dados públicos do CNPJ.
Verifica arquivos existentes, retoma downloads e valida integridade.
"""
from bs4 import BeautifulSoup
import requests, os, time, zipfile
from tqdm import tqdm  # Adicione esta linha no início do script

url_dados_abertos = "https://arquivos.receitafederal.gov.br/cnpj/dados_abertos_cnpj/"
pasta_zip = r"dados-publicos-zip"
pasta_cnpj = "dados-publicos"

# Configurações
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
}
max_tentativas = 3  # Número máximo de tentativas por arquivo


def requisitos():
    """Cria pastas se não existirem, sem apagar arquivos existentes."""
    os.makedirs(pasta_cnpj, exist_ok=True)
    os.makedirs(pasta_zip, exist_ok=True)


def get_remote_file_size(url):
    """Obtém o tamanho remoto do arquivo."""
    try:
        response = requests.head(url, headers=headers, allow_redirects=True, timeout=30)
        if response.status_code == 200:
            return int(response.headers.get("Content-Length", 0))
    except:
        pass
    return 0


def is_zip_valid(file_path):
    """Verifica se o arquivo ZIP é válido."""
    try:
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            return zip_ref.testzip() is None
    except:
        return False


def get_remote_file_size(url):
    """Obtém o tamanho remoto do arquivo de forma robusta."""
    try:
        with requests.get(url, headers=headers, stream=True, timeout=15) as response:
            if response.status_code == 200:
                return int(response.headers.get("Content-Length", 0))
    except Exception as e:
        print(f"⚠️  Erro ao obter tamanho remoto: {str(e)}")
    return -1  # Indica falha


def download_file(url, filename):
    """Baixa o arquivo com tratamento robusto de erros."""
    file_path = os.path.join(pasta_zip, filename)

    # Verificação inicial
    remote_size = get_remote_file_size(url)
    if remote_size == -1:
        print(f"❌ Não foi possível obter informações de {filename}")
        return False

    # Se o arquivo local existe e é válido, pula
    if os.path.exists(file_path):
        if os.path.getsize(file_path) == remote_size and is_zip_valid(file_path):
            print(f"⏩ {filename} já está OK.")
            return True
        else:
            print(f"⚠️  {filename} incompleto/corrompido. Reiniciando download.")
            os.remove(file_path)

    # Tenta baixar do zero
    for tentativa in range(1, max_tentativas + 1):
        try:
            print(f"\n📥 Tentativa {tentativa}/{max_tentativas} para {filename}")
            with requests.get(
                url, headers=headers, stream=True, timeout=60
            ) as response:
                response.raise_for_status()
                total_size = int(response.headers.get("Content-Length", 0))

                with open(file_path, "wb") as f, tqdm(
                    desc=filename,
                    total=total_size,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                ) as bar:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            bar.update(len(chunk))

            # Validação rigorosa
            if is_zip_valid(file_path) and os.path.getsize(file_path) == remote_size:
                print(f"✅ {filename} validado com sucesso!")
                return True
            else:
                raise Exception("Arquivo corrompido após download")

        except Exception as e:
            print(f"⚠️  Falha na tentativa {tentativa}: {str(e)}")
            if os.path.exists(file_path):
                os.remove(file_path)

    print(f"❌ Falha definitiva em {filename} após {max_tentativas} tentativas.")
    return False


def main():
    requisitos()
    print(f"\n{time.asctime()} - Iniciando...")

    # Obtém lista de arquivos
    soup = BeautifulSoup(requests.get(url_dados_abertos).text, "lxml")
    ultima_referencia = sorted(
        [
            link.get("href")
            for link in soup.find_all("a")
            if link.get("href").startswith("20")
        ]
    )[-1]
    url = url_dados_abertos + ultima_referencia
    soup = BeautifulSoup(requests.get(url).text, "lxml")

    lista = [
        url + link["href"] if not link["href"].startswith("http") else link["href"]
        for link in soup.find_all("a")
        if link["href"].endswith(".zip")
    ]

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
            if local_size == remote_size and is_zip_valid(file_path):
                print(f"⏩ {filename} já está OK. Pulando.")
                continue
        arquivos_para_baixar.append((url, filename))

    # Executa downloads mesmo com falhas parciais
    success_count = 0
    total = len(arquivos_para_baixar)

    if arquivos_para_baixar:
        print(f"\n🚀 Iniciando download de {total} arquivos...")
        for i, (url, filename) in enumerate(arquivos_para_baixar, 1):
            print(f"\n📦 Baixando arquivo {i}/{total}: {filename}")
            if download_file(url, filename):
                success_count += 1
    else:
        print("\n🎉 Todos os arquivos já estão atualizados!")

    print(
        f"\n✅ {success_count} arquivos baixados com sucesso | ❌ {len(arquivos_para_baixar)-success_count} falhas."
    )


if __name__ == "__main__":
    main()
    input("\nPressione Enter para sair.")
