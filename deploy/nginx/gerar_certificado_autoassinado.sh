#!/bin/sh
# Gera um certificado autoassinado em deploy/nginx/certs, para uso com HTTPS.
# Rode este script manualmente, como um passo explícito do deploy (veja
# deploy/README.md) -- ele NÃO roda sozinho ao subir os containers, para você
# sempre saber se está usando um autoassinado (gerado aqui) ou um certificado
# real (colocado manualmente nesses mesmos arquivos).
#
# Uso:
#   ./gerar_certificado_autoassinado.sh [dominio_ou_ip] [dias_validade]
# Exemplo:
#   ./gerar_certificado_autoassinado.sh meuservidor.exemplo.com 825
set -e

CN="${1:-localhost}"
DIAS="${2:-825}"
CERTS_DIR="$(cd "$(dirname "$0")/certs" && pwd)"

CRT="$CERTS_DIR/fullchain.pem"
KEY="$CERTS_DIR/privkey.pem"

if [ -f "$CRT" ] || [ -f "$KEY" ]; then
    echo "Já existe um certificado em $CERTS_DIR (fullchain.pem e/ou privkey.pem)."
    echo "Para gerar um novo autoassinado, apague esses arquivos primeiro e rode o script de novo."
    exit 1
fi

if ! command -v openssl >/dev/null 2>&1; then
    echo "openssl não encontrado. Instale-o (ex.: sudo apt install openssl) e rode este script novamente."
    exit 1
fi

echo "Gerando certificado autoassinado para CN=$CN, válido por $DIAS dias..."
openssl req -x509 -nodes -newkey rsa:2048 \
    -keyout "$KEY" \
    -out "$CRT" \
    -days "$DIAS" \
    -subj "/CN=$CN"

echo "Certificado autoassinado gerado em:"
echo "  $CRT"
echo "  $KEY"
echo "Como é autoassinado, o navegador vai exibir um aviso de segurança -- normal para uso interno/teste."
echo "Para produção com um domínio público, prefira um certificado real (ex.: Let's Encrypt/certbot)"
echo "colocado nesses mesmos dois arquivos, em vez de rodar este script."
