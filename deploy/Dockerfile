# Build context: raiz do repositório (veja deploy/docker-compose.yaml)
FROM python:3.13-slim

WORKDIR /rede-cnpj/rede

RUN apt-get update && apt-get install -y \
    libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY rede/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt gunicorn

# Código da aplicação: incorporado na imagem (a pasta bases/, com as bases sqlite,
# fica de fora via .dockerignore e é montada como volume em tempo de execução --
# veja docker-compose.yaml -- assim gerar uma base nova não exige rebuild).
COPY rede/ .
COPY deploy/wsgi.py .
RUN mkdir -p bases  # garante que a pasta exista mesmo sem o volume montado (docker run avulso)

ENV PYTHONPATH=/rede-cnpj/rede
ENV GUNICORN_CMD_ARGS="--bind=0.0.0.0:8000 --workers=1 --threads=4 --worker-class=gthread"

EXPOSE 8000

CMD ["gunicorn", "wsgi:application"]
