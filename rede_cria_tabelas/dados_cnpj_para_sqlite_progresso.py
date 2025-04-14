# -*- coding: utf-8 -*-
"""
Script atualizado para importar arquivos da Receita Federal para SQLite com controle de progresso.

Este script:
- Descompacta os arquivos da Receita.
- Cria uma base SQLite com tabelas e índices.
- Utiliza uma tabela _progresso para controlar o estado de cada etapa.
- Permite reinício seguro em caso de falha, reexecutando apenas as etapas incompletas.
"""

import os, sys, glob, time, zipfile, sqlite3, pandas as pd, sqlalchemy
import dask.dataframe as dd

pasta_compactados = r"dados-publicos-zip"
pasta_saida = r"dados-publicos"
db_path = os.path.join(pasta_saida, 'cnpj.db')

bApagaDescompactadosAposUso = True

# Função para log com timestamp
def log(msg):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

# Verificação e conexão inicial
if not os.path.exists(pasta_saida):
    os.makedirs(pasta_saida)

db_exists = os.path.exists(db_path)
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
engine_url = f'sqlite:///{db_path}'

if not db_exists:
    log("Criando nova base de dados...")
    conn.execute('''CREATE TABLE IF NOT EXISTS _progresso (
        etapa TEXT PRIMARY KEY,
        status TEXT,
        data_execucao TEXT
    )''')
    conn.commit()

def etapa_concluida(etapa):
    res = cursor.execute("SELECT status FROM _progresso WHERE etapa=?", (etapa,)).fetchone()
    return res and res[0] == 'ok'

def marcar_etapa_concluida(etapa):
    cursor.execute("INSERT OR REPLACE INTO _progresso (etapa, status, data_execucao) VALUES (?, 'ok', datetime('now'))", (etapa,))
    conn.commit()

# 1. Descompactar arquivos zip
etapa = 'descompactar'
if not etapa_concluida(etapa):
    log("Iniciando etapa: descompactar")
    arquivos_zip = list(glob.glob(os.path.join(pasta_compactados, r'*.zip')))
    if len(arquivos_zip) != 37:
        r = input(f'A pasta {pasta_compactados} deveria conter 37 arquivos zip. Encontrado {len(arquivos_zip)}. Prosseguir? (y/n) ')
        if r.lower() != 'y':
            sys.exit(1)
    for arq in arquivos_zip:
        log(f"Descompactando: {arq}")
        with zipfile.ZipFile(arq, 'r') as zip_ref:
            zip_ref.extractall(pasta_saida)
    marcar_etapa_concluida(etapa)
else:
    log("Etapa já concluída: descompactar, pulando.")

# 2. Códigos auxiliares
def carregaTabelaCodigo(ext, nomeTabela):
    etapa = f'codigo_{nomeTabela}'
    if etapa_concluida(etapa):
        log(f"Etapa já concluída: código_{nomeTabela}, pulando.")
        return
    arquivo = glob.glob(os.path.join(pasta_saida, '*' + ext))[0]
    log(f"Iniciando etapa: código_{nomeTabela}")
    log(f"Importando código: {nomeTabela}")
    df = pd.read_csv(arquivo, sep=';', dtype=str, encoding='latin1', header=None, names=['codigo','descricao'])
    df.to_sql(nomeTabela, conn, if_exists='replace', index=None)
    conn.execute(f'CREATE INDEX IF NOT EXISTS idx_{nomeTabela} ON {nomeTabela}(codigo);')
    conn.commit()
    if bApagaDescompactadosAposUso:
        os.remove(arquivo)
    marcar_etapa_concluida(etapa)

carregaTabelaCodigo('.CNAECSV','cnae')
carregaTabelaCodigo('.MOTICSV', 'motivo')
carregaTabelaCodigo('.MUNICCSV', 'municipio')
carregaTabelaCodigo('.NATJUCSV', 'natureza_juridica')
carregaTabelaCodigo('.PAISCSV', 'pais')
carregaTabelaCodigo('.QUALSCSV', 'qualificacao_socio')

# 3. Criação e carga das tabelas grandes
TABELAS = {
    'empresas': {
        'ext': '.EMPRECSV',
        'colunas': [
            'cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel',
            'capital_social_str', 'porte_empresa', 'ente_federativo_responsavel']
    },
    'estabelecimento': {
        'ext': '.ESTABELE',
        'colunas': [
            'cnpj_basico','cnpj_ordem','cnpj_dv','matriz_filial','nome_fantasia','situacao_cadastral',
            'data_situacao_cadastral','motivo_situacao_cadastral','nome_cidade_exterior','pais',
            'data_inicio_atividades','cnae_fiscal','cnae_fiscal_secundaria','tipo_logradouro','logradouro',
            'numero','complemento','bairro','cep','uf','municipio','ddd1','telefone1','ddd2','telefone2',
            'ddd_fax','fax','correio_eletronico','situacao_especial','data_situacao_especial']
    },
    'socios_original': {
        'ext': '.SOCIOCSV',
        'colunas': [
            'cnpj_basico','identificador_de_socio','nome_socio','cnpj_cpf_socio','qualificacao_socio',
            'data_entrada_sociedade','pais','representante_legal','nome_representante',
            'qualificacao_representante_legal','faixa_etaria']
    },
    'simples': {
        'ext': '.SIMPLES.CSV.*',
        'colunas': [
            'cnpj_basico','opcao_simples','data_opcao_simples','data_exclusao_simples',
            'opcao_mei','data_opcao_mei','data_exclusao_mei']
    }
}

for tabela, meta in TABELAS.items():
    etapa = f'carga_{tabela}'
    if etapa_concluida(etapa):
        log(f"Etapa já concluída: carga_{tabela}, pulando.")
        continue
    conn.execute(f'DROP TABLE IF EXISTS {tabela}')
    conn.commit()
    sql_cols = ', '.join([f'{c} TEXT' for c in meta['colunas']])
    conn.execute(f'CREATE TABLE {tabela} ({sql_cols})')
    arquivos = glob.glob(os.path.join(pasta_saida, '*' + meta['ext']))
    for arq in arquivos:
        log(f"Importando {arq} para {tabela}")
        ddf = dd.read_csv(arq, sep=';', header=None, names=meta['colunas'], encoding='latin1', dtype=str, na_filter=None)
        ddf.to_sql(tabela, engine_url, index=None, if_exists='append', dtype=sqlalchemy.sql.sqltypes.TEXT)
        if bApagaDescompactadosAposUso:
            os.remove(arq)
    marcar_etapa_concluida(etapa)

log('Script finalizado com sucesso.')
