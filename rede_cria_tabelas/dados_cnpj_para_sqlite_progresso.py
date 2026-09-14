# -*- coding: utf-8 -*-
"""
Script atualizado para importar arquivos da Receita Federal para SQLite com controle de progresso.

Este script:
- Descompacta os arquivos da Receita.
- Cria uma base SQLite com tabelas e índices.
- Utiliza uma tabela _progresso para controlar o estado de cada etapa.
- Permite reinício seguro em caso de falha, reexecutando apenas as etapas incompletas.
"""

import os, sys, glob, time, zipfile, sqlite3, hashlib, pandas as pd, sqlalchemy
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

# tabelas usadas sempre, mesmo em bases criadas antes destes recursos existirem
conn.execute('''CREATE TABLE IF NOT EXISTS _progresso (
    etapa TEXT PRIMARY KEY,
    status TEXT,
    data_execucao TEXT
)''')
conn.execute('''CREATE TABLE IF NOT EXISTS _config (
    chave TEXT PRIMARY KEY,
    valor TEXT
)''')
conn.commit()

def etapa_concluida(etapa):
    res = cursor.execute("SELECT status FROM _progresso WHERE etapa=?", (etapa,)).fetchone()
    return res and res[0] == 'ok'

def marcar_etapa_concluida(etapa):
    cursor.execute("INSERT OR REPLACE INTO _progresso (etapa, status, data_execucao) VALUES (?, 'ok', datetime('now'))", (etapa,))
    conn.commit()

def get_config(chave, default=None):
    res = cursor.execute("SELECT valor FROM _config WHERE chave=?", (chave,)).fetchone()
    return res[0] if res else default

def set_config(chave, valor):
    cursor.execute("INSERT OR REPLACE INTO _config (chave, valor) VALUES (?, ?)", (chave, valor))
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

# calcula e persiste a data de referência e o anoMes (usados na etapa ajustes_finais),
# pois o .EMPRECSV é apagado depois de carregado
dataReferencia = get_config('dataReferencia', 'xx/xx/2026')
anoMes = get_config('anoMes')
if not anoMes:
    arquivos_emprecsv = glob.glob(os.path.join(pasta_saida, '*.EMPRECSV'))
    if arquivos_emprecsv:
        dataReferenciaAux = arquivos_emprecsv[0].split('.')[2]  # formato DAMMDD
        if len(dataReferenciaAux) == len('D30610') and dataReferenciaAux.startswith('D'):
            dataReferencia = dataReferenciaAux[4:6] + '/' + dataReferenciaAux[2:4] + '/202' + dataReferenciaAux[1]
            anoMes = '202' + dataReferenciaAux.removeprefix('D')[:3]
            set_config('dataReferencia', dataReferencia)
            set_config('anoMes', anoMes)

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

# 4. Ajustes finais: capital social, colunas derivadas, tabela de sócios e índices
# Cada instrução SQL é marcada como uma etapa própria (não o grupo inteiro): um
# CREATE INDEX ou ALTER TABLE ADD COLUMN roda sobre tabelas com dezenas de milhões
# de linhas, exatamente onde um crash é mais provável. Como cada conn.execute() é
# seguido de commit() imediato, a instrução é atômica: se travar no meio, nada fica
# parcialmente aplicado e ela será refeita do zero; se já foi concluída, é pulada
# ao reiniciar o script.
# A marcação da etapa não é atômica com a instrução (são dois commits separados),
# e a chave da etapa é o hash do próprio texto SQL, que muda se este arquivo for
# reformatado no futuro. Em ambos os casos, um reinício pode reexecutar uma
# instrução já aplicada no banco; por isso "already exists"/"duplicate column" são
# tratados como sucesso (idempotência), em vez de travar o script para sempre.
def executa_sql_etapa(sql):
    sql = sql.strip()
    if not sql:
        return
    etapa = 'sql_' + hashlib.md5(sql.encode('utf-8')).hexdigest()[:12]
    if etapa_concluida(etapa):
        log(f"  -> já executado, pulando: {sql.splitlines()[0][:60]}")
        return
    log(f"  -> executando: {sql.splitlines()[0][:60]}")
    try:
        conn.execute(sql)
        conn.commit()
    except sqlite3.OperationalError as e:
        msg = str(e).lower()
        if 'already exists' not in msg and 'duplicate column' not in msg:
            raise
        log(f"  -> já aplicado no banco anteriormente: {e}")
    marcar_etapa_concluida(etapa)

def executa_sqls(grupo, sql_texto):
    log(f"Iniciando grupo: {grupo}")
    for sql in sql_texto.split(';'):
        executa_sql_etapa(sql)

executa_sqls('ajustes_colunas', '''
    ALTER TABLE empresas ADD COLUMN capital_social real;
    update  empresas
    set capital_social = cast( replace(capital_social_str,',', '.') as real);

    ALTER TABLE estabelecimento ADD COLUMN cnpj text;
    Update estabelecimento
    set cnpj = cnpj_basico||cnpj_ordem||cnpj_dv;
''')

executa_sqls('indices_base', '''
    CREATE  INDEX idx_empresas_cnpj_basico ON empresas (cnpj_basico);
    CREATE  INDEX idx_empresas_razao_social ON empresas (razao_social);
    CREATE  INDEX idx_estabelecimento_cnpj_basico ON estabelecimento (cnpj_basico);
    CREATE  INDEX idx_estabelecimento_cnpj ON estabelecimento (cnpj);
    CREATE  INDEX idx_estabelecimento_nomefantasia ON estabelecimento (nome_fantasia);

    CREATE INDEX idx_socios_original_cnpj_basico
    ON socios_original(cnpj_basico);
''')

executa_sqls('cnpj_base2matriz', '''
    create table cnpj_base2matriz as
    SELECT Distinct t.cnpj_basico, te.cnpj as cnpj
    from empresas t
    left join estabelecimento te on te.cnpj_basico = t.cnpj_basico
    where te.matriz_filial='1';

    CREATE INDEX idx_cnpj_base2matriz
    ON cnpj_base2matriz(cnpj_basico);
''')

sqlsSociosAte202607 = '''
    CREATE TABLE socios AS
    SELECT te.cnpj as cnpj, ts.*
    from socios_original ts
    left join cnpj_base2matriz te on te.cnpj_basico = ts.cnpj_basico
    ;
'''

sqlSociosApartir202608 = '''
    -- a partir de ago/2026 está aparecendo apenas o radicial de cnpj do sócio na coluna cnpj_cpf_socio
    -- separando em duas partes, a que tiver cnpj é feito um join para obter o cnpj completo

    CREATE TABLE socios AS
    SELECT te.cnpj as cnpj, ts.*
    from socios_original ts
    left join cnpj_base2matriz te on te.cnpj_basico = ts.cnpj_basico
    where ts.identificador_de_socio<>'1';

    insert into socios
    SELECT te.cnpj as cnpj,
     ts.cnpj_basico,
        ts.identificador_de_socio,
        ts.nome_socio,
        tes.cnpj as cnpj_cpf_socio,
        ts.qualificacao_socio,
        ts.data_entrada_sociedade,
        ts.pais,
        ts.representante_legal,
        ts.nome_representante,
        ts.qualificacao_representante_legal,
        ts.faixa_etaria
    from socios_original ts
    left join cnpj_base2matriz te on te.cnpj_basico = ts.cnpj_basico
    left join cnpj_base2matriz tes on tes.cnpj_basico = ts.cnpj_cpf_socio
    where ts.identificador_de_socio='1';
'''

# se não foi possível determinar o anoMes (base gerada antes deste recurso existir,
# e resumida a partir de um ponto em que o .EMPRECSV já havia sido apagado),
# mantém o comportamento antigo (formato até 202607) em vez de travar com NameError
executa_sqls('socios', sqlSociosApartir202608 if (anoMes and anoMes >= '202608') else sqlsSociosAte202607)

executa_sqls('indices_socios', '''
    ALTER TABLE empresas DROP COLUMN capital_social_str;
    ALTER TABLE estabelecimento DROP COLUMN cnpj_ordem;
    ALTER TABLE estabelecimento DROP COLUMN cnpj_dv;

    DROP TABLE IF EXISTS socios_original;

    CREATE INDEX idx_socios_cnpj ON socios(cnpj);
    CREATE INDEX idx_socios_cnpj_cpf_socio ON socios(cnpj_cpf_socio);
    CREATE INDEX idx_socios_nome_socio ON socios(nome_socio);
    CREATE INDEX idx_socios_representante ON socios(representante_legal);
    CREATE INDEX idx_socios_representante_nome ON socios(nome_representante);

    CREATE INDEX idx_simples_cnpj_basico ON simples(cnpj_basico);
''')

etapa = 'referencia'
if not etapa_concluida(etapa):
    log("Iniciando etapa: referencia")
    conn.execute('CREATE TABLE "_referencia" ("referencia" TEXT, "valor" TEXT);')
    qtde_cnpjs = conn.execute('select count(*) as contagem from estabelecimento;').fetchone()[0]
    conn.execute(f"insert into _referencia (referencia, valor) values ('CNPJ', '{dataReferencia}')")
    conn.execute(f"insert into _referencia (referencia, valor) values ('cnpj_qtde', '{qtde_cnpjs}')")
    conn.commit()
    marcar_etapa_concluida(etapa)
else:
    log("Etapa já concluída: referencia, pulando.")

log('Script finalizado com sucesso.')
