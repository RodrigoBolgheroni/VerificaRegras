import pandas as pd
import traceback
from sqlalchemy import create_engine, text
from config import sqlserver_config
import io
import datetime
from azure.storage.blob import BlobClient, generate_blob_sas, BlobSasPermissions
from azure.storage.blob import BlobServiceClient
import os
import urllib
from config.sqlserver_config import conecta_sqlserver
import logging
import subprocess
import numpy as np
import hashlib

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_service_sas_blob(keyblob, row_tabelaorigem, tipo, dataatual):
    # Definindo o horário de início e de expiração para o SAS Token
    start_time = datetime.datetime.now(datetime.timezone.utc)
    expiry_time = start_time + datetime.timedelta(days=7)

    # String de conexão com o Azure Blob Storage
    connection_string = f"DefaultEndpointsProtocol=https;AccountName={row_tabelaorigem['ContainerName']};AccountKey={keyblob};EndpointSuffix=core.windows.net"

    # Criando o cliente do Blob
    blob_client = BlobClient.from_connection_string(connection_string, container_name=row_tabelaorigem['ContainerName'],
                                                    blob_name=f'{row_tabelaorigem["CaminhoOrigem"]}/ValidacaoOutputs/Validacao{tipo}_{dataatual}.xlsx')

    # Gerando o SAS Token com permissões de leitura
    sas_token = generate_blob_sas(
        account_name=blob_client.account_name,
        container_name=blob_client.container_name,
        blob_name=blob_client.blob_name,
        account_key=keyblob,
        permission=BlobSasPermissions(read=True),
        expiry=expiry_time,
        start=start_time
    )

    # Imprimindo a URL com o SAS Token para depuração
    print(f"{blob_client.url}?{sas_token}")

    # Retornando a URL com SAS Token
    return f"{blob_client.url}?{sas_token}"


def UploadBlob(dic, dataframe, row_tabelaorigem, row_credenciais, keyblob, tipo):
    # Gerando a data e hora atual formatada
    dataatual = str(datetime.datetime.now())[:19].replace('-', '').replace(' ', '_').replace(':', '')

    # Conectando com o Azure Blob Storage usando a connection string
    connection_string = f"DefaultEndpointsProtocol=https;AccountName={row_tabelaorigem['ContainerName']};AccountKey={keyblob};EndpointSuffix=core.windows.net"

    # Convertendo o DataFrame pandas para BytesIO e escrevendo no formato Excel
    writer = io.BytesIO()
    dataframe.to_excel(writer, index=False)

    # Upload do arquivo para o Blob
    blob_name = f'{row_tabelaorigem["CaminhoOrigem"]}/ValidacaoOutputs/Validacao{tipo}_{dataatual}.xlsx'
    BlobClient.from_connection_string(connection_string, container_name=row_tabelaorigem['ContainerName'],
                                      blob_name=blob_name).upload_blob(writer.getvalue(), blob_type="BlockBlob", connection_timeout=1000)

    # Atualizando o dicionário com o link do artefato
    sas_url = create_service_sas_blob(keyblob, row_tabelaorigem, tipo, dataatual)
    if 'Artefatos: \n' in dic:
        dic['Artefatos: \n'].append(f'{tipo}: \n{sas_url}')
    else:
        dic['Artefatos: \n'] = [f'{tipo}: \n{sas_url}']

    return dic


def VariaveisPipeline():
    # Cria variáveis úteis durante o pipeline
    dic = {}

    # Definindo os dados e as colunas do DataFrame analítico
    dados_analitico = {
        "Coluna": [],
        "Linha": [],
        "Valor": [],
        "Critica": []
    }

    # Definindo os dados e as colunas do DataFrame agrupado
    dados_agrupado = {
        "Coluna": [],
        "Critica": [],
        "Valor": [],
        "Linhas com problemas (exemplo)": []
    }

    # Criando os DataFrames usando pandas
    analiticofinal = pd.DataFrame(dados_analitico)
    agrupadofinal = pd.DataFrame(dados_agrupado)

    return dic, analiticofinal, agrupadofinal


def CargaSqlServer(df, row_tabelaorigem, regras, nome_arquivo_origem,tabela_destino, sample_rows=2):
    """Carrega dados convertendo tudo para string (VARCHAR compatível)"""
    try:
        tabela_temp = "#tbltemp_"
        tabela_destino = f"tbl{tabela_destino}"
        
        # Identificar colunas chave (ajuste conforme suas regras)
        key_columns = [col.strip() for col in row_tabelaorigem['Chave'].split(',')] if 'Chave' in row_tabelaorigem else []

        # Use a conexão diretamente do contexto do gerenciador
        with conecta_sqlserver() as conn:
            # 1. Cria a tabela temporária
            create_temp_table_with_correct_types(conn, tabela_temp, regras, row_tabelaorigem)
            
            # 2. Substitui 'nan' por NULL
            df = df.replace({'nan': None})
            
            # 3. Insere os dados
            insert_data_to_sql_server(
                dataframe=df,
                table=tabela_temp,
                server_connection=conn,
                chunk_size=10000
            )
            
            # 4. Verifica a inserção com um SELECT de amostra
            if sample_rows > 0:
                print(f"\nVerificando amostra de {sample_rows} linhas inseridas:")
                sample_query = f"SELECT TOP {sample_rows} * FROM {tabela_temp}"
                
                # Executa a query e obtém os resultados
                result = conn.execute(text(sample_query))
                
                # Obtém os nomes das colunas
                columns = result.keys()
                
                # Imprime cabeçalho com nomes das colunas
                print("\n| " + " | ".join(columns) + " |")
                print("-" * (sum(len(col) for col in columns) + 3 * len(columns) + 1))
                
                # Imprime os dados formatados
                for row in result:
                    print("| " + " | ".join(str(value) if value is not None else "NULL" for value in row) + " |")
                
                # Verifica o total de linhas inseridas
                count_query = f"SELECT COUNT(*) AS total FROM {tabela_temp}"
                total = conn.execute(text(count_query)).scalar()
                print(f"\nTotal de linhas na tabela temporária: {total}")
            
            # 5. Executa o UPSERT para a tabela destino
            if key_columns:
                istrue,count_after = upsert_from_temp_to_destination(
                    conn=conn,
                    temp_table=tabela_temp,
                    destination_table=tabela_destino,
                    key_columns=key_columns,
                    nome_arquivo_origem=nome_arquivo_origem
                )
            else:
                print("Aviso: Nenhuma coluna chave definida - pulando upsert")
            return count_after
            
    except Exception as e:
        print(f"Erro no processo de carga: {e}")
        raise


def create_temp_table_with_correct_types(conn, tabela_temp, regras, row_tabelacliente_tipoarquivo):
    """
    Versão simplificada e robusta para criação de tabelas temporárias
    """
    try:
        # 1. Drop seguro da tabela existente
        drop_query = f"""
        BEGIN TRY
            IF OBJECT_ID('tempdb..{tabela_temp}') IS NOT NULL
            BEGIN
                DROP TABLE {tabela_temp};
                SELECT 1 AS drop_success, 'Tabela existente removida' AS message;
            END
            ELSE
                SELECT 1 AS drop_success, 'Nenhuma tabela existente' AS message;
        END TRY
        BEGIN CATCH
            SELECT 0 AS drop_success, ERROR_MESSAGE() AS message;
        END CATCH
        """
        
        drop_result = conn.execute(text(drop_query)).fetchone()
        if not drop_result or not drop_result.drop_success:
            raise RuntimeError(f"Falha ao limpar tabela existente: {getattr(drop_result, 'message', 'Erro desconhecido')}")

        # 2. Mapeamento de tipos de dados
        tipo_dado_map = {
            'inteiro': 'INT',
            'decimal': 'FLOAT',
            'texto': 'VARCHAR(255)',
            'data': 'DATE',
            'booleano': 'BIT',
            'dataehora': 'DATETIME2'
        }

        # 3. Construção das colunas dinâmicas
        colunas_sql = []
        nomes_colunas = [col.strip() for col in row_tabelacliente_tipoarquivo['Header'].split(',')]
        
        for nome_coluna in nomes_colunas:
            try:
                regra = regras[regras['DescricaoCampo'] == nome_coluna].iloc[0]
                tipo_sql = tipo_dado_map.get(regra['TipoDeDado'].lower(), 'VARCHAR(255)')
                colunas_sql.append(f"[{nome_coluna}] {tipo_sql}")
            except Exception as e:
                raise ValueError(f"Erro ao mapear coluna {nome_coluna}: {str(e)}")

        # 4. Query de criação simplificada
        create_query = f"""
        CREATE TABLE {tabela_temp} (
            {',\n            '.join(colunas_sql)},
            [ChaveMD5] VARCHAR(32),
            [DataInsercao] DATETIME2 DEFAULT SYSDATETIME(),
            [ArquivoFonte] VARCHAR(255)
        )
        """
        
        conn.execute(text(create_query))
        conn.commit()

        # 5. Verificação direta
        verification_query = f"""
        SELECT 
            CASE WHEN OBJECT_ID('tempdb..{tabela_temp}') IS NOT NULL THEN 1 ELSE 0 END AS table_exists,
            (SELECT COUNT(*) FROM tempdb.sys.columns WHERE object_id = OBJECT_ID('tempdb..{tabela_temp}')) AS column_count
        """
        
        verification = conn.execute(text(verification_query)).fetchone()
        
        if not verification or not verification.table_exists:
            raise RuntimeError("Falha crítica: Tabela não foi criada")
        
        
        print(f"Tabela temporária criada com sucesso: {tabela_temp} ({verification.column_count} colunas)")
        return True

    except Exception as e:
        conn.rollback()
        print(f"[ERRO] Falha na criação da tabela temporária: {str(e)}")
        
        # Verificação de erros comuns
        if "There is already an object named" in str(e):
            print("Solução: Tentar novamente com um nome de tabela único ou verificar conexões pendentes")
        elif "permission" in str(e).lower():
            print("Solução: Verificar permissões do usuário no tempdb")
        
        raise


def insert_data_to_sql_server(dataframe, table, server_connection, chunk_size=10000):
    """
    Insere dados no SQL Server com tratamento robusto de conversão de tipos.
    """
    try:
        cursor = server_connection.connection.cursor()
        cursor.fast_executemany = True

        # Obter tipos de dados da tabela de destino
        cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = '{table.replace('#', '')}'
        """)
        column_types = {row[0]: row[1] for row in cursor.fetchall()}

        # Preparar query
        cols = ",".join([f"[{col}]" for col in dataframe.columns])
        placeholders = ",".join(["?"] * len(dataframe.columns))
        insert_stmt = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"

        # Função de conversão segura
        def convert_value(value, col_type):
            if pd.isna(value):
                return None
            try:
                if 'numeric' in col_type.lower() or 'float' in col_type.lower():
                    return float(value) if str(value).strip() else None
                elif 'int' in col_type.lower():
                    return int(float(value)) if str(value).strip() else None
                return str(value) if value is not None else None
            except:
                return None  # Falha na conversão

        # Processar em chunks
        total = 0
        for i in range(0, len(dataframe), chunk_size):
            chunk = dataframe.iloc[i:i + chunk_size].copy()
            
            # Aplicar conversão segura para cada coluna
            for col in chunk.columns:
                sql_type = column_types.get(col, 'varchar')
                chunk[col] = chunk[col].apply(lambda x: convert_value(x, sql_type))
            
            # Converter para tuplas
            values = [tuple(x) for x in chunk.to_numpy()]
            
            try:
                cursor.executemany(insert_stmt, values)
                total += len(chunk)
                print(f"Inseridos {len(chunk)} registros (Total: {total})")
            except Exception as chunk_error:
                print(f"Erro no chunk {i}-{i+chunk_size}: {chunk_error}")
                # Logar valores problemáticos
                for idx, row in enumerate(chunk.to_dict('records')):
                    try:
                        cursor.execute(insert_stmt, tuple(row.values()))
                    except Exception as row_error:
                        print(f"Erro na linha {i+idx}: {row_error}")
                        print(f"Valores problemáticos: {row}")
                raise

        cursor.commit()
        return total

    except Exception as e:
        print(f"Erro geral na inserção: {e}")
        if 'cursor' in locals():
            cursor.rollback()
            cursor.close()
        raise

def prepare_temp_table(conn, table_name, key_columns, nome_arquivo):
    """Prepara a tabela temporária com MD5 e remove duplicatas"""
    try:
        # 1. Garantir que a coluna ChaveMD5 existe
        check_column_query = f"""
        IF NOT EXISTS (SELECT * FROM tempdb.sys.columns 
                      WHERE object_id = OBJECT_ID('tempdb..{table_name}') 
                      AND name = 'ChaveMD5')
        BEGIN
            ALTER TABLE {table_name} ADD ChaveMD5 VARBINARY(16) NULL
        END
        """
        conn.execute(text(check_column_query))
        
        # 2. Gerar MD5 para todas as linhas
        concat_parts = [f"ISNULL(CAST([{col}] AS NVARCHAR(MAX)), '')" for col in key_columns]
        concat_expression = ", '|', ".join(concat_parts)

        update_md5_query = f"""
        UPDATE {table_name} 
        SET ChaveMD5 = CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT(
            {concat_expression}
        )), 2)
        WHERE ChaveMD5 IS NULL
        """
        conn.execute(text(update_md5_query))
        
        # Contar linhas antes de remover duplicatas
        count_before = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
        print(f"Total de linhas antes de remover duplicatas: {count_before}")
        
        # 3. Remover duplicatas, mantendo apenas a última linha (por DataInsercao ou outra coluna temporal)
        remove_duplicates_query = f"""
        WITH CTE AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY ChaveMD5 ORDER BY DataInsercao DESC) AS rn
            FROM {table_name}
        )
        DELETE FROM CTE WHERE rn > 1
        """
        conn.execute(text(remove_duplicates_query))
        
        # Contar linhas depois de remover duplicatas
        count_after = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
        print(f"Total de linhas depois de remover duplicatas: {count_after}")
        print(f"Total de duplicatas removidas: {count_before - count_after}")
            
        # 4. Atualizar metadados
        conn.execute(text(f"""
            UPDATE {table_name}
            SET 
                DataInsercao = GETDATE(),
                ArquivoFonte = :nome_arquivo
            WHERE DataInsercao IS NULL
        """), {'nome_arquivo': nome_arquivo})
        
        conn.commit()
        return True,count_after

    except Exception as e:
        conn.rollback()
        print(f"Erro ao preparar tabela temporária: {str(e)}")
        raise

def datacampo(conn, nome_campo_data):
    """
    Pega datas distintas de um campo específico na tblfaturamento,
    compara com NomeData da tblcliente_tipoarquivo e
    insere datas novas na tbldataarquivo
    
    Parâmetros:
    - conn: Conexão com o banco de dados
    - nome_campo_data: Nome do campo de data na tblfaturamento a ser analisado
    
    Retorno:
    - Número de novas datas inseridas ou -1 em caso de erro
    """
    try:
        # 1. Verificar se o campo existe na tblfaturamento
        query_verifica_campo = f"""
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'tblfaturamento' 
        AND COLUMN_NAME = :nome_campo
        """
        campo_existe = conn.execute(text(query_verifica_campo), 
                                {'nome_campo': nome_campo_data}).scalar()
        
        if not campo_existe:
            print(f"Campo '{nome_campo_data}' não encontrado na tblfaturamento")
            return 0

        # 2. Pegar datas distintas do campo especificado
        query_datas_distintas = f"""
        SELECT DISTINCT CAST({nome_campo_data} AS DATE) 
        FROM tblfaturamento 
        WHERE {nome_campo_data} IS NOT NULL
        """
        datas_faturamento = [row[0] for row in conn.execute(text(query_datas_distintas)).fetchall()]

        if not datas_faturamento:
            print("Nenhuma data encontrada no campo especificado")
            return 0

        # 3. Verificar quais datas já existem na tbldataarquivo
        query_datas_existentes = """
        SELECT DISTINCT CAST(DataArquivo AS DATE) 
        FROM tbldataarquivo
        """
        datas_arquivadas = [row[0] for row in conn.execute(text(query_datas_existentes)).fetchall()]

        # 4. Filtrar apenas as novas datas
        novas_datas = [data for data in datas_faturamento if data not in datas_arquivadas]

        if not novas_datas:
            print("Todas as datas já estão arquivadas")
            return 0

        # 5. Inserir novas datas
        insercoes = 0
        for data in novas_datas:
            try:
                query_insere = """
                INSERT INTO tbldataarquivo (NomeDoCampo,Data, DataInsercao)
                VALUES (:data, GETDATE(), :nome_campo)
                """
                conn.execute(text(query_insere), 
                           {'data': data, 'nome_campo': nome_campo_data})
                insercoes += 1
            except Exception as e:
                print(f"Erro ao inserir data {data}: {str(e)}")
                continue

        conn.commit()
        print(f"Inseridas {insercoes} novas datas na tbldataarquivo")
        return insercoes

    except Exception as e:
        conn.rollback()
        print(f"Erro na função datacampo: {str(e)}")
        return -1



def upsert_from_temp_to_destination(conn, temp_table, destination_table, key_columns, nome_arquivo_origem):

    """
    Realiza o upsert (insert/update) da tabela temporária para a tabela destino.
    """
    try:
        # 1. Prepara a tabela temporária
        istrue,count_after = prepare_temp_table(conn, temp_table, key_columns, nome_arquivo_origem)

        # 2. Obter colunas da tabela temporária
        sample_query = f"SELECT TOP 1 * FROM {temp_table}"
        result = conn.execute(text(sample_query))
        row = result.fetchone()
        
        if not row:
            result.close()
            raise ValueError("Nenhuma linha encontrada na tabela temporária")
        
        # Inclui ChaveMD5 na lista de colunas para o MERGE
        data_columns = [column[0] for column in result.cursor.description
                      if column[0] not in ('DataInsercao', 'ArquivoFonte')]
        result.close()

        # 3. Construir a query MERGE incluindo ChaveMD5
        cols_for_merge = ", ".join([f"[{col}]" for col in data_columns])
        source_cols = ", ".join([f"SOURCE.[{col}]" for col in data_columns])
        update_set = ",\n                ".join([f"TARGET.[{col}] = SOURCE.[{col}]" for col in data_columns])
        join_conditions = " AND ".join([f"TARGET.[{col}] = SOURCE.[{col}]" for col in key_columns])
        
        merge_query = f"""
        MERGE INTO {destination_table} AS TARGET
        USING {temp_table} AS SOURCE
        ON {join_conditions}
        
        WHEN MATCHED THEN
            UPDATE SET
                {update_set},
                TARGET.DataInsercao = GETDATE()
                
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ({cols_for_merge}, DataInsercao, ArquivoFonte)
            VALUES ({source_cols}, GETDATE(), '{nome_arquivo_origem}')
            
        WHEN NOT MATCHED BY SOURCE THEN
            DELETE;
        """

        # 4. Executar o MERGE
        conn.execute(text(merge_query))
        conn.commit()
        
        return True,count_after

    except Exception as e:
        conn.rollback()
        print(f"Erro durante o upsert: {str(e)}")
        raise