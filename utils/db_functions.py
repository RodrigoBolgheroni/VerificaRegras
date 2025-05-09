import pandas as pd
import sys
sys.path.append('c:/Users/thmalta/Desktop/Faturamento')
from config.mysql_config import conecta_mysql
from config.sqlserver_config import conecta_sqlserver
from sqlalchemy import create_engine,text
import urllib.parse
import traceback
from datetime import timedelta,datetime
import logging
import os

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def Credential(nome_cliente):
    # Estabelecer a conexão com o banco de dados
    conn = conecta_sqlserver()
    
    # Consulta SQL
    query = f"""SELECT * FROM tblcredenciaisfaturamento WHERE ativo = 1 AND cliente = '{nome_cliente}'"""
    
    # Carregar os dados em um DataFrame
    tabela_credenciais = pd.read_sql(query, conn)
    
    # Fechar a conexão
    conn.close()
    
    return tabela_credenciais




def MysqlQuery(server_mysql, user_mysql, password_mysql, bancodedados_mysql,query):
    # Estabelecendo a conexão com o MySQL usando mysql.connector
    conn = conecta_mysql(server_mysql, user_mysql, password_mysql, bancodedados_mysql)
    
    # Carregar os dados da consulta SQL em um DataFrame
    tabela_arquivoimport = pd.read_sql(query, conn)
    
    # Fechar a conexão
    conn.close()
    
    return tabela_arquivoimport




def SqlServerQuery(query):
    # Estabelecendo a conexão com o SQL 
    conn = conecta_sqlserver()
    
    # Carregar os dados da consulta SQL em um DataFrame
    tabela_validacoes = pd.read_sql(query, conn)
    
    # Fechar a conexão
    conn.close()
    
    return tabela_validacoes



def executaquerysql(query, params=None, retorna_resultado=False, commit=False):
    """
    Executa uma query SQL com tratamento de erros e opções de retorno de resultados
    
    Parâmetros:
    - conn: Conexão com o banco de dados
    - query: String com a query SQL a ser executada
    - params: Dicionário com parâmetros para a query (opcional)
    - retorna_resultado: Se True, retorna os resultados da query (para SELECT)
    - commit: Se True, executa commit após a query (para INSERT/UPDATE/DELETE)
    
    Retorno:
    - Se retorna_resultado=True, retorna:
      - Para consultas que retornam uma única linha/valor: o valor escalar
      - Para consultas que retornam múltiplas linhas: lista de dicionários
    - Caso contrário, retorna True em caso de sucesso ou False em caso de erro
    """
    try:
        conn = conecta_sqlserver()
        if params is None:
            params = {}
        
        # Executa a query
        result = conn.execute(text(query), params)
        
        # Se for para retornar resultados
        if retorna_resultado:
            # Se a query parece ser um SELECT (simples verificação)
            if query.strip().upper().startswith('SELECT'):
                rows = result.fetchall()
                # Se não há resultados
                if not rows:
                    return None
                # Se há apenas uma coluna e uma linha, retorna o valor escalar
                if len(rows) == 1 and len(rows[0]) == 1:
                    return rows[0][0]
                # Retorna lista de dicionários para múltiplas linhas/colunas
                return [dict(row) for row in rows]
            else:
                logging.warning("Tentativa de retornar resultado de query que não é SELECT")
                return None
        
        # Se for para fazer commit
        if commit:
            conn.commit()
        
        return True
    
    except Exception as e:
        logging.error(f"Erro ao executar query SQL: {str(e)}")
        logging.error(f"Query: {query}")
        logging.error(f"Parâmetros: {params}")
        conn.rollback()
        return False





def ExecuteScriptSqlServer(row_carga, script):
    # Codificando a senha, caso contenha caracteres especiais
    password_encoded = urllib.parse.quote_plus(row_carga.Password)
    
    # String de conexão usando sqlalchemy para SQL Server
    connection_string = f"mssql+pyodbc://{row_carga.User}:{password_encoded}@{row_carga.Server}/{row_carga.BancoDeDados}?driver=ODBC+Driver+18+for+SQL+Server"
    
    # Criando o engine com a string de conexão
    engine = create_engine(connection_string)
    
    # Usando engine para executar o script
    try:
        with engine.connect() as connection:
            connection.execute(script.replace('\n', ' ').replace('\r', ' '))
        return False
    except Exception as e:
        # Captura e exibe a mensagem de erro
        print(f"Erro SQL: {str(e)}")
        return True




def CargaSqlServer(cliente, row_tabelaorigem, row_carga, linkedservice, tabela_validacoes):
    # Aqui vamos assumir que o SAS token é passado diretamente em linkedservice
    blob_sas_token = linkedservice['sas_token']  # Modifique aqui conforme necessário, se o SAS token vem de outro lugar
    
    # Caminho para o arquivo Delta
    path = f'wasbs://{row_tabelaorigem.ContainerName}@{row_tabelaorigem.ContainerName}.blob.core.windows.net/{row_tabelaorigem.CaminhoOrigem}/deltatable'
    
    # Lê o arquivo Delta (convertido para CSV ou Parquet com pandas)
    df_delta = pd.read_parquet(path)  # Substitua por pd.read_csv(path) se o arquivo for CSV
    
    # Obtendo a lista de colunas obrigatórias a partir da tabela de validações
    row_colunasobt = tabela_validacoes[tabela_validacoes['Validacao'] == 'ColunasObrigatoriasTabela'].iloc[0]
    colunas_obrigatorias = row_colunasobt['Valores'].split(',')
    
    # Valida se as colunas obrigatórias estão presentes no DataFrame
    for coluna in colunas_obrigatorias:
        if coluna not in df_delta.columns:
            raise ValueError(f"A coluna obrigatória '{coluna}' não está presente no arquivo.")
    
    # Converte a coluna DataInsercao para o formato datetime
    df_delta['DataInsercao'] = pd.to_datetime(df_delta['DataInsercao'], errors='coerce')  # Trata valores inválidos
    
    # String de conexão usando SQLAlchemy para SQL Server
    password_encoded = urllib.parse.quote_plus(row_carga.Password)
    connection_string = f"mssql+pyodbc://{row_carga.User}:{password_encoded}@{row_carga.Server}/{row_carga.BancoDeDados}?driver=ODBC+Driver+18+for+SQL+Server"
    engine = create_engine(connection_string)
    
    # Adicionando as colunas de controle
    df_delta['ArquivoFonte'] = row_tabelaorigem.TabelaOrigem
    df_delta['DataInsercao'] = pd.to_datetime('now') - pd.Timedelta(hours=3)  # Ajusta a hora conforme necessário
    
    # Envia os dados para o SQL Server
    try:
        df_delta.to_sql(
            f"{row_carga.TabelaDestinoCarga}diaria", 
            con=engine,
            if_exists='append', 
            index=False,
            method='multi'
        )
        print("Carga concluída com sucesso!")
    except Exception as e:
        print(f"Erro ao carregar dados no SQL Server: {e}")






def atualizar_status_arquivo(server_mysql, user_mysql, password_mysql, bancodedados_mysql, id_arquivo, status, mensagem_erro=None):
    """
    Atualiza o status do arquivo na tabela tblpbiarquivoimport.
    
    :param server_mysql: Servidor MySQL.
    :param user_mysql: Usuário MySQL.
    :param password_mysql: Senha MySQL.
    :param bancodedados_mysql: Banco de dados MySQL.
    :param id_arquivo: ID do arquivo a ser atualizado.
    :param status: Novo status do arquivo.
    :param mensagem_erro: Mensagem de erro (opcional).
    """
    try:
        if mensagem_erro:
            update_query = f"""
                UPDATE tblpbiarquivoimport 
                SET Status = '{status}', MsgErro = '{mensagem_erro}' 
                WHERE Id = {id_arquivo}
            """
        else:
            update_query = f"""
                UPDATE tblpbiarquivoimport 
                SET Status = '{status}', MsgErro = NULL 
                WHERE Id = {id_arquivo}
            """
        # Chama a função para executar a query
        ExecuteScriptMySQL(server_mysql, user_mysql, password_mysql, bancodedados_mysql, update_query)
        
        logger.info(f"Status do arquivo {id_arquivo} atualizado para '{status}'.")
        return True

    except Exception as e:
        logger.error(f"Erro ao atualizar status do arquivo {id_arquivo}: {e}")
        logger.error(traceback.format_exc())
        return False


def ExecuteScriptMySQL(server_mysql, user_mysql, password_mysql, bancodedados_mysql, query): 
    # Codificando a senha, caso contenha caracteres especiais
    password_mysql_encoded = urllib.parse.quote_plus(password_mysql)
    
    # String de conexão usando sqlalchemy
    connection_string = f"mysql+mysqlconnector://{user_mysql}:{password_mysql_encoded}@{server_mysql}/{bancodedados_mysql}"
    engine = create_engine(connection_string)
    
    # Usando conexão para executar a query
    with engine.connect() as connection:
        # Garante que a query é executável
        query = text(query)  # Converte a string para um objeto executável
        connection.execute(query)  # Executa a query diretamente
        connection.commit()  # Certifique-se de que a transação é comitada







def conecta_sqlserver():
    """
    Função para conectar ao SQL Server usando SQLAlchemy.
    """
    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_DATABASE')
    username = os.getenv('DB_USERNAME')
    password = os.getenv('DB_PASSWORD')

    # String de conexão
    connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    engine = create_engine(connection_string)
    return engine.connect()

def atualizarStatusArquivo(id_arquivo,ambiente,container,diretorio,nomearquivo,tamanho,quantidade_registros,quantidade_registros_unicos,status,mensagem_erro, id_tabela_pbi):
    """
    Função para atualizar a tabela de status do arquivo.
    Se o arquivo já existir, faz um UPDATE. Caso contrário, faz um INSERT.
    
    :param id_arquivo: ID do arquivo.
    :param id_tabela_pbi: ID da tabela PBI.
    :param nomearquivo: Nome do arquvio sendo processado.
    :param id_cliente: ID do cliente.
    :param id_tipo_arquivo: ID do tipo de arquivo.
    :param status: Status do processamento.
    :param tamanho: Tamanho do arquivo (opcional).
    :param quantidade_registros: Quantidade de registros no arquivo (opcional).
    :param ambiente: Local em que esta sendo processado.
    :param mensagem_erro: Mensagem de erro (opcional).
    :return: ID do status do arquivo inserido ou atualizado.
    """
    conn = None
    try:
        data_hora = datetime.now() - timedelta(hours=3)
        data_hora_str = data_hora.strftime('%Y-%m-%d %H:%M:%S')
        
        # Conecta ao banco de dados
        conn = conecta_sqlserver()
        
        # Verifica se o arquivo já existe na tabela
        query_verificacao = f"""
            SELECT Id FROM tblstatusarquivo WHERE IdTabelaPBI = {id_tabela_pbi}
        """
        resultado = conn.execute(text(query_verificacao)).fetchone()
        
        if resultado:
            # Se o arquivo já existe, faz um UPDATE
            query = f"""
                UPDATE tblstatusarquivo
                SET 
                    Ambiente = '{ambiente}',
                    Container ='{container}',
                    Diretorio = '{diretorio}',
                    NomeArquivo = '{nomearquivo}',
                    Tamanho = {tamanho if tamanho else 'NULL'},
                    QuantidadeRegistros = {quantidade_registros if quantidade_registros else 'NULL'},
                    QuantidadeRegistrosUnicos = {quantidade_registros_unicos if quantidade_registros_unicos else 'NULL'},
                    MensagemErro = '{mensagem_erro if mensagem_erro else ''}',
                    Status = '{status}',
                    DataInicioProcessamento = '{data_hora_str}',
                    DataAlteracao = '{data_hora_str}'
                WHERE IdTabelaPBI = {id_tabela_pbi}
            """
            conn.execute(text(query))
            conn.commit()
            logger.info(f"Status do arquivo {id_arquivo} atualizado para '{status}'.")
            return resultado[0],True  # Retorna o ID existente
        else:
            # Se o arquivo não existe, faz um INSERT
            query = f"""
                INSERT INTO tblstatusarquivo (
                    IdCliente_TipoArquivo,Ambiente,Container,Diretorio, NomeArquivo,Tamanho, QuantidadeRegistros,QuantidadeRegistrosUnicos,Status, MensagemErro,DataInicioProcessamento, IdTabelaPBI,DataInsercao
                ) VALUES (
                    {id_arquivo},'{ambiente}','{container}','{diretorio}','{nomearquivo}',{tamanho if tamanho else "Null"},{quantidade_registros if quantidade_registros else "Null"},{quantidade_registros_unicos if quantidade_registros_unicos else "Null"}, '{status}','{mensagem_erro if mensagem_erro else " Null"}','{data_hora_str}',{id_tabela_pbi}, 
                     '{data_hora_str}'
                )
            """
            conn.execute(text(query))
            conn.commit()
            logger.info(f"Status do arquivo {id_arquivo} inserido com status '{status}'.")
            
            # Obtém o ID do registro inserido
            resultado = conn.execute(text("SELECT SCOPE_IDENTITY() AS Id;")).fetchone()
            return resultado[0],True  # Retorna o ID
    
    except Exception as e:
        logger.error(f"Erro ao atualizar status do arquivo: {e}")
        logger.error(traceback.format_exc())
        return None,False
    finally:
        if conn:  # Fecha a conexão se ela foi aberta
            conn.close()

def atualizarStatusRegra(id_status_arquivo,id_cliente_tipoarquivo_regra, mensagem_erro):
    """
    Função para atualizar a tabela de status das regras.
    
    :param server_mysql: Servidor MySQL.
    :param user_mysql: Usuário MySQL.
    :param password_mysql: Senha MySQL.
    :param bancodedados_mysql: Banco de dados MySQL.
    :param id_status_arquivo: ID do status do arquivo (relaciona com tblStatusArquivo).
    :param id_regra: ID da regra (relaciona com tblRegra).
    :param status: Status da regra (Passou, Falhou).
    :param mensagem_erro: Mensagem de erro (opcional).
    """
    try:
        data_hora = datetime.now() - timedelta(hours=3)
        data_hora_str = data_hora.strftime('%Y-%m-%d %H:%M:%S')
        conn = conecta_sqlserver()
        
        query = f"""
INSERT INTO tbllogregraerro
           ([IdStatusArquivo],
		   [IdCliente_TipoArquivo_Regra],
		   [MensagemErro],
		   [DataInsercao])
     VALUES
           ({id_status_arquivo},{id_cliente_tipoarquivo_regra},{mensagem_erro},'{data_hora_str}'))
        """
        
        conn.execute(text(query))
        conn.commit()
    
    except Exception as e:
        logger.error(f"Erro ao atualizar status da regra: {e}")
        logger.error(traceback.format_exc())


def selecionaIdLog():
    conn = conecta_sqlserver()        
    resultado = conn.execute(text("SELECT COALESCE(MAX(IdProcessamento),0) AS IdProcessamento  FROM tbllog")).fetchone()
    IdProcessamento = resultado[0] 
    return IdProcessamento  # Retorna o ID



# Função para inserir log
def insereLog(id_processamento,id_cliente_tipoarquivo, mensagem, detalhes, status):
    conn = None
    try:
        # Cria a conexão com o banco
        conn = conecta_sqlserver()
        


        # Pega a data de hoje e a hora atual
        data_hoje = datetime.today().date()  # Pega a data atual (sem a parte da hora)
        hora_atual = datetime.now()  # Pega a data e hora atual

        # Prepara a query de inserção
        query = f"""
            INSERT INTO tbllog
            ([IdProcessamento], [IdCliente_TipoArquivo], [Data], [Hora], [Mensagem], [Detalhes], [Status])
            VALUES
            ('{id_processamento}', '{id_cliente_tipoarquivo}', '{data_hoje}', '{hora_atual}', '{mensagem}', '{detalhes}', '{status}');
        """

        # Executa a query
        conn.execute(text(query))
        conn.commit()

    except Exception as e:
        # Trata erro e faz o log
        logger.error(f"Erro ao inserir log: {e}")
        logger.error(traceback.format_exc())

    finally:
        if conn:
            # Fecha a conexão se ela foi aberta
            conn.close()















