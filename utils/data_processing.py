import pandas as pd
import traceback
from sqlalchemy import create_engine
from config import sqlserver_config
import pandas as pd
import io
import datetime
from azure.storage.blob import BlobClient,generate_blob_sas, BlobSasPermissions
import pandas as pd
from azure.storage.blob import BlobServiceClient
import os
import urllib
from config.sqlserver_config import conecta_sqlserver
import logging
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





def create_delta_table(df, row_tabelaorigem, keyblob, cliente, linkedservice):
    """
    Salva o DataFrame em um arquivo Parquet no Azure Blob Storage.

    :param df: DataFrame contendo os dados processados.
    :param row_tabelaorigem: Linha da tabela tblpbiarquivoimport com informações sobre o arquivo.
    :param keyblob: Chave de acesso ao Blob Storage.
    :param cliente: Nome do cliente.
    :param linkedservice: Nome do serviço vinculado ao Blob Storage.
    """
    try:
        # Configurações do Blob Storage
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={row_tabelaorigem['ContainerName']};AccountKey={keyblob};EndpointSuffix=core.windows.net"
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_name = row_tabelaorigem['ContainerName']
        blob_path = f"{row_tabelaorigem['CaminhoOrigem']}/deltatable/{row_tabelaorigem['TabelaOrigem']}.parquet"

        # Salva o DataFrame em um arquivo Parquet localmente
        local_file_path = f"temp_{row_tabelaorigem['TabelaOrigem']}.parquet"
        df.to_parquet(local_file_path, index=False)

        # Faz o upload do arquivo Parquet para o Blob Storage
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
        with open(local_file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        print(f"Arquivo {blob_path} salvo no Blob Storage com sucesso!")

        # Remove o arquivo local temporário
        os.remove(local_file_path)

    except Exception as e:
        print(f"Erro ao salvar a tabela Delta: {e}")
        raise



def CargaSqlServer(df, row_tabelaorigem):
    """
    Carrega os dados de um DataFrame do Pandas em uma tabela do SQL Server.

    :param df: DataFrame contendo os dados processados.
    :param row_tabelaorigem: Linha da tabela tblpbiarquivoimport com informações sobre o arquivo.
    """
    try:
        # Configurações de conexão com o SQL Server
        engine = conecta_sqlserver()

        # Nome da tabela de destino
        tabela_destino = f"{row_tabelaorigem['TabelaDestino']}diaria"

        # Carrega os dados no SQL Server
        df.to_sql(tabela_destino, con=engine, if_exists='append', index=False)

        logger.info(f"Dados carregados na tabela {tabela_destino} do SQL Server com sucesso!")

    except Exception as e:
        logger.error(f"Erro ao carregar dados no SQL Server: {e}")
        logger.error(traceback.format_exc())
        raise



