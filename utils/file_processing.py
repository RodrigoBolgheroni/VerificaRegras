import pandas as pd
import traceback
import logging
from utils.db_functions import SqlServerQuery
from azure.storage.blob import BlobServiceClient
import io
from utils.file_utils import ValidacaoExtensaoArquivo,ValidacaoVazio,ValidacaoEncodingArquivo,ValidacaoHeader

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def carregar_arquivo_csv_blob(caminho_csv, keyblob, container_name,encoding='Windows-1252', sep=';', tem_cabecalho=True):
    """
    Carrega um arquivo CSV do Azure Blob Storage em um DataFrame do Pandas.
    
    :param caminho_csv: Caminho do arquivo dentro do container.
    :param keyblob: Chave de acesso ao Azure Blob Storage.
    :param container_name: Nome do container no Azure Blob Storage.
    :param encoding: Encoding do arquivo.
    :param sep: Separador de colunas.
    :param tem_cabecalho: Indica se o arquivo tem cabeçalho.
    :return: DataFrame carregado.
    """
    try:
        if not keyblob or not container_name:
            raise ValueError("Chave de acesso (keyblob) e nome do container (container_name) são obrigatórios.")

        # Configuração do Blob Storage
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={container_name};AccountKey={keyblob};EndpointSuffix=core.windows.net"
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=caminho_csv)

        # Faz o download do arquivo
        download_stream = blob_client.download_blob()
        file_content = download_stream.readall()

        # Carrega o conteúdo em um DataFrame
        df = pd.read_csv(io.StringIO(file_content.decode(encoding)), sep=sep, header=0 if tem_cabecalho else None)

        
        logger.info("Arquivo CSV carregado do Azure Blob Storage com sucesso!")
        logger.info("DataFrame inicial (após leitura do arquivo):")
        logger.info(df.head())  # Exibe as primeiras linhas do DataFrame
        
        return df
    except Exception as e:
        logger.error(f"Erro ao carregar o arquivo CSV do Blob Storage: {e}")
        logger.error(traceback.format_exc())
        raise

def consultar_regras(id_cliente_tipo_arquivo):
    """
    Consulta as regras na tabela tblcliente_tipoarquivo_regra com base no IdCliente_TipoArquivo.
    
    :param id_cliente_tipo_arquivo: ID do cliente e tipo de arquivo.
    :return: DataFrame com as regras.
    """
    try:
        query_regras = f"""
                        SELECT ctr.[IdCliente_Tipoarquivo_Regra] AS Id,
                   ctr.[DescricaoCampo],
                   r.Regra AS RegraNome,
				   t.TipoValidacao As TipoDeDado,
				   ctr.Formato,
                   ctr.[IdTipoDeDado],
                   ctr.[Obrigatorio],
                   ctr.[IdCliente_TipoArquivo],
                   ctr.[IdRegra]
            FROM tblcliente_tipoarquivo_regra ctr
            INNER JOIN tblregra r ON ctr.IdRegra = r.IdRegra 
			INNER JOIN tbltipodedados t ON ctr.IdTipoDeDado = t.IdTipoDeDados
            WHERE IdCliente_TipoArquivo = {id_cliente_tipo_arquivo} AND r.Ativo = 1
        """
        regras = SqlServerQuery(query_regras)
        logger.info(f"Regras carregadas com sucesso para IdCliente_TipoArquivo = {id_cliente_tipo_arquivo}.")
        return regras
    except Exception as e:
        logger.error(f"Erro ao consultar regras: {e}")
        logger.error(traceback.format_exc())
        raise

def processar_arquivo(cliente, tipoarquivo):
    """
    Processa o arquivo CSV, aplicando validações e regras.
    
    :param cliente: Nome do cliente.
    :param tipoarquivo: Tipo de arquivo.
    :param caminho_csv: Caminho do arquivo CSV no Blob Storage.
    :param keyblob: Chave de acesso ao Azure Blob Storage.
    :param container_name: Nome do container no Azure Blob Storage.
    :return: Tuple (df, regras, row_tabelacliente_tipoarquivo).
    """
    try:
        # 1. Consultar as configurações do cliente e tipo de arquivo
        query_cliente_tipoarquivo = f"""
            SELECT cta.[IdCliente_TipoArquivo], cta.[IdCliente], cta.[IdTipoArquivo],
                   cta.[IdExtensaoArquivo], cta.[Encoding], cta.[IsHeader], cta.[Header],
                   cta.[Chave], c.Cliente AS ClienteNome, ta.TipoArquivo AS TipoArquivoNome,
                   ea.ExtensaoArquivo AS ExtensaoNome
            FROM tblcliente_tipoarquivo cta 
            INNER JOIN tblcliente c ON cta.IdCliente = c.IdCliente
            INNER JOIN tbltipoarquivo ta ON cta.IdTipoArquivo = ta.IdTipoArquivo
            INNER JOIN tblextensaoarquivo ea ON cta.IdExtensaoArquivo = ea.IdExtensaoArquivo 
            WHERE c.Cliente = '{cliente}' AND ta.TipoArquivo = '{tipoarquivo}' AND cta.Ativo = 1
        """
        tblcliente_tipoarquivo = SqlServerQuery(query_cliente_tipoarquivo)

        # Verifica se há apenas uma linha
        if len(tblcliente_tipoarquivo) != 1:
            raise ValueError(f"Erro: Esperava-se 1 linha na tblcliente_tipoarquivo, mas foram encontradas {len(tblcliente_tipoarquivo)}.")

        # Garante que row_tabelacliente_tipoarquivo seja uma Series
        row_tabelacliente_tipoarquivo = tblcliente_tipoarquivo.iloc[0]

        

        logger.info("Processamento do arquivo concluído com sucesso.")
        return row_tabelacliente_tipoarquivo

    except Exception as e:
        logger.error(f"Erro ao processar arquivo: {e}")
        logger.error(traceback.format_exc())
        raise

def processar_regras(row_tabelacliente_tipoarquivo):
    try:
        # Consultar as regras
        regras = consultar_regras(row_tabelacliente_tipoarquivo['IdCliente_TipoArquivo'])
        return regras, True
    except Exception:
        return None, False


def processar_df(row_tabelacliente_tipoarquivo, caminho_csv, keyblob, container_name, dic):
    """
    Processa o arquivo CSV do Blob Storage após validações iniciais.
    
    :param row_tabelacliente_tipoarquivo: Linha da tabela tblcliente_tipoarquivo.
    :param caminho_csv: Caminho do arquivo CSV.
    :param keyblob: Chave de acesso ao Azure Blob Storage.
    :param container_name: Nome do container no Azure Blob Storage.
    :param dic: Dicionário para armazenar informações de erro.
    :return: Tuple (df, flag_processa, flags, dic).
    """

    flags = {
        'flag_extensao': None,
        'flag_vazio': None,
        'flag_encoding': None,
        'flag_validacao_header': True  
    }

    # Validação da extensão
    extensao_esperada = row_tabelacliente_tipoarquivo['ExtensaoNome']
    dic, flags['flag_extensao'] = ValidacaoExtensaoArquivo(caminho_csv, extensao_esperada, dic)

    # Validação de arquivo vazio
    dic, flags['flag_vazio'] = ValidacaoVazio(caminho_csv, keyblob, container_name, dic)

    # Validação do encoding
    encoding_esperado = row_tabelacliente_tipoarquivo['Encoding']
    dic, flags['flag_encoding'], encode_arquivo = ValidacaoEncodingArquivo(caminho_csv, keyblob, container_name, encoding_esperado, dic)

    # Verifica se todas as flags iniciais são verdadeiras
    if all(flags.values()):
        try:
            # Carrega o arquivo CSV
            tem_cabecalho = row_tabelacliente_tipoarquivo['IsHeader'] == 1
            df = carregar_arquivo_csv_blob(caminho_csv, keyblob, container_name, encoding=encode_arquivo, sep=";", tem_cabecalho=tem_cabecalho)

            # Validação do cabeçalho
            df, dic, flags['flag_validacao_header'] = ValidacaoHeader(df, dic, row_tabelacliente_tipoarquivo)

            # Verifica se a validação do cabeçalho foi bem-sucedida
            if not flags['flag_validacao_header']:
                return None, False, flags, dic  # Retorna None, flag_processa=False, flags e dic

            return df, True, flags, dic  # Retorna df, flag_processa=True, flags e dic
        except Exception as e:
            dic['erro_carregamento'] = str(e)
            return None, False, flags, dic  # Retorna None, flag_processa=False, flags e dic
    else:
        return None, False, flags, dic  # Retorna None, flag_processa=False, flags e dic

