import chardet 
from charset_normalizer import from_bytes
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from functools import reduce
from datetime import timedelta
from azure.storage.blob import BlobServiceClient, BlobClient, BlobSasPermissions, generate_blob_sas
import pandas as pd  
from functools import reduce
import os
import logging
# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def LeArquivo(dic, row_tabelaorigem, row_header, tabela_validacoes, encode_recebido, cliente, keyblob, linkedservice):
    """
    Carrega um arquivo do Azure Blob Storage e aplica validações iniciais.

    :param dic: Dicionário para armazenar logs.
    :param row_tabelaorigem: Linha da tabela tblpbiarquivoimport com informações sobre o arquivo.
    :param row_header: Linha da tabela de validações com regras para o cabeçalho.
    :param tabela_validacoes: DataFrame com as regras de validação.
    :param encode_recebido: Encoding detectado do arquivo.
    :param cliente: Nome do cliente.
    :param keyblob: Chave de acesso ao Azure Blob Storage.
    :param linkedservice: Nome do serviço vinculado ao Azure Blob Storage.
    :return: DataFrame com os dados do arquivo e dicionário atualizado.
    """
    try:
        # Configurações do Blob Storage
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={row_tabelaorigem['ContainerName']};AccountKey={keyblob};EndpointSuffix=core.windows.net"
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_name = row_tabelaorigem['ContainerName']
        blob_path = f"{row_tabelaorigem['CaminhoOrigem']}/{row_tabelaorigem['TabelaOrigem']}"

        # Faz o download do arquivo
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
        download_stream = blob_client.download_blob()
        file_content = download_stream.readall()

        # Carrega o arquivo em um DataFrame
        if row_tabelaorigem['TabelaOrigem'].endswith('.csv'):
            df = pd.read_csv(pd.compat.StringIO(file_content.decode(encode_recebido)), sep=row_tabelaorigem['Separador'])
        elif row_tabelaorigem['TabelaOrigem'].endswith('.parquet'):
            df = pd.read_parquet(pd.compat.BytesIO(file_content))
        else:
            raise ValueError("Formato de arquivo não suportado.")

        # Valida o cabeçalho
        colunas_esperadas = row_header['Valores'].split(',')
        if not all(coluna in df.columns for coluna in colunas_esperadas):
            raise ValueError("Cabeçalho do arquivo não corresponde ao esperado.")

        # Atualiza o dicionário com logs
        dic['ArquivoCarregado'] = True
        dic['CaminhoArquivo'] = blob_path

        return df, dic

    except Exception as e:
        dic['ErroLeituraArquivo'] = str(e)
        raise

def ValidacaoExtensaoArquivo(caminho_csv, extensao_esperada, dic):
    """
    Valida o formato do arquivo (extensão e se o arquivo existe).
    
    :param caminho_csv: Caminho do arquivo CSV.
    :param extensao_esperada: Extensão esperada do arquivo (ex: '.csv').
    :param dic: Dicionário para armazenar logs e resultados.
    :return: Tuple (dic, flag_formato_valido).
    """
    try:
        # Verifica a extensão do arquivo
        extensao_arquivo = os.path.splitext(caminho_csv)[1].lower()
        if extensao_arquivo != extensao_esperada.lower():
            dic['Erro_Formato'] = [f"Formato de arquivo incorreto. Esperado: {extensao_esperada}, Recebido: {extensao_arquivo}", '']
            return dic, False

        # Se todas as validações passarem
        dic['Reporte'].append("Validação de formato do arquivo concluída com sucesso.")
        return dic, True

    except Exception as e:
        dic['Erro_Formato'] = [f"Erro ao validar formato do arquivo: {e}", '']
        return dic, False

def ValidacaoVazio(caminho_csv, keyblob, container_name, dic):
    """
    Valida se o arquivo no Blob Storage está vazio.
    
    :param caminho_csv: Caminho do arquivo CSV.
    :param keyblob: Chave de acesso ao Azure Blob Storage.
    :param container_name: Nome do container no Azure Blob Storage.
    :param dic: Dicionário para armazenar logs e resultados.
    :return: Tuple (dic, flag_nao_vazio).
    """
    try:
        # Configuração do Blob Storage
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={container_name};AccountKey={keyblob};EndpointSuffix=core.windows.net"
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=caminho_csv)

        # Obtém as propriedades do blob para verificar o tamanho
        blob_properties = blob_client.get_blob_properties()
        tamanho_arquivo = blob_properties.size

        if tamanho_arquivo == 0:
            dic['Erro_Vazio'] = ["Arquivo vazio: nenhum registro encontrado.", '']
            return dic, False

        # Se o arquivo não estiver vazio
        dic['Reporte'].append("Validação de arquivo vazio concluída com sucesso.")
        return dic, True

    except Exception as e:
        dic['Erro_Vazio'] = [f"Erro ao validar se o arquivo está vazio: {e}", '']
        return dic, False

def ValidacaoEncodingArquivo(caminho_csv, keyblob, container_name, encoding_esperado, dic):
    """
    Valida o encoding do arquivo no Blob Storage.
    
    :param caminho_csv: Caminho do arquivo CSV.
    :param keyblob: Chave de acesso ao Azure Blob Storage.
    :param container_name: Nome do container no Azure Blob Storage.
    :param encoding_esperado: Encoding esperado do arquivo.
    :param dic: Dicionário para armazenar logs e resultados.
    :return: Tuple (dic, flag_encode_correto, encode_recebido).
    """
    try:
        # Configuração do Blob Storage
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={container_name};AccountKey={keyblob};EndpointSuffix=core.windows.net"
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=caminho_csv)

        # Faz o download de uma amostra do arquivo
        download_stream = blob_client.download_blob()
        sample_bytes = download_stream.read(100000)  # Lê os primeiros 100.000 bytes

        # Detecta o encoding da amostra
        resultado = chardet.detect(sample_bytes)
        encode_arquivo = resultado['encoding']

        # Lista de encodings equivalentes
        encodings_equivalentes = ['windows-1252', 'iso-8859-1']

        # Verifica se o encoding detectado e o esperado estão na lista de equivalentes
        if (encode_arquivo.lower() in encodings_equivalentes and
            encoding_esperado.lower() in encodings_equivalentes):
            dic['Reporte'].append(f"Encoding do arquivo validado com sucesso: {encode_arquivo}.")
            return dic, True, encode_arquivo

        # Verifica se o encoding detectado é igual ao esperado
        if encode_arquivo and encode_arquivo.lower() == encoding_esperado.lower():
            dic['Reporte'].append(f"Encoding do arquivo validado com sucesso: {encode_arquivo}.")
            return dic, True, encode_arquivo

        # Se não for equivalente ou igual, retorna erro
        dic['Erro_Encoding'] = [
            f'O encoding do arquivo está incorreto. Deve ser {encoding_esperado}, mas foi detectado {encode_arquivo}.', ''
        ]
        return dic, False, encode_arquivo

    except Exception as e:
        dic['Erro_Encoding'] = [f'Erro ao validar o encoding do arquivo: {e}', '']
        return dic, False, None
            
def ValidacaoHeader(df, dic, row_tblcliente):
    """
    Valida o cabeçalho do arquivo:
    - Verifica se o arquivo tem cabeçalho.
    - Se não tiver, cria o cabeçalho com base na coluna 'Header' da tabela.
    - Se tiver, verifica o número de colunas e se todos os headers estão presentes.

    Args:
        df (pd.DataFrame): DataFrame do arquivo.
        dic (dict): Dicionário para armazenar resultados.
        row_tblcliente (pd.Series): Linha da tblcliente_tipoarquivo com as configurações do cliente e tipo de arquivo.

    Returns:
        df (pd.DataFrame): DataFrame com cabeçalho validado/renomeado.
        dic (dict): Dicionário atualizado com resultados da validação.
        bool: True se a validação for bem-sucedida, False caso contrário.
    """
    try:
        # Acessa os valores específicos da série
        nomes_colunas_esperadas = row_tblcliente['Header'].split(',')
        is_header = row_tblcliente['IsHeader']
        print(row_tblcliente)

        # Verifica se o arquivo tem cabeçalho
        if is_header == 0:
            # Se não tiver cabeçalho, atribui os nomes das colunas manualmente
            if len(nomes_colunas_esperadas) != df.shape[1]:
                dic['Erro_Header'] = f"Era esperado:  {len(nomes_colunas_esperadas)} colunas, mas o arquivo enviado contem {df.shape[1]}."
                return df, dic, False

            # Atribui os nomes das colunas manualmente
            df.columns = nomes_colunas_esperadas
            dic['Cabeçalho_Criado'] = "Cabeçalho criado com base na coluna 'Header' da tabela."
            logger.info("Cabeçalho criado com sucesso (arquivo sem cabeçalho).")

        # Agora que o cabeçalho foi atribuído (se necessário), valida as colunas
        colunas_arquivo_set = set(df.columns)
        colunas_esperadas_set = set(nomes_colunas_esperadas)

        # Verifica se todas as colunas esperadas estão presentes
        colunas_faltantes = colunas_esperadas_set - colunas_arquivo_set
        colunas_a_mais = colunas_arquivo_set - colunas_esperadas_set

        if colunas_faltantes:
            dic['Erro_Header'] = f"Erro: O arquivo possui {len(colunas_faltantes)} coluna(s) a menos. Colunas faltantes: {', '.join(colunas_faltantes)}."
            return df, dic, False

        if colunas_a_mais:
            dic['Erro_Header'] = f"Erro: O arquivo possui {len(colunas_a_mais)} coluna(s) a mais. Colunas a mais: {', '.join(colunas_a_mais)}."
            return df, dic, False

        # Se chegou até aqui, o cabeçalho está válido
        dic['Cabeçalho_Validado'] = "Cabeçalho validado com sucesso."
        logger.info("Cabeçalho validado com sucesso.")
        return df, dic, True

    except Exception as e:
        dic['Erro_Header'] = f"Erro ao validar o cabeçalho: {e}"
        logger.error(f"Erro ao validar o cabeçalho: {e}")
        return df, dic, False
    
def ValidacaoCamposObrigatorios(df, regras, dic):
    """
    Valida se os campos obrigatórios estão preenchidos no DataFrame.
    
    :param df: DataFrame do arquivo.
    :param regras: DataFrame com as regras (contém o campo 'Obrigatorio' e 'DescricaoCampo').
    :param dic: Dicionário para armazenar resultados.
    :return: Dicionário atualizado com resultados da validação.
    """
    try:
        # Filtra as regras onde 'Obrigatorio' é 1 (campo obrigatório)
        campos_obrigatorios = regras[regras['Obrigatorio'] == 1]['DescricaoCampo'].tolist()

        # Verifica se os campos obrigatórios estão presentes no DataFrame
        campos_faltantes = [campo for campo in campos_obrigatorios if campo not in df.columns]
        if campos_faltantes:
            dic['Erro_Campos_Obrigatorios'] = [
                f"Campos obrigatórios faltantes: {', '.join(campos_faltantes)}.", ''
            ]
            return dic, False

        # Verifica se os campos obrigatórios estão preenchidos (não nulos ou vazios)
        campos_vazios = []
        for campo in campos_obrigatorios:
            if df[campo].isnull().all() or (df[campo].astype(str).str.strip().eq('')).all():
                campos_vazios.append(campo)

        if campos_vazios:
            dic['Erro_Campos_Obrigatorios'] = [
                f"Campos obrigatórios vazios: {', '.join(campos_vazios)}.", ''
            ]
            return dic, False

        # Se todos os campos obrigatórios estiverem preenchidos
        dic['Reporte'].append("Todos os campos obrigatórios estão preenchidos.")
        return dic, True

    except Exception as e:
        dic['Erro_Campos_Obrigatorios'] = [f"Erro ao validar campos obrigatórios: {e}", '']
        return dic, False

def ValidacaoTipoDado(df, regras, dic):
    """
    Valida se os tipos de dados das colunas do DataFrame correspondem aos tipos esperados.
    
    :param df: DataFrame do arquivo.
    :param regras: DataFrame com as regras (contém o campo 'DescricaoCampo' e 'TipoDeDado').
    :param dic: Dicionário para armazenar resultados.
    :return: Dicionário atualizado com resultados da validação.
    """
    try:
        # Dicionário para mapear tipos de dados esperados
        tipo_dado_map = {
            'int': 'int64',
            'float': 'float64',
            'str': 'object',
            'date': 'datetime64[ns]',
            'bool': 'bool'
        }

        # Lista para armazenar erros de tipo de dado
        erros_tipo_dado = []

        for _, regra in regras.iterrows():
            campo = regra['DescricaoCampo']
            tipo_esperado = regra['TipoDeDado'].lower()  # Converte para minúsculas para evitar problemas de case sensitivity

            # Verifica se o campo existe no DataFrame
            if campo not in df.columns:
                erros_tipo_dado.append(f"Campo '{campo}' não encontrado no arquivo.")
                continue

            # Obtém o tipo de dado atual da coluna
            tipo_atual = str(df[campo].dtype)

            # Verifica se o tipo de dado atual corresponde ao tipo esperado
            if tipo_esperado in tipo_dado_map:
                tipo_esperado_pandas = tipo_dado_map[tipo_esperado]
                if tipo_atual != tipo_esperado_pandas:
                    erros_tipo_dado.append(f"Campo '{campo}': Tipo de dado incorreto. Esperado: {tipo_esperado_pandas}, Encontrado: {tipo_atual}.")
            else:
                erros_tipo_dado.append(f"Tipo de dado '{tipo_esperado}' não é suportado para o campo '{campo}'.")

        # Se houver erros, adiciona ao dicionário
        if erros_tipo_dado:
            dic['Erro_Tipo_Dado'] = [
                f"Erros na validação de tipos de dados:\n" + "\n".join(erros_tipo_dado), ''
            ]
            return dic, False

        # Se todos os tipos de dados estiverem corretos
        dic['Reporte'].append("Todos os tipos de dados estão corretos.")
        return dic, True

    except Exception as e:
        dic['Erro_Tipo_Dado'] = [f"Erro ao validar tipos de dados: {e}", '']
        return dic, False
def ValidacaoValoresIn(df, regra, dic):
    """
    Valida se os valores de uma coluna estão dentro de uma lista de valores permitidos.
    
    :param df: DataFrame do arquivo.
    :param regra: Linha da tabela de regras (contém 'DescricaoCampo' e 'Valores').
    :param dic: Dicionário para armazenar resultados.
    :return: Dicionário atualizado com resultados da validação.
    """
    try:
        campo = regra['DescricaoCampo']
        valores_permitidos = regra['Valores'].split(', ')  # Lista de valores permitidos

        # Verifica se o campo existe no DataFrame
        if campo not in df.columns:
            dic['Erro_ValoresIn'] = [f"Campo '{campo}' não encontrado no arquivo.", '']
            return dic, False

        # Filtra valores que não estão na lista de permitidos (ignorando nulos e vazios)
        valores_invalidos = df[~df[campo].isin(valores_permitidos) & df[campo].notna() & (df[campo] != '')]

        # Se houver valores inválidos, registra o erro
        if not valores_invalidos.empty:
            problema = f"Valores fora do esperado na coluna '{campo}'. Valores permitidos: {', '.join(valores_permitidos)}."
            dic['Erro_ValoresIn'] = [problema, valores_invalidos[[campo]].to_string(index=False)]
            return dic, False

        # Se todos os valores estiverem corretos
        dic['Reporte'].append(f"Valores da coluna '{campo}' estão dentro do esperado.")
        return dic, True

    except Exception as e:
        dic['Erro_ValoresIn'] = [f"Erro ao validar valores na coluna '{campo}': {e}", '']
        return dic, False
    
def ValidacaoCNPJ(df, regra, dic, coluna_tipo_pessoa='_c29'):
    """
    Valida se os valores de uma coluna são CNPJs ou CPFs válidos, com base no número de dígitos.
    
    :param df: DataFrame do arquivo.
    :param regra: Linha da tabela de regras (contém 'DescricaoCampo').
    :param dic: Dicionário para armazenar resultados.
    :param coluna_tipo_pessoa: Nome da coluna que indica o tipo de pessoa (PJ ou PF).
    :return: Dicionário atualizado com resultados da validação.
    """
    try:
        campo = regra['DescricaoCampo']

        # Verifica se o campo existe no DataFrame
        if campo not in df.columns:
            dic['Erro_CNPJ'] = [f"Campo '{campo}' não encontrado no arquivo.", '']
            return dic, False

        # Remove caracteres não numéricos (pontos, barras, hífens)
        df['cnpj_limpo'] = df[campo].astype(str).str.replace(r'[./-]', '', regex=True)

        # Verifica o tamanho do CNPJ/CPF e o tipo de pessoa
        condicao_cnpj = (df['cnpj_limpo'].str.len() == 14) & (df[coluna_tipo_pessoa] == 'PJ')
        condicao_cpf = (df['cnpj_limpo'].str.len() == 11) & (df[coluna_tipo_pessoa] == 'PF')

        # Filtra valores inválidos
        valores_invalidos = df[~(condicao_cnpj | condicao_cpf) & df[campo].notna() & (df[campo] != '')]

        # Se houver valores inválidos, registra o erro
        if not valores_invalidos.empty:
            problema = f"Valores inválidos na coluna '{campo}'. CNPJ deve ter 14 dígitos para PJ e CPF deve ter 11 dígitos para PF."
            dic['Erro_CNPJ'] = [problema, valores_invalidos[[campo, coluna_tipo_pessoa]].to_string(index=False)]
            return dic, False

        # Se todos os valores estiverem corretos
        dic['Reporte'].append(f"Valores da coluna '{campo}' são CNPJs ou CPFs válidos.")
        return dic, True

    except Exception as e:
        dic['Erro_CNPJ'] = [f"Erro ao validar CNPJ/CPF na coluna '{campo}': {e}", '']
        return dic, False

def IsChaveUnica(df, colunas_chave, dic):
    """
    Valida se as colunas que compõem a chave MD5 são únicas no DataFrame.

    Args:
        df (pd.DataFrame): DataFrame a ser validado.
        colunas_chave (list): Lista de colunas que compõem a chave MD5.
        dic (dict): Dicionário para armazenar resultados.

    Returns:
        dic (dict): Dicionário atualizado com resultados da validação.
        dfchave_repetidas (pd.DataFrame): DataFrame com as linhas que têm chaves duplicadas.
    """
    try:
        # Verifica se as colunas da chave MD5 estão presentes no DataFrame
        colunas_faltantes = [coluna for coluna in colunas_chave if coluna not in df.columns]

        if colunas_faltantes:
            # Se houver colunas faltantes, atualiza o dicionário com a mensagem de erro
            dic['Erro_Validacao_Chave'] = f"Colunas faltantes para a chave MD5: {', '.join(colunas_faltantes)}"
            return dic, None

        # Verifica se há valores duplicados nas colunas da chave MD5
        duplicados = df[df.duplicated(subset=colunas_chave, keep=False)]

        if not duplicados.empty:
            # Se houver duplicados, atualiza o dicionário com a mensagem de erro
            dic['Erro_Validacao_Chave'] = f"Chave MD5 não é única. Foram encontradas {len(duplicados)} linhas duplicadas."
            return dic, duplicados

        # Se a chave MD5 for válida, atualiza o dicionário com sucesso
        dic['Sucesso_Validacao_Chave'] = "Chave MD5 validada com sucesso."
        return dic, None

    except Exception as e:
        # Em caso de erro, atualiza o dicionário com a mensagem de erro
        dic['Erro_Validacao_Chave'] = f"Erro ao validar chave única: {str(e)}"
        return dic, None

