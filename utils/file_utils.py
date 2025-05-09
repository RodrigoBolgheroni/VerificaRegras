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
    try:
        extensao_arquivo = os.path.splitext(caminho_csv)[1].lower()
        if extensao_arquivo != extensao_esperada.lower():
            dic['Erro_Formato'] = [f"Formato de arquivo incorreto. Esperado: {extensao_esperada}, Recebido: {extensao_arquivo}".replace("'", "''"), '']
            return dic, False

        dic['Reporte'].append("Validação de formato do arquivo concluída com sucesso.")
        return dic, True

    except Exception as e:
        dic['Erro_Formato'] = [f"Erro ao validar formato do arquivo: {str(e).replace("'", "''")}", '']
        return dic, False

def ValidacaoVazio(caminho_csv, keyblob, container_name, dic):
    try:
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={container_name};AccountKey={keyblob};EndpointSuffix=core.windows.net"
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=caminho_csv)

        blob_properties = blob_client.get_blob_properties()
        tamanho_arquivo = blob_properties.size

        if tamanho_arquivo == 0:
            dic['Erro_Vazio'] = ["Arquivo vazio: nenhum registro encontrado.".replace("'", "''"), '']
            return dic, False

        dic['Reporte'].append("Validação de arquivo vazio concluída com sucesso.")
        return dic, True

    except Exception as e:
        dic['Erro_Vazio'] = [f"Erro ao validar se o arquivo está vazio: {str(e).replace("'", "''")}", '']
        return dic, False

def ValidacaoEncodingArquivo(caminho_csv, keyblob, container_name, encoding_esperado, dic):
    try:
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={container_name};AccountKey={keyblob};EndpointSuffix=core.windows.net"
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=caminho_csv)

        download_stream = blob_client.download_blob()
        sample_bytes = download_stream.read(100000)
        resultado = chardet.detect(sample_bytes)
        encode_arquivo = resultado['encoding']

        encodings_equivalentes = ['windows-1252', 'iso-8859-1']

        if (encode_arquivo.lower() in encodings_equivalentes and
            encoding_esperado.lower() in encodings_equivalentes):
            dic['Reporte'].append(f"Encoding do arquivo validado com sucesso: {encode_arquivo}.")
            return dic, True, encode_arquivo

        if encode_arquivo and encode_arquivo.lower() == encoding_esperado.lower():
            dic['Reporte'].append(f"Encoding do arquivo validado com sucesso: {encode_arquivo}.")
            return dic, True, encode_arquivo

        dic['Erro_Encoding'] = [
            f'O encoding do arquivo está incorreto. Deve ser {encoding_esperado}, mas foi detectado {encode_arquivo}.'.replace("'", "''"), ''
        ]
        return dic, False, encode_arquivo

    except Exception as e:
        dic['Erro_Encoding'] = [f'Erro ao validar o encoding do arquivo: {str(e).replace("'", "''")}', '']
        return dic, False, None
            
def ValidacaoHeader(df, dic, row_tblcliente):
    try:
        nomes_colunas_esperadas = row_tblcliente['Header'].split(',')
        is_header = row_tblcliente['IsHeader']

        if is_header == 0:
            if len(nomes_colunas_esperadas) != df.shape[1]:
                dic['Erro_Header'] = f"Era esperado: {len(nomes_colunas_esperadas)} colunas, mas o arquivo enviado contem {df.shape[1]}.".replace("'", "''")
                return df, dic, False

            df.columns = nomes_colunas_esperadas
            dic['Cabeçalho_Criado'] = "Cabeçalho criado com base na coluna 'Header' da tabela."

        colunas_arquivo_set = set(df.columns)
        colunas_esperadas_set = set(nomes_colunas_esperadas)

        colunas_a_mais = colunas_arquivo_set - colunas_esperadas_set
        colunas_faltantes = colunas_esperadas_set - colunas_arquivo_set

        if colunas_faltantes:
            dic['Erro_Header'] = f"Erro: O arquivo possui {len(colunas_faltantes)} coluna(s) a menos. Colunas faltantes: {', '.join(colunas_faltantes)}.".replace("'", "''")
            return df, dic, False

        if colunas_a_mais:
            dic['Erro_Header'] = f"Erro: O arquivo possui {len(colunas_a_mais)} coluna(s) a mais. Colunas a mais: {', '.join(colunas_a_mais)}.".replace("'", "''")
            return df, dic, False

        dic['Cabeçalho_Validado'] = "Cabeçalho validado com sucesso."
        return df, dic, True

    except Exception as e:
        dic['Erro_Header'] = f"Erro ao validar o cabeçalho: {str(e).replace("'", "''")}"
        return df, dic, False
    
def ValidacaoCamposObrigatorios(df, regras, dic):
    try:
        campos_obrigatorios = regras[regras['Obrigatorio'] == 1]['DescricaoCampo'].tolist()

        campos_faltantes = [campo for campo in campos_obrigatorios if campo not in df.columns]
        if campos_faltantes:
            dic['Erro_Campos_Obrigatorios'] = [
                f"Campos obrigatórios faltantes: {', '.join(campos_faltantes)}.".replace("'", "''"), ''
            ]
            return dic, False

        campos_vazios = []
        for campo in campos_obrigatorios:
            if df[campo].isnull().all() or (df[campo].astype(str).str.strip().eq('')).all():
                campos_vazios.append(campo)

        if campos_vazios:
            dic['Erro_Campos_Obrigatorios'] = [
                f"Campos obrigatórios vazios: {', '.join(campos_vazios)}.".replace("'", "''"), ''
            ]
            return dic, False

        dic['Reporte'].append("Todos os campos obrigatórios estão preenchidos.")
        return dic, True

    except Exception as e:
        dic['Erro_Campos_Obrigatorios'] = [f"Erro ao validar campos obrigatórios: {str(e).replace("'", "''")}", '']
        return dic, False

def ValidacaoTipoDado(df, regras, dic):
    """
    Valida se os tipos de dados das colunas do DataFrame correspondem aos tipos esperados.
    Tenta converter os campos para o tipo esperado e, se a conversão falhar, registra um erro.
    
    :param df: DataFrame do arquivo.
    :param regras: DataFrame com as regras (contém o campo 'DescricaoCampo' e 'TipoDeDado').
    :param dic: Dicionário para armazenar resultados.
    :return: Dicionário atualizado com resultados da validação.
    """
    try:
        # Dicionário para mapear tipos de dados esperados e funções de conversão
        tipo_dado_map = {
            'inteiro': {
                'tipos_aceitos': 'int64',
                'conversao': lambda x: pd.to_numeric(x.astype(str).str.replace(',', '.'), errors='coerce'),  # Converte para numérico, substituindo vírgula por ponto
                'mensagem_erro': 'int'
            },
            'decimal': {
                'tipos_aceitos': 'float64',
                'conversao': lambda x: pd.to_numeric(x.astype(str).str.replace(',', '.'), errors='coerce'),  # Converte para numérico, substituindo vírgula por ponto
                'mensagem_erro': 'float'
            },
            'texto': {
                'tipos_aceitos': 'object',
                'conversao': lambda x: x.astype(str),  # Converte para texto
                'mensagem_erro': 'texto'
            },
            'data': {
                'tipos_aceitos': 'datetime64[ns]',
                'conversao': lambda x: pd.to_datetime(x, format='%d/%m/%Y', errors='coerce'),  # Converte para data, usando o formato dd/mm/yyyy
                'mensagem_erro': 'data'
            },
            'booleano': {
                'tipos_aceitos': 'bool',
                'conversao': lambda x: x.astype(bool),  # Converte para booleano
                'mensagem_erro': 'booleano'
            },
            'dataehora': {
                'tipos_aceitos': 'datetime64[ns]',
                'conversao': lambda x: pd.to_datetime(x, format='%d/%m/%Y', errors='coerce'),  # Converte para data, usando o formato dd/mm/yyyy
                'mensagem_erro': 'data e hora'
            }
        }

        # Lista para armazenar erros de tipo de dado
        erros_tipo_dado = []

        regras_filtradas = regras[regras['RegraNome'] != 'ValoresIn']

        for _, regra in regras_filtradas.iterrows():
            campo = regra['DescricaoCampo']
            tipo_esperado = regra['TipoDeDado'].lower()  # Converte para minúsculas para evitar problemas de case sensitivity

            # Verifica se o campo existe no DataFrame
            if campo not in df.columns:
                erros_tipo_dado.append(f"Campo {campo} não encontrado no arquivo.")
                continue

            # Verifica se o tipo de dado esperado é suportado
            if tipo_esperado not in tipo_dado_map:
                erros_tipo_dado.append(f"Tipo de dado {tipo_esperado} não é suportado para o campo {campo}.")
                continue

            # Tenta converter o campo para o tipo esperado
            try:
                df[campo] = tipo_dado_map[tipo_esperado]['conversao'](df[campo])
            except Exception as e:
                erros_tipo_dado.append(
                    f"Campo {campo}: Não foi possível converter para {tipo_dado_map[tipo_esperado]['mensagem_erro']}. Erro: {e}"
                )
                continue

            # Verifica se o tipo de dado atual corresponde ao tipo esperado após a conversão
            tipo_esperado_pandas = tipo_dado_map[tipo_esperado]['tipos_aceitos']
            tipo_atual = str(df[campo].dtype)

            if isinstance(tipo_esperado_pandas, list):  # Caso especial para 'número' (aceita int ou float)
                if tipo_atual not in tipo_esperado_pandas:
                    erros_tipo_dado.append(
                        f"Campo {campo}: Tipo de dado incorreto. Esperado: {tipo_dado_map[tipo_esperado]['mensagem_erro']}, Encontrado: {tipo_atual}."
                    )
                    print(df[campo])
            else:
                if tipo_atual != tipo_esperado_pandas:
                    erros_tipo_dado.append(
                        f"Campo {campo}: Tipo de dado incorreto. Esperado: {tipo_dado_map[tipo_esperado]['mensagem_erro']}, Encontrado: {tipo_atual}."
                    )
                    print(df[campo])

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
        dic['Erro_Tipo_Dado'] = [f"Erro ao validar tipos de dados: {str(e).replace("'", "''")}", '']
        return dic, False
    
def ValidacaoValoresIn(df, regra, dic):
    try:
        campo = regra['DescricaoCampo']
        valores_permitidos = [v.strip().upper() for v in regra['Formato'].split(',')]
        
        if campo not in df.columns:
            dic['Erro_ValoresIn'] = [f'Campo "{campo}" não encontrado no arquivo.'.replace("'", "''"), '']
            return dic, False

        df[campo] = df[campo].astype(str).str.strip().str.upper()
        mask = (~df[campo].isin(valores_permitidos)) & (df[campo] != '') & (df[campo] != 'NAN')
        valores_invalidos = df.loc[mask, [campo]]
    
        if not valores_invalidos.empty:
            problema = f'Valores inválidos na coluna "{campo}". Esperados: {valores_permitidos}. Encontrados: {valores_invalidos[campo].unique()[:10]}'.replace("'", "''")
            dic['Erro_ValoresIn'] = [problema, valores_invalidos.head(20).to_string(index=False)]
            return dic, False

        dic['Reporte'].append(f'Valores da coluna "{campo}" válidos.'.replace("'", "''"))
        return dic, True

    except Exception as e:
        dic['Erro_ValoresIn'] = [f'Erro ao validar "{campo}": {str(e).replace("'", "''")}', '']
        return dic, False
    
def ValidacaoCNPJ(df, regra, dic):
    try:
        campo = regra['DescricaoCampo']

        if campo not in df.columns:
            dic['Erro_CNPJ'] = [f"Campo '{campo}' não encontrado no arquivo.".replace("'", "''"), '']
            return dic, False

        df['cnpj_limpo'] = df[campo].astype(str).str.replace(r'[./-]', '', regex=True).str.zfill(14)
        condicao_cnpj = df['cnpj_limpo'].str.len() == 14
        condicao_cpf = df['cnpj_limpo'].str.len() == 11

        valores_invalidos = df[~(condicao_cnpj | condicao_cpf) & df[campo].notna() & (df[campo] != '')]
        if 'cnpj_limpo' in df.columns:
            df.drop(columns=['cnpj_limpo'], inplace=True)

        if not valores_invalidos.empty:
            problema = f'Valores inválidos na coluna "{campo}". CNPJ deve ter 14 dígitos para PJ e CPF deve ter 11 dígitos para PF.'.replace("'", "''")
            dic['Erro_CNPJ'] = [problema, valores_invalidos[[campo]].to_string(index=False)]
            return dic, False

        dic['Reporte'].append(f'Valores da coluna "{campo}" são CNPJs ou CPFs válidos.'.replace("'", "''"))
        return dic, True

    except Exception as e:
        dic['Erro_CNPJ'] = [f'Erro ao validar CNPJ/CPF na coluna "{campo}": {str(e).replace("'", "''")}', '']
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

