import pandas as pd
import traceback
import logging
from utils.db_functions import SqlServerQuery, ExecuteScriptMySQL, MysqlQuery, atualizar_status_arquivo,atualizarStatusArquivo,atualizarStatusRegra,selecionaIdLog,insereLog
from utils.file_utils import (
    ValidacaoEncodingArquivo, ValidacaoExtensaoArquivo, ValidacaoHeader,
    ValidacaoCamposObrigatorios, ValidacaoTipoDado, ValidacaoValoresIn, ValidacaoCNPJ,ValidacaoVazio
)
from utils.file_processing import *
from datetime import datetime, timedelta
from utils.data_processing import CargaSqlServer, create_delta_table
from utils.email_utils import enviar_email, outputemail

from dotenv import load_dotenv
import os

# Carregar variáveis de ambiente
load_dotenv()

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def Main(cliente, tipoarquivoMySQL, tipoarquivo,ambiente, server_mysql, user_mysql, password_mysql, bancodedados_mysql, keyblob):
    """
    Função principal para processar arquivos e aplicar validações.
    
    :param cliente: Nome do cliente.
    :param tipoarquivo: Tipo de arquivo a ser processado.
    :param server_mysql: Servidor MySQL.
    :param user_mysql: Usuário MySQL.
    :param password_mysql: Senha MySQL.
    :param bancodedados_mysql: Banco de dados MySQL.
    :param keyblob: Senha do Blob.
    :param ambiente: Ambiente que esta sendo executado.
    """
    status = "Concluido"
    mensagem_erro = None
    try:
        # Consulta a tabela de arquivos
        row_tabelacliente_tipoarquivo = processar_arquivo(cliente, tipoarquivo)

        idtblcliente_tipoarquivo = row_tabelacliente_tipoarquivo['IdCliente_TipoArquivo']

        IdProcessamento = selecionaIdLog() + 1

        # Cria a Variavel de detalhe inicial
        detalhe = f"Parametro 01:{cliente} , Parametro 02:{tipoarquivo} , Parametro 03:{ambiente} , Parametro 04:{server_mysql} , Parametro 05:{user_mysql} , Parametro 06:{password_mysql} , Parametro 07:{bancodedados_mysql} , Parametro 08:{keyblob}"

        status = "Homologando"
        mensagem_erro = None

        # Insere o log no banco
        insereLog(IdProcessamento,idtblcliente_tipoarquivo,"Processamento Iniciado",detalhe,status)

        # Consulta arquivos na tabela tblpbiarquivoimport
        #query_tblarquivo_import = f"""
        #    SELECT * FROM tblpbiarquivoimport 
        #    WHERE (Status = 'NaoIniciado' OR Status = 'NaFila') 
        #    AND TipoArquivo = '{tipoarquivoMySQL}'
        #""""  
        query_tblarquivo_import = f"""
            SELECT * FROM tblpbiarquivoimport 
            WHERE (Id = 737) 
        """

        tabela_arquivoimport = MysqlQuery(server_mysql, user_mysql, password_mysql, bancodedados_mysql, query_tblarquivo_import)
        
        for _, row_tabelaarquivo_import in tabela_arquivoimport.iterrows():
            id_arquivo = row_tabelaarquivo_import['Id']
            dic = {'Reporte': []}  # Dicionário para armazenar logs

            # PARTE 1 - PROCESSAMENTO
            try:
                detalhe = "Atualiza status para homologando, importa as regras"

                # Insere o log no banco
                insereLog(IdProcessamento,idtblcliente_tipoarquivo, "Configurando o arquivo para homologação", detalhe,status)
                # Atualizar status do arquivo para "Homologando"
                id_status_arquivo, flag_atualizastatus = atualizarStatusArquivo(
                    id_arquivo=row_tabelacliente_tipoarquivo['IdCliente_TipoArquivo'],
                    ambiente=ambiente,
                    container=row_tabelaarquivo_import['ContainerName'],
                    diretorio=row_tabelaarquivo_import['CaminhoOrigem'],
                    nomearquivo=row_tabelaarquivo_import['TabelaOrigem'],
                    tamanho=None,  # Tamanho do arquivo em KB
                    quantidade_registros=None,  # Quantidade de registros no arquivo
                    status=status,
                    mensagem_erro=mensagem_erro,
                    id_tabela_pbi=row_tabelaarquivo_import['Id']
                )

                if not flag_atualizastatus:
                    mensagem = "Erro ao inserir informações da tblimport na tblstatusarquivo"
                    status = "Erro"
                    insereLog(IdProcessamento,idtblcliente_tipoarquivo, "Erro ao configurar arquivo", mensagem,status)
                    raise




                # Atualizar status no PBI
                #flag_atualizastatus_pbi = atualizar_status_arquivo(
                #    server_mysql, user_mysql, password_mysql, bancodedados_mysql,
                #    id_arquivo, status, mensagem_erro=mensagem_erro
                #)

                #if not flag_atualizastatus_pbi:
                #    mensagem = "Erro ao atualizar o status na tblpbiimport para Homologando"
                #    status = "Erro"
                #    insereLog(IdProcessamento,idtblcliente_tipoarquivo, "Erro ao configurar arquivo", mensagem,status)
                #    raise


                # Processar regras
                regras, flag_processarregras = processar_regras(row_tabelacliente_tipoarquivo)

                if not flag_processarregras:
                    mensagem = "Erro ao consultar regras do cliente e tipoarquivo"
                    status = "Erro"
                    insereLog(IdProcessamento,idtblcliente_tipoarquivo, "Erro ao configurar arquivo", mensagem,status)
                    raise

                # Inicia o processo de estrutura do arquivo
                detalhe = "Inicia as validações de estrutura do arquivo: Extensão, Encoding e se o arquivo e vazio,caso estiver tudo correto,lê o arquivo do blob"

                # Insere o log no banco
                insereLog(IdProcessamento, idtblcliente_tipoarquivo, "Processo da validação da estrutura do arquivo iniciado.", detalhe, status)

                # Processar DataFrame
                caminho_csv = row_tabelaarquivo_import['CaminhoOrigem'] + "/" + row_tabelaarquivo_import['TabelaOrigem']
                df, flag_processa, flags, dic = processar_df(row_tabelacliente_tipoarquivo, caminho_csv, keyblob, row_tabelaarquivo_import['ContainerName'], dic)

                mensagensfinal = []

                if not flag_processa:
                # Verifica cada flag de validação e gera um log individual para cada erro
                    if not flags['flag_extensao']:
                            mensagem = dic.get('Erro_Formato')[0]
                            status = "ErroHomologacao"
                            mensagensfinal.append(mensagem)
                            insereLog(IdProcessamento, idtblcliente_tipoarquivo, "Erro na validação da extensão do arquivo", mensagem, status)
                        
                    if not flags['flag_vazio']:
                            mensagem = dic.get('Erro_Vazio')[0]
                            status = "ErroHomologacao"
                            mensagensfinal.append(mensagem)
                            insereLog(IdProcessamento, idtblcliente_tipoarquivo, "Erro na validação de arquivo vazio", mensagem, status)
                        
                    if not flags['flag_encoding']:
                            mensagem = dic.get('Erro_Encoding')[0]
                            status = "ErroHomologacao"
                            mensagensfinal.append(mensagem)
                            insereLog(IdProcessamento, idtblcliente_tipoarquivo, "Erro na validação do encoding do arquivo", mensagem, status)
                        
                    if not flags['flag_validacao_header']:  # Verifica a validação do cabeçalho
                            print(dic)
                            mensagem = dic.get('Erro_Header')[0]
                            
                            status = "ErroHomologacao"
                            mensagensfinal.append(mensagem)
                            insereLog(IdProcessamento, idtblcliente_tipoarquivo, "Erro na validação do cabeçalho do arquivo", mensagem, status)
                        
                        # Se o erro não foi nas validações, mas no processamento do DataFrame
                    if all(flags.values()):
                            mensagem = "Erro ao ler arquivo do blob."
                            status = "Erro"
                            mensagensfinal.append(mensagem)
                            insereLog(IdProcessamento, idtblcliente_tipoarquivo, "Erro ao ler arquivo", mensagem, status)
                        
                    # Lança uma exceção para interromper o fluxo com todas as mensagens de erro
                    raise Exception(" | ".join(mensagensfinal))


            except Exception as e:
                logger.error(f"{e}")
                logger.error(traceback.format_exc())

                atualizarStatusArquivo(
                    id_arquivo=row_tabelacliente_tipoarquivo['IdCliente_TipoArquivo'],
                    ambiente=ambiente,
                    container=row_tabelaarquivo_import['ContainerName'],
                    diretorio=row_tabelaarquivo_import['CaminhoOrigem'],
                    nomearquivo=row_tabelaarquivo_import['TabelaOrigem'],
                    tamanho=None,  # Tamanho do arquivo em KB
                    quantidade_registros=None,  # Quantidade de registros no arquivo
                    status=status,
                    mensagem_erro=e,
                    id_tabela_pbi=row_tabelaarquivo_import['Id']
                )
                #atualizar_status_arquivo(
                #    server_mysql, user_mysql, password_mysql, bancodedados_mysql,
                #    id_arquivo, status, mensagem_erro=mensagem
                #)
                break

            
            # PARTE 2 - VALIDAÇÕES SEGUINTES
            logger.info("Validações Iniciais Bem Sucedidas")
            try:
                logger.info("Parte 2 do processamento de validações")
                detalhe = "Iniciando validações de campos obrigatórios, tipos de dados, valores permitidos e CNPJ/CPF"

                # Insere o log no banco
                insereLog(IdProcessamento, idtblcliente_tipoarquivo, "Iniciando validações adicionais", detalhe, status)

                # PARTE 3A - VALIDAÇÃO CAMPOS OBRIGATÓRIOS
                dic, flag_campos_obrigatorios = ValidacaoCamposObrigatorios(df, regras, dic)
                if not flag_campos_obrigatorios:
                    erro_msg = dic.get('Erro_Campos_Obrigatorios', ['Erro na validação de campos obrigatórios', ''])[0]
                    status = "ErroHomologacao"
                    insereLog(IdProcessamento, idtblcliente_tipoarquivo, "Erro na validação de campos obrigatórios", erro_msg, status)
                    atualizarStatusRegra(
                        id_status_arquivo,
                        regras['Id'], 
                        mensagem_erro=erro_msg
                    )
                    raise ValueError(erro_msg)

                # PARTE 3B - VALIDAÇÃO TIPO DE DADO
                dic, flag_tipo_dado = ValidacaoTipoDado(df, regras, dic)
                if not flag_tipo_dado:
                    erro_msg = dic.get('Erro_Tipo_Dado', ['Erro na validação de tipos de dados', ''])[0]
                    status = "ErroHomologacao"
                    insereLog(IdProcessamento, idtblcliente_tipoarquivo, "Erro na validação de tipos de dados", erro_msg, status)
                    atualizarStatusRegra(
                        id_status_arquivo,
                        regras['Id'], 
                        mensagem_erro=erro_msg
                    )
                    raise ValueError(erro_msg)

                # PARTE 3C - VALIDAÇÃO VALORES IN
                for _, regra in regras[regras['Regra'] == 'IsValoresIn'].iterrows():
                    dic, flag_valores_in = ValidacaoValoresIn(df, regra, dic)
                    if not flag_valores_in:
                        erro_msg = dic.get('Erro_ValoresIn', ['Erro na validação de valores permitidos', ''])[0]
                        status = "ErroHomologacao"
                        insereLog(IdProcessamento, idtblcliente_tipoarquivo, "Erro na validação de valores permitidos", erro_msg, status)
                        atualizarStatusRegra(
                            id_status_arquivo,
                            regras['Id'], 
                            mensagem_erro=erro_msg
                        )
                        raise ValueError(erro_msg)

                # PARTE 3D - VALIDAÇÃO CNPJ
                for _, regra in regras[regras['Regra'] == 'IsCNPJ'].iterrows():
                    dic, flag_cnpj = ValidacaoCNPJ(df, regra, dic)
                    if not flag_cnpj:
                        erro_msg = dic.get('Erro_CNPJ', ['Erro na validação de CNPJ/CPF', ''])[0]
                        status = "ErroHomologacao"
                        insereLog(IdProcessamento, idtblcliente_tipoarquivo, "Erro na validação de CNPJ/CPF", erro_msg, status)
                        atualizarStatusRegra(
                            id_status_arquivo,
                            regras['Id'], 
                            mensagem_erro=erro_msg
                        )
                        raise ValueError(erro_msg)

                # Atualiza o status do arquivo para sucesso nas validações
                atualizarStatusArquivo(
                    id_arquivo=row_tabelacliente_tipoarquivo['IdCliente_TipoArquivo'],
                    ambiente=ambiente,
                    container=row_tabelaarquivo_import['ContainerName'],
                    diretorio=row_tabelaarquivo_import['CaminhoOrigem'],
                    nomearquivo=row_tabelaarquivo_import['TabelaOrigem'],
                    tamanho=(df.memory_usage(deep=True).sum() / 1024),  # Tamanho do arquivo em KB
                    quantidade_registros=len(df),  # Quantidade de registros no arquivo
                    status="Sucesso",
                    mensagem_erro=None,  # Sem mensagem de erro
                    id_tabela_pbi=row_tabelaarquivo_import['Id']
                )

                # Atualizar status no PBI (código comentado)
                #flag_atualizastatus_pbi = atualizar_status_arquivo(
                #    server_mysql, user_mysql, password_mysql, bancodedados_mysql,
                #    id_arquivo, status, mensagem_erro=mensagem_erro
                #)

                #if not flag_atualizastatus_pbi:
                #    mensagem = "Erro ao atualizar o status na tblpbiimport para Sucesso"
                #    status = "Erro"
                #    insereLog(IdProcessamento, idtblcliente_tipoarquivo, "Erro ao atualizar status no PBI", mensagem, status)
                #    raise

            except Exception as e:
                logger.error(f"Erro ao aplicar validações seguintes ao arquivo {id_arquivo}: {e}")
                logger.error(traceback.format_exc())
                atualizarStatusArquivo(
                    id_arquivo=row_tabelacliente_tipoarquivo['IdCliente_TipoArquivo'],
                    ambiente=ambiente,
                    container=row_tabelaarquivo_import['ContainerName'],
                    diretorio=row_tabelaarquivo_import['CaminhoOrigem'],
                    nomearquivo=row_tabelaarquivo_import['TabelaOrigem'],
                    tamanho=(df.memory_usage(deep=True).sum() / 1024),  # Tamanho do arquivo em KB
                    quantidade_registros=len(df),  # Quantidade de registros no arquivo
                    status="Erro",
                    mensagem_erro=str(e),  # Mensagem de erro
                    id_tabela_pbi=row_tabelaarquivo_import['Id']
                )
                #atualizar_status_arquivo(
                #    server_mysql, user_mysql, password_mysql, bancodedados_mysql,
                #    id_arquivo, status, mensagem_erro=str(e)
                #)
                continue

            # PARTE 4 - CARGA NO SQL SERVER
            try:
                # Carregar dados no SQL Server
                CargaSqlServer(df, row_tabelaarquivo_import)

                # Atualizar status do arquivo para "Concluído"
                atualizarStatusArquivo(
                    id_arquivo=row_tabelacliente_tipoarquivo['IdCliente_TipoArquivo'],
                    ambiente=ambiente,
                    container=row_tabelaarquivo_import['ContainerName'],
                    diretorio=row_tabelaarquivo_import['CaminhoOrigem'],
                    nomearquivo= row_tabelaarquivo_import['TabelaOrigem'],
                    tamanho=(df.memory_usage(deep=True).sum() / 1024),  # Tamanho do arquivo em KB
                    quantidade_registros=len(df),  # Quantidade de registros no arquivo
                    status="Concluido",
                    mensagem_erro=None,  # Sem mensagem de erro
                    id_tabela_pbi=row_tabelaarquivo_import['Id']
                )

            except Exception as e:
                logger.error(f"Erro ao carregar dados no SQL Server: {e}")
                logger.error(traceback.format_exc())
                atualizarStatusArquivo(
                    id_arquivo=row_tabelacliente_tipoarquivo['IdCliente_TipoArquivo'],
                    id_tabela_pbi=row_tabelaarquivo_import['Id'],
                    id_cliente=row_tabelacliente_tipoarquivo['IdCliente'],
                    id_tipo_arquivo=row_tabelacliente_tipoarquivo['IdTipoArquivo'],
                    status="Erro",
                    tamanho=None,
                    quantidade_registros=None,
                    mensagem_erro=str(e)  # Mensagem de erro
                )
                continue

            # PARTE 5 - ENVIO DE E-MAIL
            try:
                # Gera o corpo do e-mail
                string_final = outputemail(dic)
                logger.info("Corpo do e-mail criado com sucesso.")

                # Envia o e-mail
                enviar_email(string_final, row_tabelaarquivo_import)
                logger.info("E-mail enviado com sucesso.")

            except Exception as e:
                logger.error(f"Erro ao enviar e-mail: {e}")
                logger.error(traceback.format_exc())
                # Atualiza o status do arquivo para "Erro"
                atualizarStatusArquivo(
                    id_arquivo=row_tabelacliente_tipoarquivo['IdCliente_TipoArquivo'],
                    id_tabela_pbi=row_tabelaarquivo_import['Id'],
                    id_cliente=row_tabelacliente_tipoarquivo['IdCliente'],
                    id_tipo_arquivo=row_tabelacliente_tipoarquivo['IdTipoArquivo'],
                    status="Erro",
                    tamanho=None,
                    quantidade_registros=None,
                    mensagem_erro=str(e)  # Mensagem de erro
                )

    # PARTE 6 - TRATAMENTO DE ERRO GERAL
    except Exception as e:
        logger.error(f"Erro geral no processamento: {e}")
        logger.error(traceback.format_exc())


    # Cria a Variavel de detalhe inicial
    if row_tabelaarquivo_import.isnull().all():
        detalhe = "Não há nenhum arquivo novo disponível para processamento"

        # Insere o log no banco
        insereLog(IdProcessamento, idtblcliente_tipoarquivo, "Processo Finalizado", detalhe,status)



# Executar a função Main
Main('Heineken', 'FaturamentoSAP', 'Faturamento','Prod', os.getenv('DB_HOST_MYSQL'), os.getenv('DB_USER_MYSQL'), os.getenv('DB_PASSWORD_MYSQL'), os.getenv('DB_NAME_MYSQL'), os.getenv('BLOB_KEY'))
