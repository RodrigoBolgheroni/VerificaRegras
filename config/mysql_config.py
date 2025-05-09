from sqlalchemy import create_engine
import urllib.parse

def conecta_mysql(server_mysql, user_mysql, password_mysql, bancodedados_mysql):
    password_mysql_encoded = urllib.parse.quote_plus(password_mysql)
    
    # String de conexão para SQLAlchemy
    connection_string = f"mysql+mysqlconnector://{user_mysql}:{password_mysql_encoded}@{server_mysql}/{bancodedados_mysql}"
    
    # Criando o engine e estabelecendo a conexão
    engine = create_engine(connection_string)
    
    # Retornando a conexão
    return engine.connect()
