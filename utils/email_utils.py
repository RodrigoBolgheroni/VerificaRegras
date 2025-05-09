from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib, ssl

def outputemail(dic): 
    string_final = ''
    # Verifica se o dicionário tem apenas uma chave
    if len(dic.keys()) == 1:
        string_final = '<br>' + 'Reporte' + '<br>' + dic['Reporte'][0] + '<br>' + 'Validacao concluida com sucesso!'
        return string_final
    else:
        # Para cada chave no dicionário
        for string in dic.keys():
            string_final += f'<br>{string}<br>'  # Adiciona a chave (como título)
            
            # Adiciona os valores da lista associados à chave
            for i in range(min(8, len(dic[string]))):  # Limita a 8 elementos por chave
                string_final += dic[string][i] + '<br>'
        
        return string_final



    # Conexão SSL segura com o servidor SMTP
    context = ssl.create_default_context() 
    with smtplib.SMTP(smtp_server, port) as server: 
        server.ehlo('mylowercasehost') 
        server.starttls(context=context) 
        server.ehlo('mylowercasehost') 
        server.login(sender_email, password)
        text = message.as_string()
        server.sendmail(sender_email, recipients, text)

