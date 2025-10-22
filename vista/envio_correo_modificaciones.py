
import config
from email.message import EmailMessage
import smtplib

def correo_modificacion_encuestas(correos):

    try:
        correos_destinatarios = []
        for i in correos:
            for j in i:
                correos_destinatarios.append(j)

        #Configuración de correo
        archivo_adjunto = config.RUTA_ARCHIVO_CORREO + config.NOMBRE_ARCHIVO_CORREO
        nombre_archivo =  config.NOMBRE_ARCHIVO_CORREO
        remitente = config.CORREO_REMITENTE

        mensaje =config.MENSAJE_CORREO
        servidor_smtp = config.SERVIDOR_SMTP
        puerto_servidor_smtp =int(config.PUERTO_SERVIDOR_SMTP)

        email = EmailMessage()
        email["From"] = remitente
        email["To"] = ", ".join(correos_destinatarios)
        email["Subject"] = config.ASUNTO_CORREO
        
        email.set_content(mensaje)

        #Adjuntar archivo
        with open (archivo_adjunto,"rb") as archivo:
            datos_archivo = archivo.read()
            nombre_archivo = nombre_archivo
            email.add_attachment(datos_archivo
                                ,maintype="application"
                                ,subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                ,filename=nombre_archivo)

        smtp = smtplib.SMTP(servidor_smtp,port=puerto_servidor_smtp)
        smtp.starttls()
        smtp.sendmail(remitente
                    ,correos_destinatarios
                    ,email.as_string()
                    )
        smtp.quit()

        return {'exito':True
                ,'error':None}

    except NameError as ex:
        {'exito':False
        ,'error':ex}

