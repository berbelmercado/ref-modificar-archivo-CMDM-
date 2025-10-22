
import config
from email.message import EmailMessage
import smtplib

def fn_correo_errores(correos):
    """
    Envía un correo electrónico de notificación de error con un archivo adjunto a una lista de destinatarios.

    Parámetros:
    -----------
    correos : list
        Lista de listas que contiene las direcciones de correo electrónico de los destinatarios.

    Proceso:
    --------
    - Extrae los correos electrónicos de la lista de listas y los agrega a una lista plana.
    - Configura los parámetros del correo (remitente, asunto, mensaje, servidor SMTP, puerto).
    - Adjunta el archivo de log especificado en la configuración.
    - Envía el correo a todos los destinatarios usando SMTP con TLS.
    - Si ocurre una excepción, la captura y la retorna en el diccionario de respuesta.

    Retorna:
    --------
    dict
        {'exito': True, 'data': None} si el correo se envió correctamente.
        {'exito': False, 'data': ex} si ocurrió una excepción, donde ex es el error capturado.

    Ejemplo de uso:
    ---------------
    resultado = fn_correo_errores([["destinatario1@dominio.com"], ["destinatario2@dominio.com"]])
    if resultado['exito']:
        print("Correo enviado correctamente")
    else:
        print("Error al enviar correo:", resultado['data'])
    """
    try:
        correos_destinatarios = []
        for i in correos:
            for j in i:
                correos_destinatarios.append(j)

        #Configuración de correo
        archivo_adjunto = config.RUTA_LOG
        nombre_archivo =  config.NOMBRE_ARCHIVO_ERROR
        remitente = config.CORREO_REMITENTE

        mensaje =config.MENSAJE_CORREO_ERROR
        servidor_smtp = config.SERVIDOR_SMTP
        puerto_servidor_smtp = config.PUERTO_SERVIDOR_SMTP

        email = EmailMessage()
        email["From"] = remitente
        email["To"] =", ".join(correos_destinatarios)
        email["Subject"] = config.ASUNTO_CORREO_ERROR

        email.set_content(mensaje)

        #Adjuntar archivo
        with open (archivo_adjunto,"rb") as archivo:
            datos_archivo = archivo.read()
            nombre_archivo = nombre_archivo
            email.add_attachment(datos_archivo
                                ,maintype="text"
                                ,subtype="plain"
                                ,filename=nombre_archivo)

        smtp = smtplib.SMTP(servidor_smtp,port=puerto_servidor_smtp)
        smtp.starttls()
        smtp.sendmail(remitente
                    ,correos_destinatarios
                    ,email.as_string()
                    )
        smtp.quit()

        return {'exito':True
               ,'data':None}

    except Exception as ex:

        return {'exito':False
               ,'data':ex}
