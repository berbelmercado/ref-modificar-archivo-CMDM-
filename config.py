from dotenv import load_dotenv
from servicios.resolver_rutas import resource_path
from os import getenv

#Cargamos el archivo .env
load_dotenv(resource_path('.env'))


#conexion a base de datos
SERVIDOR_SQL = getenv('SERVIDOR_SQL')
USUARIO_SQL = getenv('USUARIO_SQL')
BASE_DE_DATOS=getenv('BD_DATASTEWARD')
BD_DATASTEWARD=getenv('BD_DATASTEWARD')
CONTRASENA_SQL = getenv('CONTRASENA_SQL')

#conexion servidor FTP
SERVIDOR_FTP=getenv('SERVIDOR_FTP')
USUARIO_FTP=getenv('USUARIO_FTP')
CONTRASENA_FTP=getenv('CONTRASENA_FTP')

#Ruta a archivos
RUTA_LOG=resource_path(getenv('RUTA_LOG'))
NOMBRE_ARCHIVO_DESCARGA=getenv('NOMBRE_ARCHIVO_DESCARGA')
NOMBRE_ARCHIVO_CARGA=getenv('NOMBRE_ARCHIVO_CARGA')
RUTA_GUARDAR_ARCHIVO=resource_path(getenv('RUTA_GUARDAR_ARCHIVO'))
RUTA_GUARDAR_ARCHIVO_PR=resource_path(getenv('RUTA_GUARDAR_ARCHIVO_PR'))
RUTA_ARCHIVO_BACUP=resource_path(getenv('RUTA_ARCHIVO_BACUP'))
RUTA_ARCHIVO_CORREO=resource_path(getenv('RUTA_ARCHIVO_CORREO'))
NOMBRE_ARCHIVO_CORREO= getenv('NOMBRE_ARCHIVO_CORREO')

#Envío de correo
CORREO_REMITENTE = getenv('CORREO_REMITENTE')
SERVIDOR_SMTP = getenv('SERVIDOR_SMTP')
PUERTO_SERVIDOR_SMTP = getenv('PUERTO_SERVIDOR_SMTP')
ASUNTO_CORREO = getenv('ASUNTO_CORREO')
MENSAJE_CORREO = getenv('MENSAJE_CORREO')
NOMBRE_ADJUNTO_CORREO= getenv('NOMBRE_ADJUNTO_CORREO')

#Correo errores
ASUNTO_CORREO_ERROR=getenv('ASUNTO_CORREO_ERROR')
MENSAJE_CORREO_ERROR = getenv('MENSAJE_CORREO_ERROR')
NOMBRE_ARCHIVO_ERROR = getenv('NOMBRE_ARCHIVO_ERROR')
RANGO_FECHA_CONSULTA= getenv('RANGO_FECHA_CONSULTA')

COLUMNA_ARCHIVO_CMDM = getenv('COLUMNA_ARCHIVO_CMDM').split(',')