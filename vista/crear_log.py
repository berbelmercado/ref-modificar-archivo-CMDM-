"""
Módulo para crear logs de la ejecución de tareas.

Este módulo proporciona una función para registrar logs de la ejecución de tareas en un archivo de texto.

Dependencias:
    - servicios.resolver_rutas: Módulo personalizado para resolver rutas de archivos.
    - os: Módulo estándar de Python para interactuar con el sistema operativo.
    - datetime: Módulo estándar de Python para manejar fechas y horas.

Funciones:
    - crea_log(ingreso_datos): Ingresa un registro al archivo de log.
"""
from servicios.resolver_rutas import resource_path
import config
import datetime

def crea_log(ingreso_datos):
    """
    Ingresa un registro al archivo de log.

    Parameters:
    ingreso_datos (str): El mensaje o datos a registrar en el log.

    Variables de entorno requeridas:
    - RUTA_LOG: Ruta del archivo de log donde se guardarán los registros.
    """

    #Obtenemos la fecha actual y lo formateamos
    fecha_actual = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    #Cargamos la ruta donde vamos a guardar el archivo log
    ruta_log = config.RUTA_LOG

    #leemos el archivo txt y le escribimos las nuevas lineas
    with open (ruta_log,'a') as log:
        log.writelines(f'{fecha_actual} {ingreso_datos}\n')
        #log.writelines(f'{ingreso_datos}\n')
