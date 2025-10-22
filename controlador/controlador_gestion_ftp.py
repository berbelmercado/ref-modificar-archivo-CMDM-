"""
Módulo controlador_gestion_ftp.py

Este módulo define la clase GestionFTP, encargada de gestionar la conexión y operaciones con el servidor FTP, como descargar, eliminar y cargar archivos, además de registrar eventos y errores en el log.

Clases:
-------
GestionFTP
    - Encapsula la lógica para conectar al FTP, consultar la ruta, validar la existencia de archivos, descargar, eliminar y cargar archivos en el servidor FTP.

Dependencias:
-------------
- ConexionFTP: Clase para manejar la conexión y operaciones con el servidor FTP.
- ConsultasSql: Clase para consultas a la base de datos (no utilizada directamente aquí).
- crea_log: Función para registrar eventos y errores en el log.
- ConsultarRutaFtp: Clase para consultar la ruta del archivo en el FTP.

Atributos:
----------
- __obj_ruta_ftp: Instancia de ConsultarRutaFtp para obtener la ruta del archivo en el FTP.
- __conexion_ftp: Instancia de ConexionFTP para manejar la conexión y operaciones FTP.
- __ruta_ftp: Ruta del archivo en el servidor FTP.
- estado_archivo: Estado de existencia del archivo en el FTP.

Métodos:
--------
- fn_conexion_ftp(self):
    Consulta la ruta FTP, crea la conexión y valida el acceso al servidor FTP.
    Retorna True si la conexión es exitosa, False en caso contrario y registra el error en el log.

- fn_descargar_archivo_ftp(self):
    Conecta al FTP, valida la existencia del archivo y lo descarga si existe.
    Retorna True si la descarga es exitosa, False en caso contrario y registra el error en el log.

- fn_eliminar_archivo_ftp(self):
    Conecta al FTP y elimina el archivo especificado.
    Retorna True si la eliminación es exitosa, False en caso contrario y registra el error en el log.

- fn_cargar_archivo_ftp(self):
    Conecta al FTP y carga el archivo especificado.
    Retorna True si la carga es exitosa, False en caso contrario y registra el error en el log.

Notas:
------
- Todos los métodos desconectan del FTP después de realizar la operación.
- Los errores y eventos importantes se registran en el log para trazabilidad.
- El flujo está diseñado para ser robusto ante errores de conexión y operaciones fallidas.

"""
from modelo.conexion_ftp import ConexionFTP
from vista.crear_log import crea_log
from servicios.consultar_ruta_ftp import ConsultarRutaFtp

class GestionFTP:
    """
    Clase para la gestión de operaciones con el servidor FTP:
    conexión, descarga, eliminación y carga de archivos.
    """
    #Constructor
    def __init__(self):
        """
        Inicializa los objetos necesarios para la gestión FTP.
        """
        self.__obj_ruta_ftp = ConsultarRutaFtp()
        self.__conexion_ftp = None
        self.__ruta_ftp = None

    def fn_conexion_ftp(self):
        """
        Consulta la ruta del archivo en el FTP y establece la conexión.
        Retorna True si la conexión es exitosa, False en caso contrario.
        Registra los errores en el log.
        """
        #Consultamos la ruta donde se encuentra el archivo en el FTP
        dic_retorno_consulta = self.__obj_ruta_ftp.fn_consultar_ruta_ftp()

        if dic_retorno_consulta['exito']:

            self.__ruta_ftp = dic_retorno_consulta['data']

            #Declaramos objeto de conexión FTP
            self.__conexion_ftp = ConexionFTP(self.__ruta_ftp)

            #Conectamos al FTP
            dic_retorno_conexion_ftp = self.__conexion_ftp.fn_conectar_ftp()

            #Validamos la conexión
            if dic_retorno_conexion_ftp['exito'] is True:
                return True
            else:
                crea_log(f'Error - Falló conexión a FTP Error: {dic_retorno_conexion_ftp['error']}\n')
                return False
        else:
            False

    def fn_descargar_archivo_ftp(self):
        """
        Descarga el archivo desde el servidor FTP si existe en la ruta especificada.
        Retorna True si la descarga es exitosa, False en caso contrario.
        Registra los errores en el log.
        """
        if self.fn_conexion_ftp():
            #Validamos si el archivo existe en la ruta FTP
            self.estado_archivo = self.__conexion_ftp.fn_validar_archivo_ftp()

            if self.estado_archivo is True:
                #Descargamos el archivo
                dic_retorno_descarga_ftp = self.__conexion_ftp.fn_descargar_archivo_ftp()

                if dic_retorno_descarga_ftp['exito']:
                    self.__conexion_ftp.fn_desconecta()
                    return True
                else:
                    self.__conexion_ftp.fn_desconecta()
                    crea_log(f'Error - Error al descargar el archivo: {dic_retorno_descarga_ftp['error']}\n')
                    return False
            else:
                self.__conexion_ftp.fn_desconecta()
                crea_log('Error - El archivo no existe en la ruta FTP especificada.\n')
                return False

    def fn_eliminar_archivo_ftp(self):
        """
        Elimina el archivo especificado en el servidor FTP.
        Retorna True si la eliminación es exitosa, False en caso contrario.
        Registra los errores en el log.
        """
        if self.fn_conexion_ftp():
            dic_retorno_eliminar_archivo_ftp = self.__conexion_ftp.fn_eliminar_archivo_ftp()

            if not dic_retorno_eliminar_archivo_ftp['exito']:
                self.__conexion_ftp.fn_desconecta()
                crea_log(f'Error - Error al eliminar el archivo en el ftp: {dic_retorno_eliminar_archivo_ftp['error']}')
                return False
            else:
                self.__conexion_ftp.fn_desconecta()
                return True
        else:
            return False

    def fn_cargar_archivo_ftp(self):
        """
        Carga el archivo especificado al servidor FTP.
        Retorna True si la carga es exitosa, False en caso contrario.
        Registra los errores y eventos en el log.
        """
        if self.fn_conexion_ftp():

            dic_retorno_cargar_archivo = self.__conexion_ftp.fn_cargar_archivo_ftp()
            if dic_retorno_cargar_archivo['exito']:
                crea_log(f'Se carga correctamente el archivo al FTP')
                self.__conexion_ftp.fn_desconecta()
                return True
            else:
                crea_log(f'Error - No fue posible cargar el archivo al ftp: {dic_retorno_cargar_archivo['error']}\n')
                self.__conexion_ftp.fn_desconecta()
                return False
