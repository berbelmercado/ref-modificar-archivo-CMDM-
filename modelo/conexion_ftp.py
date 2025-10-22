import ftplib
import config

class ConexionFTP:

    def __init__(self,ruta_ftp):
        """
        Inicializa la conexión al servidor FTP.

        Variables de entorno requeridas:
        - SERVIDOR_FTP: Dirección del servidor FTP.
        - USUARIO_FTP: Nombre de usuario para la conexión FTP.
        - CONTRASENA_FTP: Contraseña para la conexión FTP.
        """
        self.__host = config.SERVIDOR_FTP
        self.__user = config.USUARIO_FTP
        self.__passwd = config.CONTRASENA_FTP
        self.__nombre_archivo = config.NOMBRE_ARCHIVO_DESCARGA
        self.__ruta_descarga_archivo = config.RUTA_GUARDAR_ARCHIVO
        self.__nombre_archivo_carga = config.NOMBRE_ARCHIVO_CARGA
        self.__ruta_ftp = ruta_ftp
        self.__ftp = None

    def fn_conectar_ftp(self):
        """Conecta al servidor FTP y navega a la ruta especificada."""
        # Crear la conexión FTP
        try:
            self.__ftp = ftplib.FTP(host = self.__host
                                  ,user = self.__user
                                  ,passwd = self.__passwd)

            # Nos ubicamos en la ruta que nos interesa
            self.directorio = self.__ftp.cwd(self.__ruta_ftp)
            return {'exito':True,'error':None}
        except Exception as ex:
            return {'exito':False, 'error':ex}
        
    def fn_validar_archivo_ftp(self):
        # Validar si el archivo existe en la ruta FTP
        archivos_ftp = self.__ftp.nlst()
        if self.__nombre_archivo not in archivos_ftp:
            self.fn_desconecta()
            return False
        else:
            return True

    def fn_descargar_archivo_ftp(self):

        #descargamos el archivo
        try:
            with open (self.__ruta_descarga_archivo, 'wb') as data:
                self.__ftp.retrbinary(f'RETR {self.__nombre_archivo}'
                                                ,data.write)
            return {'exito':True,'error':None}
        except Exception as ex:
            return {'exito':False, 'error':ex}

    def fn_eliminar_archivo_ftp(self):
        try:
            #Elimina archivo en ftp
            self.__ftp.delete(self.__nombre_archivo)
            return {'exito':True,'error':None}
        except Exception as ex:
            return {'exito':False,'error':ex}

    def fn_cargar_archivo_ftp(self):
        try:
            with open(self.__ruta_descarga_archivo, "rb") as datos:
                self.__ftp.storbinary(f'STOR {self.__ruta_ftp+self.__nombre_archivo_carga}'
                                            ,datos
                                            ,callback=None)
            return {'exito':True,'error':None}
        except Exception as ex:
            return {'exito':False,'error':ex}

    def fn_desconecta(self):
        """Función para liberar la conexión al FTP"""
        #liberar conexion
        self.__ftp.close()
