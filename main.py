"""
Módulo main.py

Este módulo contiene el punto de entrada principal para la ejecución del sistema de procesamiento de archivos CMDM, integración con FTP y envío de correos de notificación. 
Orquesta el flujo completo de descarga, procesamiento, carga y notificación, utilizando los controladores definidos en el sistema.

Dependencias:
-------------
- GestionFTP: Clase para la gestión de operaciones con el servidor FTP (descarga, eliminación, carga).
- ControladorGestionCorreos: Clase para la gestión y envío de correos electrónicos de modificaciones y errores.
- ControladorGestionArchivoCmdm: Clase para el procesamiento y generación de archivos CMDM.

Flujo principal:
----------------
1. Descarga el archivo CMDM desde el servidor FTP.
2. Procesa el archivo descargado:
   - Si el archivo no está vacío y no hay error, elimina el archivo original del FTP y carga el nuevo archivo procesado.
   - Si el archivo está vacío, genera el archivo CMDM solo con información de la base de datos y realiza el mismo flujo de eliminación y carga.
3. Envía correos de notificación:
   - Si la carga del nuevo archivo es exitosa, envía correo de modificaciones.
   - Si ocurre algún error en la eliminación o carga, envía correo de errores.
4. Si la descarga del archivo desde el FTP falla, envía correo de error.

Funciones:
----------
- main():
    Ejecuta el flujo principal de procesamiento, integración FTP y notificación por correo.

Notas:
------
- El módulo debe ejecutarse como script principal (`__main__`).
- Todos los eventos importantes y errores se gestionan mediante los controladores y se notifican por correo.
- El flujo está diseñado para ser robusto ante archivos vacíos, errores de FTP y problemas de procesamiento.

"""
from controlador.controlador_gestion_ftp import GestionFTP
from controlador.controlador_gestion_correos import ControladorGestionCorreos
from controlador.controlador_gestion_archivo_cmdm import ControladorGestionArchivoCmdm


def main():
    """
    Función principal que orquesta el flujo de procesamiento de archivos CMDM:
    - Descarga el archivo desde el FTP.
    - Procesa el archivo y genera el nuevo archivo CMDM.
    - Elimina el archivo original del FTP y carga el nuevo archivo.
    - Envía correos de notificación según el resultado de las operaciones.
    """
    obj_gestion_ftp = GestionFTP()
    obj_gestion_correos = ControladorGestionCorreos()
    obj_gestion_archivo = ControladorGestionArchivoCmdm()

    #Descargamos el archivo desde el FTP
    retorno_descarga_ftp = obj_gestion_ftp.fn_descargar_archivo_ftp()

    if retorno_descarga_ftp:
        #Realizamos la modificación del archivo
        retorno_archivo = obj_gestion_archivo.fn_gestion_archivo()
        #Si el archivo no está vacío y no hay error eliminamos el archivo original del ftp
        if retorno_archivo['error'] == True and retorno_archivo['tamano']==False:
            #Eliminamos archivo
            retorno_eliminacion_ftp = obj_gestion_ftp.fn_eliminar_archivo_ftp()
            #Si se elimina correctamente cargamos el nuevo archivo
            if retorno_eliminacion_ftp:
                #Cargamos el archivo nuevo al FTP
                retorno_carga_ftp  = obj_gestion_ftp.fn_cargar_archivo_ftp()
                #si se carga correctamente el nuevo archivo
                if retorno_carga_ftp:
                    #Enviamos correo de modificaciones
                    obj_gestion_correos.fn_correo_modificaciones()
                else:
                    #Enviamos correo de errores si falla la carga de nuevo archivo
                    obj_gestion_correos.fn_correo_error()
            else:
                #Enviamos correo de errores si falla la eliminación del archivo viejo
                obj_gestion_correos.fn_correo_error()

        #si el archivo está vacío ejecutamos el proceso para cargar datos solo desde la bd
        elif retorno_archivo['error'] == False and retorno_archivo['tamano']==True:

            #Generamos archivo cmdm con información de la bd
            retorno_archivo = obj_gestion_archivo.fn_cargar_data_cmdm()

            if retorno_archivo['error'] == True:
                #Eliminamos archivo
                retorno_eliminacion_ftp = obj_gestion_ftp.fn_eliminar_archivo_ftp()
                #Si se elimina correctamente cargamos el nuevo archivo
                if retorno_eliminacion_ftp:
                    #Cargamos el archivo nuevo al FTP
                    retorno_carga_ftp  = obj_gestion_ftp.fn_cargar_archivo_ftp()
                    #si se carga correctamente el nuevo archivo
                    if retorno_carga_ftp:
                        #Enviamos correo de modificaciones
                        obj_gestion_correos.fn_correo_modificaciones()
                    else:
                        #Enviamos correo de errores si falla la carga de nuevo archivo
                        obj_gestion_correos.fn_correo_error()
                else:
                    #Enviamos correo de errores si falla la eliminación del archivo viejo
                    obj_gestion_correos.fn_correo_error()
    else:
        obj_gestion_correos.fn_correo_error()

if __name__=="__main__":
    main()
