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

    obj_gestion_ftp = GestionFTP()
    obj_gestion_correos = ControladorGestionCorreos()
    obj_gestion_archivo = ControladorGestionArchivoCmdm()

    # Descarga archivo desde FTP
    retorno_descarga_ftp = obj_gestion_ftp.fn_descargar_archivo_ftp()

    if not retorno_descarga_ftp:
        obj_gestion_correos.fn_correo_error()
        return

    # Procesa archivo (o delta si no existe/está vacío)
    retorno_archivo = obj_gestion_archivo.fn_gestion_archivo()

    # Si hubo un error en el pipeline → correo error
    if retorno_archivo["error"]:
        obj_gestion_correos.fn_correo_error()
        return

    # Si el pipeline fue exitoso → continuamos con FTP
    # Eliminamos el archivo original del FTP
    retorno_eliminacion_ftp = obj_gestion_ftp.fn_eliminar_archivo_ftp()

    if not retorno_eliminacion_ftp:
        obj_gestion_correos.fn_correo_error()
        return

    # Cargamos nuevo archivo CMDM generado al FTP
    retorno_carga_ftp = obj_gestion_ftp.fn_cargar_archivo_ftp()

    if not retorno_carga_ftp:
        obj_gestion_correos.fn_correo_error()
        return

    # Todo bien → enviamos correo de modificaciones
    obj_gestion_correos.fn_correo_modificaciones()


if __name__ == "__main__":
    main()
