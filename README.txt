Sistema de Procesamiento de Archivos CMDM
Este proyecto automatiza el procesamiento de archivos CMDM, la integración con servidores FTP y el envío de notificaciones por correo electrónico. Está diseñado para orquestar el flujo completo de descarga, procesamiento, carga y notificación, utilizando una arquitectura modular y robusta.

Características principales
Descarga automática de archivos CMDM desde FTP
Procesamiento y validación de datos contra una base de datos SQL Server
Generación de archivos CMDM y reportes para correo
Carga y reemplazo de archivos en el servidor FTP
Envío de correos de notificación por modificaciones y errores
Registro de eventos y errores en archivos de log
Gestión de reenvíos y vehículos de servicio público
creación de backup del archivo original antes de realizar los cambios.

Estructura del proyecto
main.py: Punto de entrada principal. Orquesta el flujo completo de procesamiento y notificación.
controlador: Módulos de control para FTP, correos y procesamiento de archivos.
modelo: Lógica de acceso y manipulación de datos, consultas SQL y procesamiento de archivos.
servicios: Utilidades para consulta de rutas, destinatarios y resolución de rutas.
vista: Funciones para envío de correos y registro de logs.
log: Carpeta donde se almacenan los archivos de log.
.venv: Entorno virtual de Python para dependencias.
Flujo principal
Descarga el archivo CMDM desde el servidor FTP.
Procesa el archivo descargado:
Si el archivo no está vacío y no hay error, elimina el archivo original del FTP y carga el nuevo archivo procesado.
Si el archivo está vacío, genera el archivo CMDM solo con información de la base de datos y realiza el mismo flujo de eliminación y carga.
Envía correos de notificación:
Si la carga del nuevo archivo es exitosa, envía correo de modificaciones.
Si ocurre algún error en la eliminación o carga, envía correo de errores.
Si la descarga del archivo desde el FTP falla, envía correo de error.

Requisitos
Python 3.8+
Paquetes: pandas, pyodbc, y otros, vea archivo de requeriments.txt.Ejecute:  pip install -r requeriments.txt
Acceso a un servidor FTP y a una base de datos SQL Server y acceso a un servidor SMTP para envío de correos.
Configuración de rutas y credenciales en el archivo .env
Ejecución
Instala las dependencias en tu entorno virtual:
Configura las rutas, credenciales y parámetros en .env
Ejecuta el sistema:

Registro y monitoreo
Todos los eventos importantes y errores se registran en log\log.txt.
El sistema está diseñado para ser robusto ante archivos vacíos, errores de FTP y problemas de procesamiento.