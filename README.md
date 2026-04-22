# Automatización de Archivo CMDM — Encuestas de Satisfacción Renault Colombia

Sistema de automatización end-to-end para el procesamiento del archivo **CMDM** de encuestas de satisfacción de clientes Renault en Colombia (SOFASA).  
Integra servidores FTP, bases de datos SQL Server con múltiples fuentes (**SGS, SISC, CONEXION, DATASTEWARD**), lógica de negocio compleja sobre VINs y notificaciones automáticas por correo electrónico.

---

## ¿Qué problema resuelve?

El archivo CMDM es el insumo principal para el sistema de encuestas de satisfacción de Renault.  
El proceso manual implicaba:

- Descargar el archivo desde el FTP  
- Cruzarlo con múltiples bases de datos  
- Aplicar reglas de negocio sobre tipos de vehículo, acuerdos de comunicación, vehículos de servicio público y reenvíos  
- Generar el archivo final actualizado  
- Cargarlo nuevamente al FTP  

Este sistema **automatiza cada paso del flujo**, reduciendo errores y tiempos de ejecución.

---

## 🚀 Flujo del pipeline
```text
FTP] ──descarga──► [Validación y lectura CSV]
│
▼
[SQL Server: consulta reporte DDA]
│
┌────────────┴────────────┐
▼                         ▼
VINs con entrega DDA      VINs sin entrega DDA
│                         │
│                 INSERT delta_cmdm_file
│                         │
└────────────┬────────────┘
▼
[Consulta y actualización de estados delta]
│
▼
[Fusión de DataFrames + fechas de entrega DDA]
│
▼
[Consulta vehículos servicio público → exclusión]
│
▼
[Consulta y procesamiento de reenvíos]
│
▼
[Modificación columna HO según acuerdos y tipo VP/VU]
│
▼
[Generación Excel para correo + archivo CMDM + backup]
│
┌──────┴──────┐
▼             ▼
[FTP: carga]   [SMTP: correo modificaciones]
│             │
si error ──────► [SMTP: correo errores]
```

---

## Arquitectura

```text
ref-modificar-archivo-CMDM-/
│
├── main.py                                      # Orquestador principal del pipeline
├── config.py                                    # Configuración centralizada (rutas, columnas, BD)
├── requeriments.txt
│
├── controlador/
│   ├── controlador_gestion_archivo_cmdm.py      # Pipeline principal (20 etapas encadenadas)
│   │                                            # Cada etapa es un método independiente que
│   │                                            # recibe y enriquece un contexto compartido (dict)
│   ├── controlador_gestion_ftp.py               # Conexión, descarga, eliminación y carga FTP
│   │                                            # Consulta la ruta FTP dinámica desde SQL Server
│   └── controlador_gestion_correos.py           # Envío de correos de modificaciones y errores
│                                                # Consulta destinatarios desde SQL Server
│
├── modelo/
│   ├── consultas_sql.py                         # Capa de acceso a datos (715 líneas):
│   │                                            #   - Consulta reporte DDA por lista de VINs
│   │                                            #   - INSERT masivo a delta_cmdm_file (executemany)
│   │                                            #   - Validación VINs entregados tipo VP/VU
│   │                                            #   - Consulta y actualización de estados
│   │                                            #   - Query de información completa para correo
│   │                                            #     (cruza SGS, SISC, CONEXION, DATASTEWARD)
│   │                                            #   - Consulta destinatarios y rutas FTP desde BD
│   └── conexion_ftp.py                          # Operaciones FTP: conectar, validar, descargar,
│                                                # eliminar y cargar archivos
│
├── vista/
│   ├── crear_log.py                             # Registro de eventos e errores en archivo .log
│   ├── envio_correo_modificaciones.py           # Correo SMTP con Excel adjunto de cambios
│   └── envio_correo_errores.py                  # Correo SMTP de notificación de error
│
└── servicios/
    ├── consulta_correos_destinatarios.py        # Consulta destinatarios desde SQL Server
    └── consultar_ruta_ftp.py                    # Consulta ruta FTP dinámica desde SQL Server
```
| Tecnología | Uso |
| --- | --- |
| **Python 3.x** | Lenguaje principal |
| **Pandas** | Lectura, transformación y fusión de DataFrames CSV/Excel |
| **pyodbc** | Conexión y operaciones a SQL Server (múltiples bases de datos) |
| **ftplib** | Integración FTP: descarga, eliminación y carga de archivos |
| **smtplib** | Envío de correos automáticos con adjuntos Excel |
| **openpyxl** | Generación de archivos Excel para correo y backup |
| **python-dotenv** | Gestión segura de credenciales |
| **PyInstaller** | Empaquetado como ejecutable ``.exe`` para producción |

Instalación y configuración
1. Clonar el repositorio
bash
git clone https://github.com/berbelmercado/ref-modificar-archivo-CMDM-.git
cd ref-modificar-archivo-CMDM-
2. Crear entorno virtual e instalar dependencias
bash
python -m venv venv
venv\Scripts\activate
pip install -r requeriments.txt
3. Configurar variables de entorno
Crea un archivo config.py o .env basándote en la plantilla de ejemplo:

env
# SQL Server
SERVIDOR_SQL=tu_servidor
BASE_DE_DATOS=tu_base_de_datos
USUARIO_SQL=tu_usuario
CONTRASENA_SQL=tu_contraseña

# SMTP
SMTP_HOST=smtp.servidor.com
SMTP_PORT=587
SMTP_USER=correo@empresa.com
SMTP_PASSWORD=contraseña

# Rutas
RUTA_GUARDAR_ARCHIVO=./archivos/cmdm/
RUTA_ARCHIVO_CORREO=./archivos/correo/
RUTA_ARCHIVO_BACKUP=./archivos/backup/
⚠️ Nunca subas credenciales al repositorio. Están excluidas por .gitignore.

4. Ejecutar
bash
python main.py
📌 Despliegue en producción
El sistema se despliega como tarea programada en Windows. Para generar el ejecutable:

bash
pyinstaller --onefile main.py
