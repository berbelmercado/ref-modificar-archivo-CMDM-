#  Automatización de Procesamiento CMDM — Integración FTP + SQL Server + SMTP

Sistema de automatización end-to-end que orquesta el ciclo completo de procesamiento de archivos CSV/CMDM para clientes de Renault Colombia (SOFASA). Integra servidores FTP, bases de datos SQL Server y notificaciones por correo electrónico, implementado con arquitectura MVC en Python.

---

##  ¿Qué hace este proyecto?

El sistema reemplaza un proceso manual de actualización de archivos de encuestas de satisfacción de clientes automotrices, automatizando cada paso del pipeline de extremo a extremo:

1. **Descarga** el archivo CMDM vigente desde un servidor FTP remoto
2. **Procesa** el archivo CSV aplicando reglas de negocio almacenadas en SQL Server  
   - Si el archivo está vacío o no existe, genera el CMDM directamente desde la base de datos
3. **Elimina** el archivo original del FTP una vez procesado correctamente
4. **Carga** el nuevo archivo CMDM generado al servidor FTP
5. **Notifica** el resultado por correo electrónico:
   -  Correo de modificaciones exitosas si todo el pipeline es correcto
   -  Correo de error con detalle de la etapa fallida si ocurre algún problema

El manejo de errores está implementado por etapas: si cualquier paso del pipeline falla, el sistema detiene la ejecución y envía una notificación de error inmediatamente, sin continuar con pasos posteriores.

---

##  Arquitectura

El proyecto sigue el patrón de diseño **MVC (Modelo-Vista-Controlador)**, lo que permite separar claramente las responsabilidades y facilitar la extensión del sistema sin modificar la lógica central.

```
ref-modificar-archivo-CMDM/
│
├── main.py                          # Orquestador principal del pipeline
├── config.py                        # Configuración centralizada (credenciales, rutas, parámetros)
├── requeriments.txt                 # Dependencias del proyecto
│
├── controlador/
│   ├── controlador_gestion_ftp.py          # Operaciones FTP: descarga, eliminación, carga
│   ├── controlador_gestion_correos.py      # Envío de notificaciones SMTP
│   └── controlador_gestion_archivo_cmdm.py # Lógica de procesamiento del archivo CMDM
│
├── modelo/                          # Capa de acceso a datos (SQL Server)
│
└── vista/                           # Capa de presentación / reportes
```

### Flujo del pipeline

```
[FTP Server] ──descarga──► [Procesamiento CSV + SQL Server] ──elimina──► [FTP Server]
                                        │                                      │
                                        ▼                                      ▼
                              [Archivo CMDM generado] ──────carga──────► [FTP Server]
                                        │
                                        ▼
                              [SMTP] ──► Correo modificaciones / error
```

---

##  Tecnologías utilizadas

| Tecnología | Uso |
|---|---|
| **Python 3.x** | Lenguaje principal |
| **Pandas** | Procesamiento y transformación de archivos CSV |
| **pyodbc** | Conexión y consultas a SQL Server |
| **ftplib** (stdlib) | Integración con servidor FTP |
| **smtplib** (stdlib) | Envío de notificaciones por correo SMTP |
| **python-dotenv** | Gestión segura de variables de entorno |
| **openpyxl** | Soporte para archivos Excel si se requiere |
| **PyInstaller** | Empaquetado como ejecutable `.exe` para despliegue en producción |

---

##  Instalación y configuración

### 1. Clonar el repositorio

```bash
git clone https://github.com/berbelmercado/ref-modificar-archivo-CMDM-.git
cd ref-modificar-archivo-CMDM-
```

### 2. Crear entorno virtual e instalar dependencias

```bash
python -m venv venv
venv\Scripts\activate        # Windows
pip install -r requeriments.txt
```

### 3. Configurar variables de entorno

Crea un archivo `.env` en la raíz del proyecto basándote en la plantilla:

```env
# SQL Server
DB_SERVER=tu_servidor
DB_NAME=tu_base_de_datos
DB_USER=tu_usuario
DB_PASSWORD=tu_contraseña

# FTP
FTP_HOST=ftp.servidor.com
FTP_USER=usuario_ftp
FTP_PASSWORD=contraseña_ftp
FTP_RUTA_REMOTA=/ruta/en/ftp/

# SMTP / Correo
SMTP_HOST=smtp.servidor.com
SMTP_PORT=587
SMTP_USER=correo@empresa.com
SMTP_PASSWORD=contraseña_correo
CORREO_DESTINO=destinatario@empresa.com
```

>  **Nunca subas el archivo `.env` al repositorio.** Está incluido en `.gitignore`.

### 4. Ejecutar el sistema

```bash
python main.py
```

---

##  Requisitos previos

- Python 3.10+
- Acceso a SQL Server con las tablas de reglas de negocio configuradas
- Credenciales FTP válidas con permisos de lectura, escritura y eliminación
- Cuenta SMTP habilitada para envío de correos automáticos
- Windows (para despliegue como `.exe` con PyInstaller)

---

##  Notas de despliegue

Este sistema fue diseñado para ejecutarse como tarea programada en un servidor Windows. El ejecutable generado con **PyInstaller** permite desplegarlo sin necesidad de tener Python instalado en el servidor de producción.

Para generar el ejecutable:

```bash
pyinstaller --onefile main.py
```

---

## 👤 Autor

**Mauricio Berbel Mercado**  
Técnico de Sistemas de Información — Positivo S+  
[LinkedIn](https://linkedin.com/in/tu-perfil) · [GitHub](https://github.com/berbelmercado)
