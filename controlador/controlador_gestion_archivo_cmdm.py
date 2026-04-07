"""
Módulo controlador_gestion_archivo_cmdm.py

Este módulo define la clase ControladorGestionArchivoCmdm, que gestiona el flujo principal de procesamiento de archivos CMDM, integración con la base de datos, generación de archivos para correo, actualización de estados y registro de logs.

Clases:
-------
ControladorGestionArchivoCmdm
    - Encapsula la lógica de negocio para procesar archivos CMDM, consultar y actualizar información en la base de datos, preparar archivos para envío por correo y registrar eventos en logs.

Atributos principales:
----------------------
- __ruta_archivo_cmdm: Ruta del archivo CMDM a procesar.
- __columnas_archivo_cmdm: Lista de columnas esperadas en el archivo CMDM.
- __ruta_archivo_correo: Ruta donde se genera el archivo para correo.
- __ruta_archivo_backup: Ruta donde se almacena el backup del archivo CMDM.
- __obj_procesar_archivo: Instancia de ProcesarArchivo para operaciones de procesamiento.
- __dataframe_servicio_publico: DataFrame con información de vehículos de servicio público.
- __dataframe: DataFrame principal con la información del archivo CMDM.
- __dataframe_file_cmdm: DataFrame con la información final para el archivo CMDM.
- __dataframe_vin_no_dda: DataFrame con VINs que no tienen entrega en DDA.
- __dataframe_vin_dda: DataFrame con VINs que sí tienen entrega en DDA.
- __df_reenvios: DataFrame con información de reenvíos.

Métodos:
--------
- fn_gestion_archivo(self):
    Ejecuta el flujo completo de procesamiento del archivo CMDM:
    - Valida existencia y tamaño del archivo.
    - Lee y trata datos nulos.
    - Consulta reporte DDA y separa VINs entregados/no entregados.
    - Inserta VINs no entregados en la tabla delta_cmdm_file.
    - Consulta y actualiza estados en la base de datos.
    - Fusiona DataFrames y actualiza fechas de entrega.
    - Consulta y actualiza vehículos de servicio público.
    - Consulta y procesa reenvíos.
    - Modifica columna HO según acuerdos y tipo de vehículo.
    - Prepara información para correo y genera archivo Excel.
    - Elimina vehículos de servicio público del archivo final.
    - Genera archivo CMDM y backup.
    - Registra eventos y errores en el log.

- fn_cargar_data_cmdm(self):
    Ejecuta el flujo de carga de datos CMDM desde la tabla delta_cmdm_file:
    - Consulta datos procesados.
    - Actualiza estados y fechas de entrega.
    - Consulta y actualiza vehículos de servicio público.
    - Procesa reenvíos y actualiza estados.
    - Modifica columna HO y prepara información para correo.
    - Genera archivo Excel y CMDM final.
    - Registra eventos y errores en el log.

Dependencias:
-------------
- config: Módulo de configuración con rutas y columnas.
- pandas: Manipulación de DataFrames.
- ProcesarArchivo: Clase para procesamiento de archivos y operaciones de base de datos.
- correo_modificacion_encuestas: Función para envío de correos de modificaciones.
- crea_log: Función para registrar eventos en log.

Notas:
------
- Todas las operaciones críticas registran eventos en el log para trazabilidad.
- El controlador maneja errores y retorna diccionarios con el estado de la operación.
- El flujo está diseñado para ser robusto ante archivos vacíos, errores de consulta y problemas de actualización en la base de datos.

"""

import config
import pandas as pd
from modelo.procesar_archivo import ProcesarArchivo
from vista.crear_log import crea_log


class ControladorGestionArchivoCmdm:

    def __init__(self):
        self.__ruta_archivo_cmdm = config.RUTA_GUARDAR_ARCHIVO
        self.__columnas_archivo_cmdm = config.COLUMNA_ARCHIVO_CMDM
        self.__ruta_archivo_correo = (
            config.RUTA_ARCHIVO_CORREO + config.NOMBRE_ARCHIVO_CORREO
        )
        self.__ruta_archivo_backup = config.RUTA_ARCHIVO_BACUP
        self.__obj = ProcesarArchivo()

    # ==================================================
    #                PIPELINE PRINCIPAL
    # ==================================================
    def fn_gestion_archivo(self):

        contexto = {}

        pasos = [
            self._validar_archivo,
            self._leer_archivo_si_existe,
            self._tratar_datos,
            self._consultar_reporte_dda,
            self._separar_vines,
            self._insertar_no_dda,
            self._consultar_delta,
            self._actualizar_delta,
            self._fusionar_data,
            self._consultar_fechas_dda,
            self._aplicar_fechas,
            self._consultar_servicio_publico,
            self._consultar_reenvios,
            self._modificar_ho,
            self._preparar_info_email,
            self._consultar_info_email,
            self._generar_excel,
            self._eliminar_publicos,
            self._generar_cmdm,
            self._generar_backup,
        ]

        for paso in pasos:
            res = paso(contexto)
            if not res["ok"]:
                crea_log(f"Error en {paso.__name__}: {res['error']}")
                return {"error": True, "tamano": False}

        return {"error": False, "tamano": True}

    # ==================================================
    #            DEFINICIÓN DE CADA ETAPA
    # ==================================================

    def _validar_archivo(self, ctx):
        ctx["archivo_tiene_contenido"] = self.__obj.archivo_vacio(
            self.__ruta_archivo_cmdm
        )
        return {"ok": True}

    def _leer_archivo_si_existe(self, ctx):
        if not ctx["archivo_tiene_contenido"]:
            ctx["df"] = pd.DataFrame(columns=self.__columnas_archivo_cmdm)
            return {"ok": True}

        res = self.__obj.fn_leer_archivo(self.__ruta_archivo_cmdm)
        if not res["exito"]:
            return {"ok": False, "error": "No se pudo leer archivo CMDM"}

        ctx["df"] = res["data"]
        return {"ok": True}

    def _tratar_datos(self, ctx):
        if ctx["df"].empty:
            return {"ok": True}

        ctx["df"] = self.__obj.fn_tratar_datos_nulos(ctx["df"])
        return {"ok": True}

    def _consultar_reporte_dda(self, ctx):
        lista_vin = ctx["df"]["SDI_VHCL.VIN"].tolist() if not ctx["df"].empty else []

        res = self.__obj.consultar_reporte_dda(lista_vin)
        if not res["exito"]:
            return {"ok": False, "error": res["error"]}

        ctx["vin_dda"] = res["data"]
        return {"ok": True}

    def _separar_vines(self, ctx):

        df = ctx["df"]

        if df.empty:
            ctx["df_no_dda"] = pd.DataFrame()
            ctx["df_dda"] = pd.DataFrame()
            return {"ok": True}

        df_no_dda, df_dda = self.__obj.fn_separar_vin(ctx["vin_dda"], df)

        ctx["df_no_dda"] = df_no_dda
        ctx["df_dda"] = df_dda
        return {"ok": True}

    def _insertar_no_dda(self, ctx):
        if ctx["df_no_dda"].empty:
            return {"ok": True}

        r = self.__obj.fn_insertar_data_delta_cmdm(ctx["df_no_dda"])
        if not r["exito"]:
            return {"ok": False, "error": r["error"]}

        return {"ok": True}

    def _consultar_delta(self, ctx):
        r = self.__obj.fn_consultar_data_delta_cmdm(self.__columnas_archivo_cmdm)
        if not r["exito"]:
            return {"ok": False, "error": r["error"]}

        ctx["df_delta"] = r["data"]
        return {"ok": True}

    def _actualizar_delta(self, ctx):
        if ctx["df_delta"].empty:
            return {"ok": True}

        lista_vin = ctx["df_delta"]["SDI_VHCL.VIN"].tolist()
        r = self.__obj.fn_actualizar_estado_delta(lista_vin)

        if not r["exito"]:
            return {"ok": False, "error": r["error"]}

        return {"ok": True}

    def _fusionar_data(self, ctx):
        df_total = self.__obj.fn_fusionar_dataframes(ctx["df_dda"], ctx["df_delta"])
        ctx["df_final"] = df_total
        return {"ok": True}

    def _consultar_fechas_dda(self, ctx):
        if ctx["df_final"].empty:
            ctx["df_fechas"] = pd.DataFrame()
            return {"ok": True}

        lista_vin = ctx["df_final"]["SDI_VHCL.VIN"].tolist()
        r = self.__obj.fn_consultar_fechas_dda_vin(lista_vin)

        if not r["exito"]:
            return {"ok": False, "error": r["error"]}

        ctx["df_fechas"] = r["data"]
        return {"ok": True}

    def _aplicar_fechas(self, ctx):
        if ctx["df_final"].empty:
            return {"ok": True}

        df = self.__obj.fn_fusionar_dataframes_merge(ctx["df_final"], ctx["df_fechas"])
        df = self.__obj.fn_actualizar_fechas_archivo(df)

        ctx["df_final"] = df
        return {"ok": True}

    def _consultar_servicio_publico(self, ctx):
        r = self.__obj.fn_consultar_data_servicio_publico()
        if not r["exito"]:
            return {"ok": False, "error": r["error"]}

        ctx["df_publicos"] = r["data"]
        return {"ok": True}

    def _consultar_reenvios(self, ctx):
        r = self.__obj.fn_consultar_reenvios(self.__columnas_archivo_cmdm)
        if not r["exito"]:
            return {"ok": False, "error": r["error"]}

        ctx["df_reenvios"] = r["data"]

        if not ctx["df_reenvios"].empty:
            ctx["df_final"] = self.__obj.fn_fusionar_dataframes(
                ctx["df_final"], ctx["df_reenvios"]
            )

        return {"ok": True}

    def _modificar_ho(self, ctx):
        df, df_mod = self.__obj.fn_mod_col_ho(ctx["df_final"])
        ctx["df_final"] = df
        ctx["df_mod_ho"] = df_mod
        return {"ok": True}

    def _preparar_info_email(self, ctx):

        df_email = self.__obj.fn_prep_info_email(
            dataframe_vin_no_dda=ctx["df_no_dda"],
            dataframe_vin_dda_email=ctx["df_dda"],
            dataframe_lista_cmdm=ctx["df_delta"],
            dataframe_reenvios_cmdm=ctx["df_reenvios"],
            dataframe_servicio_publico=ctx["df_publicos"],
        )

        ctx["df_email_pre"] = df_email
        return {"ok": True}

    def _consultar_info_email(self, ctx):
        lista_vin = ctx["df_email_pre"]["SDI_VHCL.VIN"].tolist()
        r = self.__obj.fn_consul_info_email(lista_vin)

        if not r["exito"]:
            return {"ok": False, "error": r["error"]}

        df = self.__obj.fn_fusionar_dataframes_merge(r["data"], ctx["df_email_pre"])
        df = self.__obj.fn_columna_ho_email(df, ctx["df_mod_ho"])

        ctx["df_email_final"] = df
        return {"ok": True}

    def _generar_excel(self, ctx):
        self.__obj.fn_generar_archivo_ecxel(
            ctx["df_email_final"], self.__ruta_archivo_correo
        )
        return {"ok": True}

    def _eliminar_publicos(self, ctx):
        ctx["df_final"] = self.__obj.fn_eliminar_pub_cmdm(ctx["df_final"])
        return {"ok": True}

    def _generar_cmdm(self, ctx):
        self.__obj.fn_generar_archivo_cmdm(ctx["df_final"], self.__ruta_archivo_cmdm)
        return {"ok": True}

    def _generar_backup(self, ctx):
        if not ctx["df"].empty:
            self.__obj.fn_generar_backup_archivo_cmdm(
                ctx["df"], self.__ruta_archivo_backup
            )

        return {"ok": True}
