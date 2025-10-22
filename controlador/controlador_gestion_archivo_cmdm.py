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
from vista.envio_correo_modificaciones import  correo_modificacion_encuestas

from vista.crear_log import crea_log

class ControladorGestionArchivoCmdm():
    """
    Clase principal para la gestión y procesamiento de archivos CMDM.

    Métodos:
    --------
    - fn_gestion_archivo: Ejecuta el flujo completo de procesamiento y actualización de datos CMDM.
    - fn_cargar_data_cmdm: Ejecuta el flujo de carga y actualización de datos CMDM desde la base de datos.
    """
    def __init__(self):
        """
        Inicializa las rutas, columnas y DataFrames necesarios para el procesamiento.
        """
        self.__ruta_archivo_cmdm = config.RUTA_GUARDAR_ARCHIVO
        self.__columnas_archivo_cmdm = config.COLUMNA_ARCHIVO_CMDM
        self.__ruta_archivo_correo = config.RUTA_ARCHIVO_CORREO+config.NOMBRE_ARCHIVO_CORREO
        self.__ruta_archivo_backup = config.RUTA_ARCHIVO_BACUP
        self.__obj_procesar_archivo = ProcesarArchivo()
        #Declaración de dataframes
        self.__dataframe_servicio_publico = pd.DataFrame(columns = ['SDI_VHCL.VIN'])
        self.__dataframe = pd.DataFrame()
        self.__dataframe_file_cmdm = pd.DataFrame(columns=config.COLUMNA_ARCHIVO_CMDM)
        self.__dataframe_vin_no_dda = pd.DataFrame()
        self.__dataframe_vin_dda = pd.DataFrame()
        self.__df_reenvios = pd.DataFrame()

    def fn_gestion_archivo(self):
        """
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

        Retorna:
        --------
        dict: Estado de la operación con claves 'error' y 'tamano'.
        """
        #Validamos si el archivo existe y no está vacío
        if self.__obj_procesar_archivo.archivo_vacio(self.__ruta_archivo_cmdm):
            #Leemos el archivo y retornamos un dataframe
            dic_retorno_lectura_archivo = self.__obj_procesar_archivo.fn_leer_archivo(self.__ruta_archivo_cmdm)

            #Si el archivo se leyó correctamente, procesamos los datos
            if dic_retorno_lectura_archivo['exito']:

                #Dataframe con toda la información en el archivo CMDM
                self.__dataframe = dic_retorno_lectura_archivo['data']

                #Procesamos los datos del dataframe convertimos los NaN a campos vacíos
                self.__dataframe = self.__obj_procesar_archivo.fn_tratar_datos_nulos(self.__dataframe)

                #Consultamos el reporte DDA con los VINs filtrados
                dic_retorno_reporte_dda = self.__obj_procesar_archivo.consultar_reporte_dda(
                    self.__dataframe['SDI_VHCL.VIN'].tolist())

                #Si se consulta la información correctamente procedemos a seleccionar la información
                if dic_retorno_reporte_dda['exito']:

                    lista_vin_dda = dic_retorno_reporte_dda['data']

                    #Separamos los VIN que están en DDA y los que no
                    self.__dataframe_vin_no_dda, self.__dataframe_vin_dda = self.__obj_procesar_archivo.fn_separar_vin(
                        lista_vin_dda, self.__dataframe)

                    #Si hay datos que no están en dda procedemos a insertar la información en la tabla delta_cmdm
                    if not self.__dataframe_vin_no_dda.empty:

                        #Insertamos los VINs que no están en DDA en la tabla delta_cmdm_file
                        dic_retorno_insercion_delta = self.__obj_procesar_archivo.fn_insertar_data_delta_cmdm(self.__dataframe_vin_no_dda)

                        #Validamos si se inserta o no correctamente la información
                        if dic_retorno_insercion_delta['exito']:

                            #Consultamos la data de la tabla delta_cmdm_file que ya cuentan con entrega en DDA
                            dic_retorno_consulta_delta = self.__obj_procesar_archivo.fn_consultar_data_delta_cmdm(
                                self.__columnas_archivo_cmdm)

                            if dic_retorno_consulta_delta['exito']:

                                dataframe_db_dda = dic_retorno_consulta_delta['data']

                                if not dataframe_db_dda.empty:
                                    #Actualizamos el estado de los VINs que ya cuentan con entrega en DDA en la tabla delta_cmdm_file
                                    dic_retorno_actualizacion_delta = self.__obj_procesar_archivo.fn_actualizar_estado_delta(
                                        dataframe_db_dda['SDI_VHCL.VIN'].tolist())
                                
                                    #Si la actualización fue no fue exitosa realizamos creamos log y retornamos error
                                    if not dic_retorno_actualizacion_delta['exito']:
                                        crea_log(f'Error - Error al actualizar estado de VINs en delta_cmdm_file {dic_retorno_actualizacion_delta["error"]}')
                                        return {'error':False, 'tamano':False}

                                #de los VIN que si tienen entrega dda y los que se reprocesan
                                self.__dataframe_file_cmdm = self.__obj_procesar_archivo.fn_fusionar_dataframes(
                                    self.__dataframe_vin_dda
                                    ,dataframe_db_dda)

                                #Si hay datos con entregas en DDA se consultan las fechas
                                if not self.__dataframe_file_cmdm.empty:
                                    #Consultamos las fechas de entrega de los VINs en DDA
                                    dic_retorno_fecha_dda = self.__obj_procesar_archivo.fn_consultar_fechas_dda_vin(
                                        self.__dataframe_file_cmdm['SDI_VHCL.VIN'].tolist())

                                    if dic_retorno_fecha_dda['exito']:
                                        #Hacemos merge de los dos dataframes para agregar las fechas DDA
                                        self.__dataframe_file_cmdm = self.__obj_procesar_archivo.fn_fusionar_dataframes_merge(
                                            self.__dataframe_file_cmdm,
                                            dic_retorno_fecha_dda['data'])

                                        #Modificamos las fechas de entrega en extranet por las fechas de entrega en DDA
                                        self.__dataframe_file_cmdm = self.__obj_procesar_archivo.fn_actualizar_fechas_archivo(
                                            self.__dataframe_file_cmdm)
                                    else:
                                        crea_log (f'Error- error al consultar fechas de entrega DDA: {dic_retorno_fecha_dda["error"]}')
                                        return {'error':False, 'tamano':False}

                                #Consultamos los vehículos con servicio público almacenados en la tabla delta
                                dict_retorno_data_publicos = self.__obj_procesar_archivo.fn_consultar_data_servicio_publico()

                                if dict_retorno_data_publicos['exito']:
                                    #Guardamos la información en el dataframe
                                    self.__dataframe_servicio_publico = dict_retorno_data_publicos['data']
                                    #Si el dataframe no está vacío actualizamos el estado en tabla delta cmdm
                                    if not self.__dataframe_servicio_publico.empty:
                                        dic_retorno_actualizacion_delta = self.__obj_procesar_archivo.fn_actualizar_estado_delta(
                                        self.__dataframe_servicio_publico['SDI_VHCL.VIN'].tolist())

                                        if not dic_retorno_actualizacion_delta['exito']:
                                            crea_log('Error al actualizar estado de vehículos públicos')
                                            return {'error':False,'tamano':False}
                                else:
                                    crea_log(f'Error al consultar vehículos públicos con entrega en DDA {dict_retorno_data_publicos['error']}')
                                    return {'error':False,'tamano':False}

                                #Consultamos los reenvíos
                                dic_retorno_reenvio = self.__obj_procesar_archivo.fn_consultar_reenvios(
                                    self.__columnas_archivo_cmdm)

                                #Fusionamos el df de los VIN incluidos en el archivo con los reenvíos
                                if dic_retorno_reenvio['exito']:

                                    #Guardamos la información de la bd en la variable df_reenvios
                                    self.__df_reenvios = dic_retorno_reenvio['data']

                                    if not self.__df_reenvios.empty:
                                        #Incluimos los reenvíos en el dataframe de los datos que van a quedar en el archivo CMDM
                                        self.__dataframe_file_cmdm = self.__obj_procesar_archivo.fn_fusionar_dataframes(
                                            self.__dataframe_file_cmdm
                                            ,self.__df_reenvios)

                                        #Actualizamos el estado para que no se repitan reenvíos
                                        dic_retorno_actualizacion_delta = self.__obj_procesar_archivo.fn_actualizar_estado_delta(
                                            self.__df_reenvios['SDI_VHCL.VIN'].tolist())

                                        if not dic_retorno_actualizacion_delta['exito']:
                                            crea_log(f'Error - Error al actualizar estado de VINs en delta_cmdm_file reenvios {dic_retorno_actualizacion_delta["error"]}')
                                            return {'error':False, 'tamano':False}
                                    
                                    #Modificamos la columna HO para los VIN con acuerdos en si y vehiculos particulares
                                    self.__dataframe_file_cmdm,dataframe_vin_modificados = self.__obj_procesar_archivo.fn_mod_col_ho(
                                        self.__dataframe_file_cmdm)
                                    #preparamos el archivo para correo y agregamos columna Entrega_dda con sus valores
                                    df_entrega_dda = self.__obj_procesar_archivo.fn_prep_info_email(self.__dataframe_vin_no_dda
                                                                                            ,self.__dataframe_vin_dda
                                                                                            ,dataframe_db_dda
                                                                                            ,self.__df_reenvios
                                                                                            ,self.__dataframe_servicio_publico )

                                    dic_retorno_email = self.__obj_procesar_archivo.fn_consul_info_email(
                                        df_entrega_dda['SDI_VHCL.VIN'].tolist())

                                    if dic_retorno_email['exito']:
                                        #Fusionamos datarame con la información de la bd con las columnas de entrega dda para tener una columnas adicional
                                        df_archivo_email = self.__obj_procesar_archivo.fn_fusionar_dataframes_merge(
                                            dic_retorno_email['data']
                                            ,df_entrega_dda
                                        )

                                        #Unimos dataframe consulta con los dataframes de preparación de correo
                                        df_archivo_email = self.__obj_procesar_archivo.fn_columna_ho_email(
                                            df_archivo_email
                                            ,dataframe_vin_modificados
                                            )

                                        #Generamos archivo de excel 
                                        self.__obj_procesar_archivo.fn_generar_archivo_ecxel(df_archivo_email
                                                                                            ,self.__ruta_archivo_correo)
                                        
                                        #Eliminamos los vehículos con servicio público del archivo cmdm
                                        self.__dataframe_file_cmdm = self.__obj_procesar_archivo.fn_eliminar_pub_cmdm(self.__dataframe_file_cmdm )

                                        #Generamos archivo CMDM
                                        self.__obj_procesar_archivo.fn_generar_archivo_cmdm(self.__dataframe_file_cmdm
                                                                                            ,self.__ruta_archivo_cmdm)
                                        
                                        #Creamos backup para archivo cmdm
                                        self.__obj_procesar_archivo.fn_generar_backup_archivo_cmdm(self.__dataframe
                                                            ,self.__ruta_archivo_backup)
                                        crea_log('Se modifica correctamente el archivo CMDM')

                                        return {'error':True, 'tamano':False}

                                    else:
                                        crea_log(f'Error al consultar información para email: {dic_retorno_email['error']}')
                                        return {'error':False, 'tamano':False}

                                else:
                                    #Si no hay reenvios o si ocurre un error en la consulta creamos log
                                    crea_log(f'Reenvio: {dic_retorno_reenvio['error']}')
                                    return {'error':False, 'tamano':False}
                            else:
                                crea_log (f'Error- error al consultar data almacenada en tabla delta cmdm {dic_retorno_consulta_delta["error"]}')
                                return {'error':False, 'tamano':False}
                        else:
                            crea_log(f"""Error - Error al insertar información de la data que no esta en DDA fn_insertar_data_delta_cmdm:
                                      {dic_retorno_insercion_delta["error"]}""")
                            return {'error':False, 'tamano':False}
                else:
                    crea_log(f'Error- error al consultar reporte DDA: {dic_retorno_reporte_dda["error"]}')
                    return {'error':False, 'tamano':False}
            else:
                crea_log('Error - No se pudo leer el archivo')
                return {'error':False, 'tamano':False}
        else:
            crea_log('Archivo cmdm vacio')
            return {'error':False,'tamano':True}

    def fn_cargar_data_cmdm(self):
        """
        Ejecuta el flujo de carga de datos CMDM desde la tabla delta_cmdm_file:
        - Consulta datos procesados.
        - Actualiza estados y fechas de entrega.
        - Consulta y actualiza vehículos de servicio público.
        - Procesa reenvíos y actualiza estados.
        - Modifica columna HO y prepara información para correo.
        - Genera archivo Excel y CMDM final.
        - Registra eventos y errores en el log.

        Retorna:
        --------
        dict: Estado de la operación con clave 'error'.
        """
        #Consultamos si hay vin con entrega en CMDM en la tabla delta_cmdm_file
        dic_retorno_consulta_delta = self.__obj_procesar_archivo.fn_consultar_data_delta_cmdm(
            self.__columnas_archivo_cmdm)

        if dic_retorno_consulta_delta['exito']:

            self.__dataframe_file_cmdm = dic_retorno_consulta_delta['data']

            #Si hay procesados
            if not self.__dataframe_file_cmdm.empty:
                #Actualizamos el estado de los VINs que ya cuentan con entrega en DDA en la tabla delta_cmdm_file
                dic_retorno_actualizacion_delta = self.__obj_procesar_archivo.fn_actualizar_estado_delta(
                    self.__dataframe_file_cmdm['SDI_VHCL.VIN'].tolist())

                if not dic_retorno_actualizacion_delta['exito']:
                    crea_log(f'Error - Error al actualizar estado de VINs en delta_cmdm_file {dic_retorno_actualizacion_delta["error"]}')
                    return {'error':False}

                #Consultamos las fechas de entrega de los VINs en DDA
                dic_retorno_fecha_dda = self.__obj_procesar_archivo.fn_consultar_fechas_dda_vin(
                    self.__dataframe_file_cmdm['SDI_VHCL.VIN'].tolist())

                if dic_retorno_fecha_dda['exito']:
                    #Hacemos merge de los dos dataframes para agregar las fechas DDA
                    self.__dataframe_file_cmdm = self.__obj_procesar_archivo.fn_fusionar_dataframes_merge(
                        self.__dataframe_file_cmdm,
                        dic_retorno_fecha_dda['data'])

                    #Modificamos las fechas de entrega en extranet por las fechas de entrega en DDA
                    self.__dataframe_file_cmdm = self.__obj_procesar_archivo.fn_actualizar_fechas_archivo(
                        self.__dataframe_file_cmdm)
                else:
                    crea_log (f'Error- error al consultar fechas de entrega DDA: {dic_retorno_fecha_dda["error"]}')
                    return {'error':False}

                #Consultamos los vehículos con servicio público almacenados en la tabla delta
                dict_retorno_data_publicos = self.__obj_procesar_archivo.fn_consultar_data_servicio_publico()

                if dict_retorno_data_publicos['exito']:
                    #Guardamos la información en el dataframe
                    self.__dataframe_servicio_publico = dict_retorno_data_publicos['data']
                    #Si el dataframe no está vacío actualizamos el estado en tabla delta cmdm
                    if not self.__dataframe_servicio_publico.empty:
                        dic_retorno_actualizacion_delta = self.__obj_procesar_archivo.fn_actualizar_estado_delta(
                        self.__dataframe_servicio_publico['SDI_VHCL.VIN'].tolist())

                        if not dic_retorno_actualizacion_delta['exito']:
                            crea_log('Error al actualizar estado de vehículos públicos')
                            return {'error':False}
                else:
                    crea_log(f'Error al consultar vehículos públicos con entrega en DDA {dict_retorno_data_publicos['error']}')
                    return {'error':False}

            #Consultamos los reenvíos
            dic_retorno_reenvio = self.__obj_procesar_archivo.fn_consultar_reenvios(
                self.__columnas_archivo_cmdm)

            #Fusionamos el df de los VIN incluidos en el archivo con los reenvíos
            if dic_retorno_reenvio['exito']:

                #Guardamos la información de la bd en la variable df_reenvios
                self.__df_reenvios = dic_retorno_reenvio['data']

                if not self.__df_reenvios.empty:
                    #Incluimos los reenvíos en el dataframe de los datos que van a quedar en el archivo CMDM
                    self.__dataframe_file_cmdm = self.__obj_procesar_archivo.fn_fusionar_dataframes(
                        self.__dataframe_file_cmdm
                        ,self.__df_reenvios)

                    #Actualizamos el estado para que no se repitan reenvíos
                    dic_retorno_actualizacion_delta = self.__obj_procesar_archivo.fn_actualizar_estado_delta(
                        self.__df_reenvios['SDI_VHCL.VIN'].tolist())

                    if not dic_retorno_actualizacion_delta['exito']:
                        crea_log(f'Error - Error al actualizar estado de VINs en delta_cmdm_file reenvios {dic_retorno_actualizacion_delta["error"]}')
                        return {'error':False}

                #Modificamos la columna HO para los VIN con acuerdos en si y vehiculos particulares
                self.__dataframe_file_cmdm,dataframe_vin_modificados = self.__obj_procesar_archivo.fn_mod_col_ho(
                    self.__dataframe_file_cmdm)

                #preparamos el archivo para correo y agregamos columna Entrega_dda con sus valores
                df_entrega_dda = self.__obj_procesar_archivo.fn_prep_info_email(
                                                                        dataframe_lista_cmdm= self.__dataframe_file_cmdm
                                                                        ,dataframe_reenvios_cmdm = self.__df_reenvios
                                                                        ,dataframe_servicio_publico= self.__dataframe_servicio_publico )

                dic_retorno_email = self.__obj_procesar_archivo.fn_consul_info_email(
                    df_entrega_dda['SDI_VHCL.VIN'].tolist())

                if dic_retorno_email['exito']:
                    #Fusionamos datarame con la información de la bd con las columnas de entrega dda para tener una columnas adicional
                    df_archivo_email = self.__obj_procesar_archivo.fn_fusionar_dataframes_merge(
                        dic_retorno_email['data']
                        ,df_entrega_dda
                    )

                    #Unimos dataframe consulta con los dataframes de preparación de correo
                    df_archivo_email = self.__obj_procesar_archivo.fn_columna_ho_email(
                        df_archivo_email
                        ,dataframe_vin_modificados
                        )

                    #Generamos archivo de excel 
                    self.__obj_procesar_archivo.fn_generar_archivo_ecxel(df_archivo_email
                                                                        ,self.__ruta_archivo_correo)

                    #Eliminamos los vehículos con servicio público del archivo cmdm
                    self.__dataframe_file_cmdm = self.__obj_procesar_archivo.fn_eliminar_pub_cmdm(self.__dataframe_file_cmdm )

                    #Generamos archivo CMDM
                    self.__obj_procesar_archivo.fn_generar_archivo_cmdm(self.__dataframe_file_cmdm
                                                                        ,self.__ruta_archivo_cmdm)

                    crea_log('Se crea correctamente el archivo CMDM con información de la tabla delta')
                    return {'error':True}
                else:
                    crea_log(f'Error al consultar información para email: {dic_retorno_email['error']}')
                    return {'error':False}

            else:
                #Si no hay reenvios o si ocurre un error en la consulta creamos log
                crea_log(f'Reenvio: {dic_retorno_reenvio['error']}')
                return {'error':False}
        else:
            crea_log (f'Error- error al consultar data almacenada en tabla delta cmdm {dic_retorno_consulta_delta["error"]}')
            return {'error':False}
