"""
Módulo procesar_archivo.py

Este módulo define la clase ProcesarArchivo, que encapsula la lógica para el procesamiento de archivos CMDM, manipulación de DataFrames, interacción con la base de datos SQL Server y generación de archivos para reportes y respaldo.

Clases:
-------
ProcesarArchivo
    - Proporciona métodos para leer, tratar, separar, fusionar y modificar datos de archivos CMDM.
    - Integra consultas y actualizaciones en la base de datos relacionadas con VINs, reportes DDA, reenvíos y servicio público.
    - Permite la generación de archivos Excel, CSV y backups, así como la preparación de información para envío de correos.

Dependencias:
-------------
- pandas: Manipulación de DataFrames.
- datetime: Manejo de fechas y horas.
- ConsultasSql: Clase para operaciones con la base de datos.
- resource_path: Función para resolver rutas de archivos.

Atributos:
----------
- __obj_consultas_sql: Instancia de ConsultasSql para operaciones de base de datos.

Métodos:
--------
- archivo_vacio(ruta_archivo): Verifica si el archivo está vacío.
- fn_leer_archivo(ruta_archivo): Lee el archivo CSV y retorna un DataFrame.
- fn_tratar_datos_nulos(dataframe): Trata valores nulos y normaliza columnas específicas.
- consultar_reporte_dda(lista_vin): Consulta VINs entregados en DDA en la base de datos.
- fn_separar_vin(Lista_vin_dda, dataframe): Separa VINs entregados y no entregados en DDA.
- fn_insertar_data_delta_cmdm(dataframe_vin_no_dda): Inserta VINs no entregados en DDA en la base de datos.
- fn_consultar_data_delta_cmdm(columnas): Consulta datos de la tabla delta_cmdm_file.
- fn_actualizar_estado_delta(lista_vin): Actualiza el estado de los VINs en la tabla delta_cmdm_file.
- fn_fusionar_dataframes(dataframe1, dataframe2): Fusiona dos DataFrames por concatenación.
- fn_mod_col_ho(dataframe): Modifica la columna de acuerdo de encuesta y retorna los VIN modificados.
- fn_consultar_fechas_dda_vin(lista_vin_dda_fecha): Consulta fechas de entrega DDA para una lista de VINs.
- fn_fusionar_dataframes_merge(dataframe1, dataframe2): Fusiona dos DataFrames por la columna VIN.
- fn_actualizar_fechas_archivo(dataframe_vin_dda): Actualiza fechas en el DataFrame según información de entrega.
- fn_consultar_reenvios(columnas): Consulta VINs marcados para reenvío.
- fn_prep_info_email(...): Prepara un DataFrame con información de VIN y estado de entrega DDA para correo.
- fn_consul_info_email(lista_vin_email): Consulta información detallada de VINs para envío de correos.
- fn_columna_ho_email(dataframe_email, lista_vin_ho_si): Agrega columna indicando si hubo cambio HO.
- fn_generar_archivo_ecxel(df_email, ruta_excel): Genera archivo Excel con la información de correo.
- fn_generar_archivo_cmdm(dataframe_file_cmdm, ruta_csv_cmdm): Genera archivo CSV CMDM final.
- fn_generar_backup_archivo_cmdm(dataframe, ruta_backup): Genera archivo backup del CMDM con fecha y hora.
- fn_consultar_data_servicio_publico(): Consulta VINs de servicio público entregados en DDA.
- fn_eliminar_pub_cmdm(dataframe): Elimina registros de vehículos de servicio público del DataFrame.

Notas:
------
- Todos los métodos que interactúan con la base de datos gestionan la conexión y desconexión automáticamente.
- El módulo está diseñado para ser utilizado en el flujo principal de procesamiento y generación de reportes CMDM.
- Los métodos devuelven diccionarios con claves 'exito', 'data' y 'error' para facilitar el manejo de resultados y errores.

"""
from servicios.resolver_rutas import resource_path
from os import path
from modelo.consultas_sql import ConsultasSql
import pandas as pd
from datetime import datetime

class ProcesarArchivo():
    """
    Clase para el procesamiento de archivos CMDM y gestión de datos relacionados con VINs y reportes DDA.
    """
    def __init__(self):
        self.__obj_consultas_sql = ConsultasSql()

    def archivo_vacio(self,ruta_archivo):
        """
        Verifica si el archivo está vacío.

        Parameters:
        -----------
        ruta_archivo : str
            Ruta del archivo a verificar.

        Returns:
        --------
        bool: True si el archivo tiene contenido, False si está vacío.
        """
        if path.getsize(ruta_archivo) == 0:
            return False
        else:
            return True

    def fn_leer_archivo(self,ruta_archivo):
        """
        Lee el archivo CSV y retorna un DataFrame con una columna adicional 'ESTADO'.

        Parameters:
        -----------
        ruta_archivo : str
            Ruta del archivo CSV.

        Returns:
        --------
        dict: {'exito': True, 'data': dataframe} o {'exito': False, 'data': None}
        """
        try:
            dataframe = pd.read_csv(ruta_archivo
                                    ,sep = ';')

            dataframe['ESTADO']='false'#agregaoms una nueva columna llamada 'ESTADO'

            return {'exito':True
                    ,'data':dataframe
                    }

        except:
            return {'exito':False
                    ,'data':None}
        
    def fn_tratar_datos_nulos(self,dataframe):
        """
        Trata valores nulos en columnas específicas y normaliza los datos.

        Parameters:
        -----------
        dataframe : pandas.DataFrame

        Returns:
        --------
        pandas.DataFrame: DataFrame con datos tratados.
        """
        for column in dataframe.columns:

            if column == 'SDI_PRTY.PHN_NMBR_1':
                #Convierte los valores de la columna 'SDI_PRTY.PHN_NMBR_1' a enteros si no están vacíos o NaN.
                dataframe["SDI_PRTY.PHN_NMBR_1"] = dataframe["SDI_PRTY.PHN_NMBR_1"].apply(
                                lambda x:'' if pd.isna(x) else int(x))

            if column == 'SDI_PRTY.PHN_NMBR_2':
                dataframe["SDI_PRTY.PHN_NMBR_2"] = dataframe["SDI_PRTY.PHN_NMBR_2"].apply(
                                lambda x:'' if pd.isna(x) else int(x))

            if column == 'SDI_VHCL.DLVRY_DLR_CD':
                #Convierte los valores de la columna 'SDI_VHCL.DLVRY_DLR_CD' a enteros si no están vacíos o NaN.
                dataframe['SDI_VHCL.DLVRY_DLR_CD'] = dataframe['SDI_VHCL.DLVRY_DLR_CD'].apply(lambda x:'' if pd.isna(x) else int(x))

            if column == 'SDI_VHCL.SLLNG_DLR_CD':
                #Convierte los valores de la columna 'SDI_VHCL.SLLNG_DLR_CD' a enteros si no están vacíos o NaN.
                dataframe['SDI_VHCL.SLLNG_DLR_CD'] = dataframe['SDI_VHCL.SLLNG_DLR_CD'].apply(lambda x:'' if pd.isna(x) else int(x))

            dataframe[column] = dataframe[column].apply(lambda x:'' if pd.isna(x) else x)

        return dataframe

    def consultar_reporte_dda(self,lista_vin):
        """
        Consulta VINs entregados en DDA en la base de datos.

        Parameters:
        -----------
        lista_vin : list
            Lista de VINs a consultar.

        Returns:
        --------
        dict: {'exito': True, 'data': Lista_vin_dda, 'error': None} o {'exito': False, ...}
        """
        Lista_vin_dda = []
        estado_conexion, mensaje_error = self.__obj_consultas_sql.conectar_db_conexion()#Conectamos a la base de datos|

        if estado_conexion:

            #Consultamos vin en tabla reporte dda
            dic_consulta_resultado = self.__obj_consultas_sql.fn_consulta_estado_dda(lista_vin)

            if dic_consulta_resultado['exito']:

                Lista_vin_dda = [i[0] for i in dic_consulta_resultado['data']]

                self.__obj_consultas_sql.desconectar()

                return {'exito':True
                        ,'data':Lista_vin_dda
                        ,'error':None
                        }
            else:
                return {'exito':False
                        ,'data':None
                        ,'error':dic_consulta_resultado['error']
                        }

        return {'exito':False
                ,'data':None
                ,'error':mensaje_error
                }

    def fn_separar_vin(self, Lista_vin_dda, dataframe):
        """
        Separa VINs entregados y no entregados en DDA.

        Parameters:
        -----------
        Lista_vin_dda : list
            Lista de VINs entregados en DDA.
        dataframe : pandas.DataFrame

        Returns:
        --------
        tuple: (dataframe_vin_no_dda, dataframe_vin_dda)
        """
        #Vin que No están entregados en DDA
        dataframe_vin_no_dda = dataframe[~dataframe["SDI_VHCL.VIN"].isin(Lista_vin_dda)]
        #Vin que si están entregados en DDA
        dataframe_vin_dda = dataframe[dataframe["SDI_VHCL.VIN"].isin(Lista_vin_dda)]
        return dataframe_vin_no_dda, dataframe_vin_dda

    def fn_insertar_data_delta_cmdm(self, dataframe_vin_no_dda):
        """
        Inserta los datos de los VINs que no están en DDA en la base de datos.

        Parameters:
        -----------
        dataframe_vin_no_dda : pandas.DataFrame

        Returns:
        --------
        dict: {'exito': True} o {'exito': False, 'error': ...}
        """
        estado_conexion, mensaje_error = self.__obj_consultas_sql.conectar_db_conexion()

        if estado_conexion:
            dic_retorno = self.__obj_consultas_sql.fn_insertar_vin_delta_cmdm(dataframe_vin_no_dda)
            self.__obj_consultas_sql.desconectar()

            if dic_retorno['exito']:
                return {'exito': True}
            else:
                return  {'exito': dic_retorno['exito'], 'error': dic_retorno['error']}
        else:
            return {'exito': dic_retorno['exito'], 'error': mensaje_error}
    
    def fn_consultar_data_delta_cmdm(self,columnas):
        """
        Consulta los datos de la tabla delta_cmdm_file.

        Parameters:
        -----------
        columnas : list
            Lista de nombres de columnas para el DataFrame.

        Returns:
        --------
        dict: {'exito': True, 'data': dataframe_lista_cmdm} o {'exito': False, 'error': ...}
        """
        #Conectamos a la base de datos
        estado_conexion, mensaje_error = self.__obj_consultas_sql.conectar_db_conexion()

        if estado_conexion:
            #Validamos si el vin ya cuenta con entrega en DDA
            dic_retorno_delta = self.__obj_consultas_sql.fn_validar_vin_cmdm_dda()
            self.__obj_consultas_sql.desconectar()

            if dic_retorno_delta['exito']:

                lista_vin_dda = [[j for j in i] for i in dic_retorno_delta['data']]

                #Creamos dataframe con la respuesta de la consulta
                dataframe_lista_cmdm = pd.DataFrame(lista_vin_dda
                                                ,columns = columnas)

                return {'exito': True, 'data': dataframe_lista_cmdm}
            else:

                return {'exito': False, 'error': dic_retorno_delta['error']}

        else:
            return {'exito': False, 'error': mensaje_error}

    def fn_actualizar_estado_delta(self, lista_vin):
        """
        Actualiza el estado de los VINs en la tabla delta_cmdm_file.

        Parameters:
        -----------
        lista_vin : list

        Returns:
        --------
        dict: {'exito': True} o {'exito': False, 'error': ...}
        """
        estado_conexion, mensaje_error = self.__obj_consultas_sql.conectar_db_conexion()

        if estado_conexion:
            dic_retorno = self.__obj_consultas_sql.fn_actu_estado_cmdm(lista_vin)
            self.__obj_consultas_sql.desconectar()
            if dic_retorno['exito']:
                return {'exito': True}
            else:
                return {'exito': False, 'error': dic_retorno['error']}
        else:
            return {'exito': False, 'error': mensaje_error}
    
    def fn_fusionar_dataframes(self,dataframe1,dataframe2):
        """
        Fusiona dos DataFrames por concatenación.

        Parameters:
        -----------
        dataframe1, dataframe2 : pandas.DataFrame

        Returns:
        --------
        pandas.DataFrame: DataFrame fusionado.
        """ 
        return pd.concat(
                        [dataframe1,dataframe2]
                        ,ignore_index = True
                        )

    def fn_mod_col_ho(self,dataframe):
        """
        Modifica la columna 'SDI_PRTY.SRVY_AGRMNT' a 'Y' según condiciones y retorna los VIN afectados.

        Retorna:
            dataframe: DataFrame modificado.
            dataframe_vin_modificados: DataFrame con los VIN a los que se les hizo la modificación.
        """
        # Filtra y modifica las columnas acuerdos en si y vehiculos particulares --------------------
        filter_condition = (
            (dataframe["SDI_PRTY.CMMNCTN_AGRMNT_EML_REN"] == 'Y') &
            (dataframe["SDI_PRTY.CMMNCTN_AGRMNT_PST_REN"] == 'Y') &
            (dataframe["SDI_PRTY.CMMNCTN_AGRMNT_PHN_REN"] == 'Y') &
            (dataframe["SDI_PRTY.CMMNCTN_AGRMNT_SMS_REN"] == 'Y') &
            (dataframe["SDI_VHCL.VHCL_TYP_CD"] == 'VP') &
            (dataframe["SDI_PRTY.SRVY_AGRMNT"] == 'N')
        )

        # Captura los VIN antes de modificar
        dataframe_vin_modificados = dataframe.loc[filter_condition, ["SDI_VHCL.VIN"]].copy()

        # Realiza la modificación
        dataframe.loc[filter_condition, "SDI_PRTY.SRVY_AGRMNT"] = 'Y'

        return dataframe,dataframe_vin_modificados

    def fn_consultar_fechas_dda_vin(self,lista_vin_dda_fecha):
        """
        Consulta fechas de entrega DDA para una lista de VINs.

        Parameters:
        -----------
        lista_vin_dda_fecha : list

        Returns:
        --------
        dict: {'exito': True, 'data': dataframe_vin_fecha_entrega} o {'exito': False, 'error': ...}
        """
        #Conectamos a la base de datos
        estado_conexion, mensaje_error = self.__obj_consultas_sql.conectar_db_conexion()

        if estado_conexion:

            dic_retorno_fecha_dda = self.__obj_consultas_sql.fn_consultar_fechas_vin(lista_vin_dda_fecha)

            if dic_retorno_fecha_dda['exito']:
                self.__obj_consultas_sql.desconectar()

                list_vin_fecha = [[j for j in i] for i in dic_retorno_fecha_dda['data']]
                #Creamos dataframe con la respuesta de la consulta
                dataframe_vin_fecha_entrega = pd.DataFrame(list_vin_fecha
                                                        ,columns = ['SDI_VHCL.VIN','FECHA_DATE','FECHA_DATETIME']
                                                        )
                return {'exito': True, 'data':dataframe_vin_fecha_entrega}
            else:
                self.__obj_consultas_sql.desconectar()
                return {'exito': False, 'error': dic_retorno_fecha_dda['error']}
        else:
            return {'exito': False, 'error': mensaje_error}

    def fn_fusionar_dataframes_merge(self,dataframe1,dataframe2):
        """
        Fusiona dos DataFrames por la columna VIN usando merge.

        Parameters:
        -----------
        dataframe1, dataframe2 : pandas.DataFrame

        Returns:
        --------
        pandas.DataFrame: DataFrame fusionado.
        """
        return pd.merge(dataframe1,dataframe2
                        ,how='left'
                        ,on = 'SDI_VHCL.VIN'
                        )

    def fn_actualizar_fechas_archivo(self,dataframe_vin_dda):
        """
        Actualiza fechas en el DataFrame según información de entrega.

        Parameters:
        -----------
        dataframe_vin_dda : pandas.DataFrame

        Returns:
        --------
        pandas.DataFrame: DataFrame con fechas actualizadas.
        """
        dataframe_vin_dda['SDI_VHCL.LAST_UPDATE_DATE'] = dataframe_vin_dda['FECHA_DATETIME'].combine_first(
            dataframe_vin_dda['SDI_VHCL.LAST_UPDATE_DATE'])

        dataframe_vin_dda['SDI_VHCL.VLD_FRM_DT'] = dataframe_vin_dda['FECHA_DATE'].combine_first(
            dataframe_vin_dda['SDI_VHCL.VLD_FRM_DT'])

        dataframe_vin_dda['SDI_VHCL.DLVRY_DT'] = dataframe_vin_dda['FECHA_DATE'].combine_first(
            dataframe_vin_dda['SDI_VHCL.DLVRY_DT'])

        dataframe_vin_dda['SDI_PRTY.SRVY_AGRMNT_DATE'] = dataframe_vin_dda['FECHA_DATE'].combine_first(
            dataframe_vin_dda['SDI_PRTY.SRVY_AGRMNT_DATE'])

        dataframe_vin_dda = dataframe_vin_dda.drop(columns = ['FECHA_DATE','FECHA_DATETIME'])

        return dataframe_vin_dda

    def fn_consultar_reenvios(self,columnas):
        """
        Consulta VINs marcados para reenvío.

        Parameters:
        -----------
        columnas : list

        Returns:
        --------
        dict: {'exito': True, 'data': dataframe_lista_cmdm} o {'exito': False, 'error': ...}
        """
        lista_vin_reenvio = []

        #Conectamos a la base de datos
        estado_conexion, mensaje_error = self.__obj_consultas_sql.conectar_db_conexion()

        if estado_conexion:
            #Validamos si hay vines marcados para reenvio
            dic_retorno_reenvio = self.__obj_consultas_sql.fn_reenvio_vin_cmdm()
            self.__obj_consultas_sql.desconectar()

            if dic_retorno_reenvio['exito']:

                lista_vin_reenvio = [[j for j in i] for i in dic_retorno_reenvio['data']]

                #Creamos dataframe con la respuesta de la consulta
                dataframe_lista_cmdm = pd.DataFrame(lista_vin_reenvio
                                                ,columns = columnas)

                return {'exito': True, 'data': dataframe_lista_cmdm}

            else:

                return {'exito': False, 'error': dic_retorno_reenvio['error']}

        else:
            return {'exito': False, 'error': mensaje_error}
        
    def fn_prep_info_email(self,
                        dataframe_vin_no_dda=None,
                        dataframe_vin_dda_email=None,
                        dataframe_lista_cmdm=None,
                        dataframe_reenvios_cmdm=None,
                        dataframe_servicio_publico = None):
        """
        Prepara un DataFrame con la información de VIN y su estado de entrega DDA.
        Todos los parámetros son opcionales y pueden ser None o DataFrame vacío.

        Parameters:
        -----------
        dataframe_vin_no_dda, dataframe_vin_dda_email, dataframe_lista_cmdm, dataframe_reenvios_cmdm, dataframe_servicio_publico : pandas.DataFrame or None

        Returns:
        --------
        pandas.DataFrame: DataFrame con información para correo.
        """
        columnas = ['SDI_VHCL.VIN', 'Entrega_dda']

        # Inicializa los dataframes vacíos
        dataframe_email_no_dda = pd.DataFrame(columns=columnas)
        dataframe_email_dda = pd.DataFrame(columns=columnas)
        dataframe_email_delta_cmdm = pd.DataFrame(columns=columnas)
        dataframe_email_reenvios_cmdm = pd.DataFrame(columns=columnas)
        dataframe_email_servicio_publico = pd.DataFrame(columns=columnas)

        if dataframe_vin_no_dda is not None and not dataframe_vin_no_dda.empty:
            dataframe_email_no_dda = dataframe_vin_no_dda[['SDI_VHCL.VIN']].copy()
            dataframe_email_no_dda['Entrega_dda'] = 'No'

        if dataframe_vin_dda_email is not None and not dataframe_vin_dda_email.empty:
            dataframe_email_dda = dataframe_vin_dda_email[['SDI_VHCL.VIN']].copy()
            dataframe_email_dda['Entrega_dda'] = 'Si'

        if dataframe_lista_cmdm is not None and not dataframe_lista_cmdm.empty:
            dataframe_email_delta_cmdm = dataframe_lista_cmdm[['SDI_VHCL.VIN']].copy()
            dataframe_email_delta_cmdm['Entrega_dda'] = 'Procesado'

        if dataframe_reenvios_cmdm is not None and not dataframe_reenvios_cmdm.empty:
            dataframe_email_reenvios_cmdm = dataframe_reenvios_cmdm[['SDI_VHCL.VIN']].copy()
            dataframe_email_reenvios_cmdm['Entrega_dda'] = 'Reenvio'

        if dataframe_servicio_publico is not None and not dataframe_servicio_publico.empty:
            dataframe_email_servicio_publico = dataframe_servicio_publico[['SDI_VHCL.VIN']].copy()
            dataframe_email_servicio_publico['Entrega_dda'] = 'Si'

        # Une solo los dataframes que no están vacíos
        dataframes_a_unir = [
            df for df in [
                dataframe_email_no_dda,
                dataframe_email_dda,
                dataframe_email_delta_cmdm,
                dataframe_email_reenvios_cmdm,
                dataframe_email_servicio_publico
            ] if not df.empty
        ]

        if dataframes_a_unir:
            dataframe_vin_email = pd.concat(dataframes_a_unir, ignore_index=True)
        else:
            dataframe_vin_email = pd.DataFrame(columns=columnas)
        return dataframe_vin_email

    def fn_consul_info_email(self,lista_vin_email):
        """
        Consulta información detallada de VINs para envío de correos.

        Parameters:
        -----------
        lista_vin_email : list

        Returns:
        --------
        dict: {'exito': True, 'data': dataframe_email} o {'exito': False, 'error': ...}
        """
        #Conectamos a la base de datos
        estado_conexion, mensaje_error = self.__obj_consultas_sql.conectar_db_conexion()

        if estado_conexion:
            #Validamos si el vin ya cuenta con entrega en DDA
            dic_retorno_email = self.__obj_consultas_sql.fn_consulta_info_vin_email(lista_vin_email)
            self.__obj_consultas_sql.desconectar()

            if dic_retorno_email['exito']:

                lista_vin_email = [[j for j in i] for i in dic_retorno_email['data']]

                #Creamos dataframe con la respuesta de la consulta
                dataframe_email = pd.DataFrame(lista_vin_email
                                                ,columns = ['N° Identificación'
                                                            ,'Nombre Cliente'
                                                            ,'Apellido Cliente'
                                                            ,'Correo Extranet'
                                                            ,'SDI_VHCL.VIN'
                                                            ,'Tipo Servicio'
                                                            ,'Código BIR'
                                                            ,'Concesionario'
                                                            ,'Fecha de entrega Extranet'
                                                            ,'Razón'
                                                            ,'Acuerdo Email'
                                                            ,'Acuerdo Cod Postal'
                                                            ,'Acuerto Telefono'
                                                            ,'Acuerto SMS'
                                                            ,'Tipo Persona'
                                                            ,'Describción Política'
                                                            ,'Fecha de entrega DDA']
                                                )
                if 'Fecha de entrega DDA' in dataframe_email.columns:
                    dataframe_email['Fecha de entrega DDA'] = dataframe_email['Fecha de entrega DDA'].fillna('')

                return {'exito': True, 'data': dataframe_email}
            else:
                return {'exito': False, 'error': dic_retorno_email['error']}
        else:
            return {'exito': False, 'error': mensaje_error}

    def fn_columna_ho_email(self,dataframe_email,lista_vin_ho_si):
        """
        Agrega columna 'Cambio HO' indicando si el VIN tuvo modificación.

        Parameters:
        -----------
        dataframe_email : pandas.DataFrame
        lista_vin_ho_si : list

        Returns:
        --------
        pandas.DataFrame: DataFrame con columna 'Cambio HO'.
        """
        # Agregar columna "Cambio HO"
        dataframe_email["Cambio HO"] = dataframe_email["SDI_VHCL.VIN"].isin(lista_vin_ho_si).map({True: "Si", False: "No"})
        return dataframe_email

    def fn_generar_archivo_ecxel(self
                               ,df_email
                               ,ruta_excel):
        """
        Genera archivo Excel con la información de correo.

        Parameters:
        -----------
        df_email : pandas.DataFrame
        ruta_excel : str

        Returns:
        --------
        None
        """
        #Eliminamos registros duplicados
        df_email = df_email.drop_duplicates()

        #Cambiamos nombre de columnas de VIN
        df_email = df_email.rename(columns ={"SDI_VHCL.VIN":"Vin"} )

        #Generamos archivo de excel en la ruta específicada
        df_email.to_excel(ruta_excel
                        ,index=False)

    def fn_generar_archivo_cmdm(self,dataframe_file_cmdm
                                ,ruta_csv_cmdm):
        """
        Genera archivo CSV CMDM final.

        Parameters:
        -----------
        dataframe_file_cmdm : pandas.DataFrame
        ruta_csv_cmdm : str

        Returns:
        --------
        None
        """
        #Eliminamos columna estado
        dataframe_file_cmdm = dataframe_file_cmdm.drop(columns=['ESTADO'])

        #Eliminamos los duplicados
        dataframe_file_cmdm = dataframe_file_cmdm.drop_duplicates()

        dataframe_file_cmdm.to_csv(ruta_csv_cmdm
                                ,index = False
                                ,sep = ';')

    def fn_generar_backup_archivo_cmdm(self
                                       ,dataframe
                                       ,ruta_backup):
        """
        Genera archivo backup del CMDM con fecha, hora, minutos y segundos.

        Parameters:
        -----------
        dataframe : pandas.DataFrame
        ruta_backup : str

        Returns:
        --------
        None
        """
        #optenemos fecha y hora para renombrar el archivo backup
        fecha_hora = datetime.now()
        fecha_hora = fecha_hora.strftime('%d%m%Y.%H%M%S')

        #Concatenamos la fecha y hora formateada para crear el nobre del archivo deseado
        ruta_archivo_backup = ruta_backup+fecha_hora

        #Eliminamos columna estado
        dataframe = dataframe.drop(columns=['ESTADO'])
        #Generamos archivo
        dataframe.to_csv(ruta_archivo_backup
                                ,index = False
                                ,sep = ';')

    def fn_consultar_data_servicio_publico(self):
        """
        Consulta VINs de servicio público entregados en DDA.

        Returns:
        --------
        dict: {'exito': True, 'data': dataframe_lista_cmdm_publico} o {'exito': False, 'error': ...}
        """
        #Conectamos a la base de datos
        estado_conexion, mensaje_error = self.__obj_consultas_sql.conectar_db_conexion()

        if estado_conexion:
            #Validamos si el vin ya cuenta con entrega en DDA y es servicio diferente a particular
            dic_retorno_delta = self.__obj_consultas_sql.fn_validar_vin_dda_publicos()
            self.__obj_consultas_sql.desconectar()

            if dic_retorno_delta['exito']:

                lista_vin_dda = [i[0] for i in dic_retorno_delta['data']]

                #Creamos dataframe con la respuesta de la consulta
                dataframe_lista_cmdm_publico = pd.DataFrame(lista_vin_dda
                                                ,columns = ['SDI_VHCL.VIN'])

                return {'exito': True, 'data': dataframe_lista_cmdm_publico}
            else:
                return {'exito': False, 'error': dic_retorno_delta['error']}
        else:
            return {'exito': False, 'error': mensaje_error}

    def fn_eliminar_pub_cmdm(self,dataframe):
        """
        Elimina registros de vehículos de servicio público del DataFrame.

        Parameters:
        -----------
        dataframe : pandas.DataFrame

        Returns:
        --------
        pandas.DataFrame: DataFrame sin vehículos de servicio público.
        """
        df_no_publicos = dataframe[dataframe["SDI_VHCL.VHCL_TYP_CD"] != 'VU']
        return df_no_publicos