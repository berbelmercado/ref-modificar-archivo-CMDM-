"""
Módulo consultas_sql.py

Este módulo define la clase ConsultasSql, encargada de gestionar la conexión y las operaciones con la base de datos SQL Server para el sistema de encuestas CMDM. 
Incluye métodos para consultar, insertar, actualizar y validar información relacionada con VINs, reportes DDA, destinatarios de correo, rutas FTP y otros datos relevantes.

Clases:
-------
ConsultasSql
    - Encapsula la lógica para conectar a la base de datos, ejecutar consultas y manejar resultados y errores.

Atributos:
----------
- __server: Servidor SQL.
- __database: Base de datos.
- __username: Usuario de la base de datos.
- __password: Contraseña de la base de datos.
- __cursor: Cursor de la conexión SQL.

Métodos:
--------
- conectar_db_conexion(self): Conecta a la base de datos SQL Server.
- desconectar(self): Desconecta y cierra la conexión a la base de datos.
- fn_consultar_ruta_ftp(self): Consulta la ruta FTP desde la base de datos.
- fn_consultar_fechas_vin(self, lista_vin): Consulta las fechas de entrega DDA para una lista de VINs.
- fn_consultar_destinatarios(self): Consulta los destinatarios de correos electrónicos.
- fn_consulta_estado_dda(self, lista_vin): Consulta el estado de entrega DDA para una lista de VINs.
- fn_insertar_vin_delta_cmdm(self, df_vin_no_dda): Inserta datos de VINs no entregados en DDA en la tabla temporal delta_cmdm_file.
- typeToSize(self, df): Determina los tamaños de los tipos de datos para la inserción en la base de datos.
- fn_validar_vin_cmdm_dda(self): Valida los VINs entregados en DDA y tipo de vehículo VP en la tabla delta_cmdm_file.
- fn_actu_estado_cmdm(self, lista_vin): Actualiza el estado de los VINs en la tabla delta_cmdm_file.
- fn_reenvio_vin_cmdm(self): Consulta los VINs que requieren reenvío en la tabla delta_cmdm_file.
- fn_consulta_info_vin_email(self, lista_vin): Consulta información detallada de los VINs para envío de correos.
- fn_validar_vin_dda_publicos(self): Valida los VINs de servicio público entregados en DDA.

Dependencias:
-------------
- pyodbc: Conexión y operaciones con SQL Server.
- config: Variables de configuración (servidor, base de datos, usuario, contraseña).

Notas:
------
- Todos los métodos retornan diccionarios con claves 'exito', 'data' y 'error' para facilitar el manejo de resultados y errores.
- El módulo está diseñado para ser utilizado por otros componentes del sistema que requieren interacción con la base de datos.
- Se recomienda validar que las listas de VINs no estén vacías antes de ejecutar consultas con parámetros.

"""
import pyodbc
import config

class ConsultasSql:
    """
    Clase para la gestión de consultas y operaciones en la base de datos SQL Server para el sistema CMDM.
    """
    #Constructor
    def __init__(self):
        """
        Inicializa la conexión con los parámetros definidos en el archivo de configuración.
        """
        self.__server = config.SERVIDOR_SQL
        self.__database = config.BASE_DE_DATOS
        self.__username = config.USUARIO_SQL
        self.__password = config.CONTRASENA_SQL
        self.__cursor = None

    def conectar_db_conexion(self):
        """
        Conecta a la base de datos SQL Server.

        Returns:
        --------
        tuple: (True, None) si la conexión es exitosa, (False, ex) si ocurre un error.
        """
        try:
            self.__conexion = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={self.__server};DATABASE={self.__database};UID={self.__username};PWD={self.__password}')
            self.__cursor = self.__conexion.cursor()
            return True,None
        except pyodbc.Error as ex:
            return False, ex

    def desconectar(self):
        """
        Desconecta y cierra la conexión a la base de datos.
        """
        self.__cursor.commit() #Ejecutamos los cambios
        self.__cursor.close() #Cerramos cursor
        self.__conexion.close() #Cerramos la conexion

    def fn_consultar_ruta_ftp(self):
        """
        Consulta la ruta FTP desde la base de datos.

        Returns:
        --------
        dict: {'exito': True, 'data': ruta_ftp, 'error': None} o {'exito': False, 'data': None, 'error': ex}
        """
        try:
            #query sql
            sql_query = """SELECT CADEM03_VALOR
                            FROM CONEXION..[TADEM03_PARAMETROS]
                            WHERE CADEM03_DESCRIPCION LIKE'Ruta ftp'
                        """

            self.__cursor.execute(sql_query)

            ruta_ftp = self.__cursor.fetchall()[0]

            return {'exito':True
                    , 'data':ruta_ftp[0]
                    , 'error':None}

        except Exception as ex:
            return {'exito':False
                    ,'data':None
                    ,'error':ex}

    def fn_consultar_fechas_vin(self,lista_vin):
        """
        Consulta las fechas de entrega DDA para una lista de VINs.

        Parameters:
        -----------
        lista_vin : list
            Lista de VINs a consultar.

        Returns:
        --------
        dict: {'exito': True, 'data': lista_vin} o {'exito': False, 'error': ex}
        """
        try:
            placeholders = ', '.join(['?'] * len(lista_vin))
            #Consulta sql
            sql_query = f"""SELECT vin,
                            CONVERT(VARCHAR,fecha_entrega,103) AS fecha_entrega_dda,
                            CONVERT(VARCHAR,fecha_entrega,103)+' '+ '12:00:00' AS  fecha_entrega_dda_tiem 
                        FROM DATASTEWARD.[dbo].[reporte_dda]
                        WHERE vin IN ({placeholders})
                        """

            self.__cursor.execute(sql_query,lista_vin)
            
            lista_vin = self.__cursor.fetchall()

            return {'exito':True, 'data':lista_vin}
        
        except Exception as ex:

            return {'exito':False,'error':ex}

    def fn_consultar_destinatarios(self):
        """
        Consulta los destinatarios de correos electrónicos desde la base de datos.

        Returns:
        --------
        dict: {'exito': True, 'data': destinatarios, 'error': None} o {'exito': False, 'data': None, 'error': ex}
        """
        try:
            query_info = f"""
                            SELECT CADEM03_VALOR
                            FROM CONEXION..[TADEM03_PARAMETROS]
                            WHERE CADEM03_DESCRIPCION LIKE'Email cambios encuestas'
                        """

            self.__cursor.execute(query_info)

            destinatarios = self.__cursor.fetchall()

            return {'exito':True
                    ,'data':destinatarios
                    ,'error':None
                    }
        except Exception as ex:
            return {'exito':False
                    ,'data':None
                    ,'error':ex}
    
    def fn_consulta_estado_dda(self,lista_vin):
        """
        Consulta el estado de entrega DDA para una lista de VINs.

        Parameters:
        -----------
        lista_vin : list
            Lista de VINs a consultar.

        Returns:
        --------
        dict: {'exito': True, 'data': res_lista_vin, 'error': None} o {'exito': False, 'data': None, 'error': ex}
        """
        try:
            res_lista_vin = []

            placeholders = ', '.join(['?'] * len(lista_vin))

            sql_query_dda = f""" SELECT vin FROM [DATASTEWARD].[dbo].[reporte_dda]
                                    WHERE vin IN ({placeholders})
                            """
            self.__cursor.execute(sql_query_dda,lista_vin)

            res_lista_vin = self.__cursor.fetchall()

            return {'exito':True
                    ,'data':res_lista_vin
                    ,'error':None}

        except Exception as ex:
            return {'exito':False
                    ,'data':None
                    ,'error':ex}

    def fn_insertar_vin_delta_cmdm(self,df_vin_no_dda):
        """
        Inserta los datos de un DataFrame en la tabla temporal delta_cmdm_file.

        Parameters:
        -----------
        df_vin_no_dda : pandas.DataFrame
            DataFrame que contiene los datos a insertar.

        Returns:
        --------
        dict: {'exito': True, 'error': None} o {'exito': False, 'error': e}
        """
        param = "(?"

        for i in range(len(df_vin_no_dda.columns)):
            if i != 0:
                if i != len(df_vin_no_dda.columns)-1:
                    param += ",?"
                else:
                    param += ",?)"

        query_sql = f'INSERT INTO DATASTEWARD..delta_cmdm_file VALUES {param}'

        try:
            self.__cursor.setinputsizes(self.typeToSize(df_vin_no_dda))
            self.__cursor.executemany(query_sql, df_vin_no_dda.values.tolist())

            return {'exito': True, 'error': None}

        except Exception as e:
            return {'exito': False, 'error':e}

    def typeToSize(self,df):
        """
        Determina los tamaños de los tipos de datos en el DataFrame para la inserción en la base de datos.

        Parámetros:
        -----------
        df : pandas.DataFrame
            DataFrame que contiene los datos a insertar.

        Returns:
        --------
        list
            Lista de tamaños de los tipos de datos.
        """
        try:
            types = df.dtypes.values.tolist()
            size = []

            for i in types:
                if (i == "int64") or (i == "int32"):
                    size.append([(pyodbc.SQL_WVARCHAR, 255, 0)])
                if i == ("O") or ("object")or ("float64"):
                    size.append([(pyodbc.SQL_WVARCHAR, 255, 0)])
                if i == "datetime64":
                    size.append([(pyodbc.SQL_TYPE_DATE)])
                if (i == "<M8[ns]") or (i == "datetime64[ns]"):
                    size.append([(pyodbc.SQL_TYPE_DATE)])
                if i == "bool":
                    size.append([(pyodbc.SQL_INTEGER)])

            return size
        except Exception as e:
            return None

    def fn_validar_vin_cmdm_dda(self):
        """
        Valida los VINs entregados en DDA y tipo de vehículo VP en la tabla delta_cmdm_file.

        Returns:
        --------
        dict: {'exito': True, 'data': lista_cmdm} o {'exito': False, 'error': ex}
        """
        rango_consulta = config.RANGO_FECHA_CONSULTA
        lista_cmdm = []
        try:
            sql_query = f""" SELECT CMDM.*
                             FROM DATASTEWARD.[dbo].[delta_cmdm_file] AS CMDM
                                INNER JOIN DATASTEWARD.[dbo].[reporte_dda] AS DDA
                                    ON CMDM.SDI_VHCL_VIN = DDA.vin
                            WHERE CMDM.estado=0
                            AND CMDM.SDI_VHCL_VHCL_TYP_CD = 'VP'
                            AND DDA.fecha_entrega BETWEEN (CONVERT (DATE, GETDATE(){rango_consulta})) AND CONVERT(DATE, GETDATE())
                         """
            self.__cursor.execute(sql_query)
            lista_cmdm = self.__cursor.fetchall()

            return {'exito':True
                    ,'data':lista_cmdm}

        except Exception as ex:

            return {'exito':False
                    ,'error':ex}
    
    def fn_actu_estado_cmdm(self,lista_vin):
        """
        Actualiza el estado de los VINs en la tabla delta_cmdm_file.

        Parameters:
        -----------
        lista_vin : list
            Lista de VINs a actualizar.

        Returns:
        --------
        dict: {'exito': True} o {'exito': False, 'error': ex}
        """
        placeholders = ', '.join(['?'] * len(lista_vin))
        try:

            sql_query = f""" UPDATE  DATASTEWARD.[dbo].[delta_cmdm_file]
                        SET estado=1
                        WHERE SDI_VHCL_VIN IN ({placeholders}) 
                        """
            self.__cursor.execute(sql_query,lista_vin)
            return {'exito':True}
        except Exception as ex:
            return {'exito':False
                    ,'error':ex}
    
    def fn_reenvio_vin_cmdm(self):
        """
        Consulta los VINs que requieren reenvío en la tabla delta_cmdm_file.

        Returns:
        --------
        dict: {'exito': True, 'data': lista_cmdm} o {'exito': False, 'error': ex}
        """
        lista_cmdm = []
        try:
            sql_query = f""" SELECT CMDM.*
                             FROM DATASTEWARD.[dbo].[delta_cmdm_file] AS CMDM
                                INNER JOIN DATASTEWARD.[dbo].[reenvio_encuestas_cmdm] AS RRE
                                    ON CMDM.SDI_VHCL_VIN = RRE.VIN
                            WHERE CMDM.estado=0
                            AND SDI_VHCL_VHCL_TYP_CD = 'VP'
                         """
            self.__cursor.execute(sql_query)
            lista_cmdm = self.__cursor.fetchall()

            return {'exito':True
                    ,'data':lista_cmdm}

        except Exception as ex:
                return {'exito':False
                        ,'error':ex}

    def fn_consulta_info_vin_email(self,lista_vin):
        """
        Consulta información detallada de los VINs para envío de correos.

        Parameters:
        -----------
        lista_vin : list
            Lista de VINs a consultar.

        Returns:
        --------
        dict: {'exito': True, 'data': res_lista_vin} o {'exito': False, 'error': ex}
        """
        try:
            res_lista_vin = []

            placeholders = ', '.join(['?'] * len(lista_vin))

            sql_query_email = f""" with a as (
                                        SELECT 
                                        veh.numvin as vin,
                                        CODV.CB08_CODVEH,
                                        Conexion.dbo.[CorregirNumero](cli.TELRESCLI)  as Telefono,
                                        SUBSTRING(Conexion.dbo.[CorregirNumero](cli.CELCLI),1,10) as Celular,
                                        CASE
                                            WHEN cli.CORELECLI is NULL OR LTRIM(cli.CORELECLI) = '' THEN replace(replace(replace(replace(replace((cj.Vlr_Email), char(44), ' '), char(10), ''), char(13), ''), char(59), ''), CHAR(34), '') 
                                        ELSE replace(replace(replace(replace(replace((cli.corelecli), char(44), ' '), char(10), ''), char(13), ''), char(59), ''), CHAR(34), '')
                                        END as correo_NatJur  
                                        FROM SGS.dbo.VEH_Vehiculos veh
                                        inner join [SISC].[dbo].[GEN_CLIENTES] cli on veh.NumIdeCli = cli.NUMIDECLI and veh.CODTIPIDE=cli.CodTipIde
                                        left join Conexion.dbo.EXTT_DetalleVentaJuridica dj on dj.codVehiculo = veh.NumVin
                                        left join Conexion.dbo.EXTT_ContactoPersonaJuridica cj on cj.Ide_Cedula = dj.idContactoUno
                                        INNER JOIN CONEXION..TB08_Vehiculo CODV ON VEH.TipVehSap=CODV.CB08_CODVEH
                                        where veh.FecVtaVeh is not null 
                                        and numvin in ({placeholders})
                                        and veh.CodConCom != '100209'  
                                        and veh.CodConCom != '100047' 
                                        and veh.CodConCom != '100048' 
                                        and veh.CodConCom != '100186' 
                                        and veh.CodConCom != '86200065' 
                                        and veh.CodConCom != '86200089' 
                                        and veh.CodConCom != '100049' 
                                        and veh.CodConCom != '100323' 
                                        and veh.CodConCom != '100187'  
                                        and veh.CodConCom != '0'  
                                        and veh.CodConCom != '10'  
                                        and veh.CodConCom != '11'  
                                        and veh.CodConCom != '15'  
                                        and veh.CodConCom != '18'  and veh.CodConCom != '19'  and veh.CodConCom != '62'  
                                        and veh.CodConCom != '101'  and veh.CodConCom != '104'  and veh.CodConCom != '105'  
                                        and veh.CodConCom != '106'  and veh.CodConCom != '107'  and veh.CodConCom != '108'  
                                        and veh.CodConCom != '109'  and veh.CodConCom != '136'  and veh.CodConCom != '139'  
                                        and veh.CodConCom != '150'  and veh.CodConCom != '158'  and veh.CodConCom != '166'  
                                        and veh.CodConCom != '207'  and veh.CodConCom != '260'  and veh.CodConCom != '301'  
                                        and veh.CodConCom != '302'  and veh.CodConCom != '310'  and veh.CodConCom != '402'  
                                        and veh.CodConCom != '408'  and veh.CodConCom != '410'  and veh.CodConCom != '501'  
                                        and veh.CodConCom != '502' and veh.CodConCom != '601' and veh.CodConCom != '801' 
                                        and veh.CodConCom != '1101' and veh.CodConCom != '1201' and veh.CodConCom != '1202' 
                                        and veh.CodConCom != '502' and veh.CodConCom != '1401' and veh.CodConCom != '1501' 
                                        and veh.CodConCom != '1601' and veh.CodConCom != '1701' and veh.CodConCom != '1801' 
                                        and veh.CodConCom != '2101' and veh.CodConCom != '3101' and veh.CodConCom != '4001' 
                                        and veh.CodConCom != '4002' and veh.CodConCom != '5001' and veh.CodConCom != '5003' 
                                        and veh.CodConCom != '7000' and veh.CodConCom != '9102' and veh.CodConCom != '9104'  
                                        and veh.CodConCom != '9105' and veh.CodConCom != '9106' and veh.CodConCom != '9107' 
                                        and veh.CodConCom != '9109' and veh.CodConCom != '9110' and veh.CodConCom != '9201' 
                                        and veh.CodConCom != '9207' and veh.CodConCom != '9208' and veh.CodConCom != '9209' 
                                        and veh.CodConCom != '9304' and veh.CodConCom != '9306' and veh.CodConCom != '9308' 
                                        and veh.CodConCom != '9309' and veh.CodConCom != '9404' and veh.CodConCom != '23500' 
                                        and veh.CodConCom != '90100' and veh.CodConCom != '100026' and veh.CodConCom != '100038' 
                                        and veh.CodConCom != '100172' and veh.CodConCom != '100179' and veh.CodConCom != '100204' 
                                        and veh.CodConCom != '100210' and veh.CodConCom != '100217' and veh.CodConCom != '100221' 
                                        and veh.CodConCom != '100252' and veh.CodConCom != '100253' and veh.CodConCom != '100255' and veh.CodConCom != '100256' 
                                        and veh.CodConCom != '100257' and veh.CodConCom != '100269' 
                                        and veh.CodConCom != '100281' and veh.CodConCom != '100353' and veh.CodConCom != '86200688' 
                                        and veh.CodConCom != '86200702' and veh.CodConCom != '86200707' and veh.CodConCom != '86200708' and veh.CodConCom != '86200727'),

                                        b as (
                                        select idclientefinal,vin from a
                                        inner join Conexion.dbo.EXTT_DetalleVentaJuridica dj on dj.codVehiculo = a.vin),

                                        c as (
                                        select vin,CORELECLI as cliente_final_jur from b
                                        inner join [SISC].[dbo].[GEN_CLIENTES] gc on gc.CODCLI = b.idclientefinal),

                                        d as(
                                        select a.vin as VIN, 
                                        CASE
                                            WHEN LEN(a.Telefono)!=7 and LEN(a.Celular)=7 THEN a.Celular
                                            WHEN LEN(a.Telefono) = 10 THEN ' '
                                            WHEN ((a.Telefono) IS NULL OR (a.Telefono) ='') and LEN(a.Celular)!=7 THEN ' '
                                            ELSE a.Telefono
                                        END as TelefonoFin,
                                        CASE 
                                            WHEN LEN(a.Telefono)=10 and LEN(a.Celular)!=10 THEN a.Telefono
                                            WHEN LEN(a.Celular)=7 THEN ' '
                                            ELSE a.Celular
                                        END as CelularFin,
                                        CASE 
                                            WHEN correo_NatJur IS NULL OR correo_NatJur = '' THEN replace(replace(replace(replace(replace((cliente_final_jur), char(44), ' '), char(10), ''), char(13), ''), char(59), ''), CHAR(34), '')
                                        ELSE correo_NatJur
                                        END as Correo
                                        from a
                                        left join c on a.vin = c.vin),
                                        e as (
                                        SELECT CB08_NOMVEH,TB08.CB08_CODVEH  FROM a
                                        INNER JOIN CONEXION..TB08_VEHICULO AS TB08
                                            ON a.CB08_CODVEH= TB08.CB08_CODVEH
                                        ),

                                        SDI_CMDM as(
                                        SELECT cli.NUMIDECLI AS ID_CUSTOMER,

                                        CASE 
                                            when (cli.NOMCLI = 'POR DEFINIR') then ' '
                                            WHEN (cli.NOMCLI=CLI.APECLI) THEN ' '
                                            when (cli.NOMCLI LIKE '% S.A' OR cli.NOMCLI LIKE '% S.A ' OR cli.NOMCLI LIKE '% S.A.' OR cli.NOMCLI LIKE '% SAS' OR cli.NOMCLI LIKE '% SAS ' OR cli.NOMCLI LIKE '% S.A.S' OR cli.NOMCLI LIKE '% S A' OR cli.NOMCLI LIKE '% S.A.S.' OR cli.NOMCLI LIKE '% LTDA' OR cli.NOMCLI LIKE 'COOPERATIVA%' OR cli.NOMCLI LIKE 'BANCO%' OR cli.NOMCLI LIKE 'BBVA%%' OR cli.NOMCLI LIKE 'CONSULTORES%' OR cli.NOMCLI LIKE 'TRANSPORTES%' OR cli.NOMCLI LIKE 'SUPERTIENDAS%' OR cli.NOMCLI LIKE 'DROGUERIAS%' OR cli.NOMCLI LIKE 'LEASING%' OR cli.NOMCLI LIKE 'TECNOLOGIA%' OR cli.NOMCLI LIKE 'INVERSORA%') then ' '
                                            when (cli.nomcli is not null AND (cli.NOMCLI LIKE '% S.A' OR cli.NOMCLI LIKE '% S.A ' OR cli.NOMCLI LIKE '% S.A.' OR cli.NOMCLI LIKE '% SAS' OR cli.NOMCLI LIKE '% SAS ' OR cli.NOMCLI LIKE '% S.A.S' OR cli.NOMCLI LIKE '% S A' OR cli.NOMCLI LIKE '% S.A.S.' OR cli.NOMCLI LIKE '% LTDA' OR cli.NOMCLI LIKE 'COOPERATIVA%' OR cli.NOMCLI LIKE 'BANCO' OR cli.NOMCLI LIKE 'BBVA%' OR cli.NOMCLI LIKE 'CONSULTORES%' OR cli.NOMCLI LIKE 'TRANSPORTES%' OR cli.NOMCLI LIKE 'SUPERTIENDAS%' OR cli.NOMCLI LIKE 'DROGUERIAS%' OR cli.NOMCLI LIKE 'LEASING%' OR cli.NOMCLI LIKE 'TECNOLOGIA%' OR cli.NOMCLI LIKE 'INVERSORA%') AND (cli.APECLI NOT LIKE '% S.A' OR cli.APECLI NOT LIKE '% S.A.' OR cli.APECLI NOT LIKE '% S.A ' OR cli.APECLI NOT LIKE '% SAS' OR cli.APECLI NOT LIKE '% S.A.S' OR cli.APECLI NOT LIKE '% S A' OR cli.APECLI NOT LIKE '% S.A.S.' OR cli.APECLI NOT LIKE '% LTDA' OR cli.APECLI NOT LIKE 'COOPERATIVA%' OR cli.APECLI NOT LIKE 'BANCO%' OR cli.APECLI NOT LIKE 'BBVA%' OR cli.APECLI NOT LIKE 'CONSULTORES%' OR cli.APECLI NOT LIKE 'TRANSPORTES%' OR cli.APECLI NOT LIKE 'SUPERTIENDAS%' OR cli.APECLI NOT LIKE 'DROGUERIAS%' OR cli.APECLI NOT LIKE 'LEASING%' OR cli.APECLI NOT LIKE 'TECNOLOGIA%' OR cli.APECLI NOT LIKE 'INVERSORA%')) then ' '
                                            when (cli.nomcli is not null AND (cli.NOMCLI LIKE '% S.A' OR cli.NOMCLI LIKE '% S.A ' OR cli.NOMCLI LIKE '% S.A.' OR cli.NOMCLI LIKE '% SAS' OR cli.NOMCLI LIKE '% SAS ' OR cli.NOMCLI LIKE '% S.A.S' OR cli.NOMCLI LIKE '% S A' OR cli.NOMCLI LIKE '% S.A.S.' OR cli.NOMCLI LIKE '% LTDA' OR cli.NOMCLI LIKE 'COOPERATIVA%' OR cli.NOMCLI LIKE 'BANCO' OR cli.NOMCLI LIKE 'BBVA%' OR cli.NOMCLI LIKE 'CONSULTORES%' OR cli.NOMCLI LIKE 'TRANSPORTES%' OR cli.NOMCLI LIKE 'SUPERTIENDAS%' OR cli.NOMCLI LIKE 'DROGUERIAS%' OR cli.NOMCLI LIKE 'LEASING%' OR cli.NOMCLI LIKE 'TECNOLOGIA%' OR cli.NOMCLI LIKE 'INVERSORA%') AND (cli.APECLI NOT LIKE '% S.A' OR cli.APECLI NOT LIKE '% S.A.' OR cli.APECLI NOT LIKE '% S.A ' OR cli.APECLI NOT LIKE '% SAS' OR cli.APECLI NOT LIKE '% S.A.S' OR cli.APECLI NOT LIKE '% S A' OR cli.APECLI NOT LIKE '% S.A.S.' OR cli.APECLI NOT LIKE '% LTDA' OR cli.APECLI NOT LIKE 'COOPERATIVA%' OR cli.APECLI NOT LIKE 'BANCO%' OR cli.APECLI NOT LIKE 'BBVA%' OR cli.APECLI NOT LIKE 'CONSULTORES%' OR cli.APECLI NOT LIKE 'TRANSPORTES%' OR cli.APECLI NOT LIKE 'SUPERTIENDAS%' OR cli.APECLI NOT LIKE 'DROGUERIAS%' OR cli.APECLI NOT LIKE 'LEASING%' OR cli.APECLI NOT LIKE 'TECNOLOGIA%' OR cli.APECLI NOT LIKE 'INVERSORA%')) then ' '
                                            when (cli.nomcli is not null AND (cli.NOMCLI NOT LIKE '% S.A' OR cli.NOMCLI NOT LIKE '% S.A ' OR cli.NOMCLI NOT LIKE '% S.A.' OR cli.NOMCLI NOT LIKE '% SAS' OR cli.NOMCLI NOT LIKE '% SAS ' OR cli.NOMCLI NOT LIKE '% S.A.S' OR cli.NOMCLI NOT LIKE '% S A' OR cli.NOMCLI NOT LIKE '% S.A.S.' OR cli.NOMCLI NOT LIKE '% LTDA' OR cli.NOMCLI NOT LIKE 'COOPERATIVA%' OR cli.NOMCLI NOT LIKE 'BANCO%' OR cli.NOMCLI NOT LIKE 'BBVA%' OR cli.NOMCLI NOT LIKE 'CONSULTORES%' OR cli.NOMCLI NOT LIKE 'TRANSPORTES%' OR cli.NOMCLI NOT LIKE 'SUPERTIENDAS%' OR cli.NOMCLI NOT LIKE 'DROGUERIAS%' OR cli.NOMCLI NOT LIKE 'LEASING%' OR cli.NOMCLI NOT LIKE 'TECNOLOGIA%' OR cli.NOMCLI NOT LIKE 'INVERSORA%') AND (cli.APECLI LIKE '% S.A' OR cli.APECLI LIKE '% S.A.' OR cli.APECLI LIKE '% S.A ' OR cli.APECLI LIKE '% SAS' OR cli.APECLI LIKE '% S.A.S' OR cli.APECLI LIKE '% S A' OR cli.APECLI LIKE '% S.A.S.' OR cli.APECLI LIKE '% LTDA' OR cli.APECLI LIKE 'COOPERATIVA%' OR cli.APECLI LIKE 'BANCO%' OR cli.APECLI LIKE 'BBVA%' OR cli.APECLI LIKE 'CONSULTORES%' OR cli.APECLI LIKE 'TRANSPORTES%' OR cli.APECLI LIKE 'SUPERTIENDAS%' OR cli.APECLI LIKE 'DROGUERIAS%' OR cli.APECLI LIKE 'LEASING%' OR cli.APECLI  LIKE 'TECNOLOGIA%' OR cli.APECLI LIKE 'INVERSORA%')) then ' '
                                            WHEN (cli.NOMCLI is not null) THEN replace(replace(replace(replace(replace(replace((cli.NOMCLI), char(10), ''), char(13), ''), char(59), ''), char(44), ' '), char(163), 'Ñ') , CHAR(165), 'Ñ')
                                            else  
                                            
                                                    ' ' 
                                            end as  CUSTOMER_NAME_1,
                                            
                                        CASE 
                                            when (cli.NOMCLI = 'POR DEFINIR') then ' '
                                            when (cli.apecli is null) then cli.nomcli
                                            when (cli.nomcli is not null AND (cli.NOMCLI LIKE '% S.A' OR cli.NOMCLI LIKE '% S.A.' OR cli.NOMCLI LIKE '% SAS' OR cli.NOMCLI LIKE '% S.A.S' OR cli.NOMCLI LIKE '% S A' OR cli.NOMCLI LIKE '% S.A.S.' OR cli.NOMCLI LIKE '% LTDA' OR cli.NOMCLI LIKE 'COOPERATIVA%' OR cli.NOMCLI LIKE 'BANCO%' OR cli.NOMCLI LIKE 'BBVA%' OR cli.NOMCLI LIKE 'CONSULTORES%' OR cli.NOMCLI LIKE 'TRANSPORTES%' OR cli.NOMCLI LIKE 'SUPERTIENDAS%' OR cli.NOMCLI LIKE 'DROGUERIAS%' OR cli.NOMCLI LIKE 'LEASING%' OR cli.NOMCLI LIKE 'TECNOLOGIA%' OR cli.NOMCLI LIKE 'INVERSORA%') AND (cli.APECLI NOT LIKE '% S.A' OR cli.APECLI NOT LIKE '% S.A.' OR cli.APECLI NOT LIKE '% SAS' OR cli.APECLI NOT LIKE '% S.A.S' OR cli.APECLI NOT LIKE '% S A' OR cli.APECLI NOT LIKE '% S.A.S.' OR cli.APECLI NOT LIKE '% LTDA' OR cli.APECLI NOT LIKE 'COOPERATIVA%' OR cli.APECLI NOT LIKE 'BANCO%' OR cli.APECLI NOT LIKE 'BBVA%' OR cli.APECLI NOT LIKE 'CONSULTORES%' OR cli.APECLI NOT LIKE 'TRANSPORTES%' OR cli.APECLI NOT LIKE 'SUPERTIENDAS%' OR cli.APECLI NOT LIKE 'DROGUERIAS%' OR cli.APECLI NOT LIKE 'LEASING%' OR cli.APECLI  NOT LIKE 'TECNOLOGIA%' OR cli.APECLI NOT LIKE 'INVERSORA%')) then cli.nomcli
                                            when (cli.nomcli is not null AND LEN(cli.NOMCLI)> LEN(CLI.APECLI) AND (cli.NOMCLI LIKE '% S.A' OR cli.NOMCLI LIKE '% S.A.' OR cli.NOMCLI LIKE '% SAS' OR cli.NOMCLI LIKE '% S.A.S' OR cli.NOMCLI LIKE '% S A' OR cli.NOMCLI LIKE '% S.A.S.' OR cli.NOMCLI LIKE '% LTDA' OR cli.NOMCLI LIKE 'COOPERATIVA%' OR cli.NOMCLI LIKE 'BANCO%' OR cli.NOMCLI LIKE 'BBVA%' OR cli.NOMCLI LIKE 'CONSULTORES%' OR cli.NOMCLI LIKE 'TRANSPORTES%' OR cli.NOMCLI LIKE 'SUPERTIENDAS%' OR cli.NOMCLI LIKE 'DROGUERIAS%' OR cli.NOMCLI LIKE 'LEASING%' OR cli.NOMCLI LIKE 'TECNOLOGIA%' OR cli.NOMCLI LIKE 'INVERSORA%') AND (cli.APECLI  LIKE '% S.A' OR cli.APECLI  LIKE '% S.A.' OR cli.APECLI  LIKE '% SAS' OR cli.APECLI  LIKE '% S.A.S' OR cli.APECLI  LIKE '% S A' OR cli.APECLI  LIKE '% S.A.S.' OR cli.APECLI  LIKE '% LTDA' OR cli.APECLI  LIKE 'COOPERATIVA%' OR cli.APECLI  LIKE 'BANCO%' OR cli.APECLI LIKE 'BBVA%' OR cli.APECLI LIKE 'CONSULTORES%' OR cli.APECLI LIKE 'TRANSPORTES%' OR cli.APECLI LIKE 'SUPERTIENDAS%' OR cli.APECLI LIKE 'DROGUERIAS%' OR cli.APECLI LIKE 'LEASING%' OR cli.APECLI  LIKE 'TECNOLOGIA%' OR cli.APECLI LIKE 'INVERSORA%')) then cli.nomcli
                                            when (cli.apecli is not null AND LEN(cli.APECLI)> LEN(CLI.NOMCLI) AND (cli.NOMCLI LIKE '% S.A' OR cli.NOMCLI LIKE '% S.A.' OR cli.NOMCLI LIKE '% SAS' OR cli.NOMCLI LIKE '% S.A.S' OR cli.NOMCLI LIKE '% S A' OR cli.NOMCLI LIKE '% S.A.S.' OR cli.NOMCLI LIKE '% LTDA' OR cli.NOMCLI LIKE 'COOPERATIVA%' OR cli.NOMCLI LIKE 'BANCO%' OR cli.NOMCLI LIKE 'BBVA%' OR cli.NOMCLI LIKE 'CONSULTORES%' OR cli.NOMCLI LIKE 'TRANSPORTES%' OR cli.NOMCLI LIKE 'SUPERTIENDAS%' OR cli.NOMCLI LIKE 'DROGUERIAS%' OR cli.NOMCLI LIKE 'LEASING%' OR cli.NOMCLI LIKE 'TECNOLOGIA%' OR cli.NOMCLI LIKE 'INVERSORA%') AND (cli.APECLI  LIKE '% S.A' OR cli.APECLI  LIKE '% S.A.' OR cli.APECLI  LIKE '% SAS' OR cli.APECLI  LIKE '% S.A.S' OR cli.APECLI  LIKE '% S A' OR cli.APECLI  LIKE '% S.A.S.' OR cli.APECLI  LIKE '% LTDA' OR cli.APECLI  LIKE 'COOPERATIVA%' OR cli.APECLI  LIKE 'BANCO%' OR cli.APECLI LIKE 'BBVA%' OR cli.APECLI LIKE 'CONSULTORES%' OR cli.APECLI LIKE 'TRANSPORTES%' OR cli.APECLI LIKE 'SUPERTIENDAS%' OR cli.APECLI LIKE 'DROGUERIAS%' OR cli.APECLI LIKE 'LEASING%' OR cli.APECLI  LIKE 'TECNOLOGIA%' OR cli.APECLI LIKE 'INVERSORA%')) then cli.apecli
                                            WHEN (cli.NOMCLI is not null AND LEN(cli.NOMCLI)> LEN(CLI.APECLI) AND cli.NOMCLI LIKE '%'+cli.APECLI+'%') THEN replace(replace(replace(replace(replace((cli.NOMCLI), char(10), ''), char(13), ''), char(59), ''), CHAR(34), ''), char(163), 'Ñ') 
                                            WHEN (cli.APECLI is not null) THEN replace(replace(replace(replace(replace(replace(replace((cli.APECLI), char(10), ''), char(13), ''), char(59), ''), CHAR(34), ''), char(44), ' '), char(163), 'Ñ'), CHAR(165), 'Ñ')
                                                    else  
                                                    ' ' 
                                            end as  CUSTOMER_SURNAME_1,
                                            '' AS CUSTOMER_SURNAME_2,
                                        --cli.CODTIPPER as Tipo_de_pedido,
                                            '' as COMPANY_NAME,
                                        CASE 
                                            WHEN (cli.NOMCLI LIKE '% SA' OR cli.NOMCLI LIKE '% S.A.' OR cli.NOMCLI LIKE '% SAS' OR cli.NOMCLI LIKE '% S.A.S' OR cli.NOMCLI LIKE '% S A' OR cli.NOMCLI LIKE '% S.A.S.' OR cli.NOMCLI LIKE '% LTDA' OR cli.NOMCLI LIKE 'COOPERATIVA%' OR cli.NOMCLI LIKE 'BANCO%' OR cli.NOMCLI LIKE 'BBVA%%' OR cli.NOMCLI LIKE 'CONSULTORES%' OR cli.NOMCLI LIKE 'TRANSPORTES%' OR cli.NOMCLI LIKE 'SUPERTIENDAS%' OR cli.NOMCLI LIKE 'DROGUERIAS%' OR cli.NOMCLI LIKE 'LEASING%' OR cli.NOMCLI LIKE 'TECNOLOGIA%' OR cli.NOMCLI LIKE 'INVERSORA%') then 'Empresa'
                                            WHEN (cli.APECLI LIKE '% SA' OR cli.APECLI LIKE '% S.A.' OR cli.APECLI LIKE '% SAS' OR cli.APECLI LIKE '% S.A.S' OR cli.APECLI LIKE '% S A' OR cli.APECLI LIKE '% S.A.S.' OR cli.APECLI LIKE '% LTDA' OR cli.APECLI LIKE 'COOPERATIVA%' OR cli.APECLI LIKE 'BANCO%' OR cli.APECLI LIKE 'BBVA%%' OR cli.APECLI LIKE 'CONSULTORES%' OR cli.APECLI LIKE 'TRANSPORTES%' OR cli.APECLI LIKE 'SUPERTIENDAS%' OR cli.APECLI LIKE 'DROGUERIAS%' OR cli.APECLI LIKE 'LEASING%' OR cli.APECLI  LIKE 'TECNOLOGIA%' OR cli.APECLI LIKE 'INVERSORA%') then 'Empresa'
                                            WHEN (cli.CODTIPPER = 1) THEN 'Persona Natural'  
                                            WHEN (cli.CODTIPPER = 2) THEN 'Persona Juridica' 
                                            else ' '
                                            end as  TYPE_OF_CLIENT,

                                            '' AS SUB_TYPE_OF_CLIENT,

                                            cli.CODTIPPER AS TYPE_OF_ORDER,
                                        CASE 
                                        WHEN (cli.CODTIPPER = 1) THEN 'Persona fisica'
                                        ELSE ' '
                                        END AS TYPE_OF_ORDER_DESCRIPTION,
                                        /*
                                        CASE 
                                            WHEN (cli.CODOCU = 3) THEN 'Independent'  
                                            WHEN (cli.CODOCU = 6) THEN 'Independent' 
                                            else ' '
                                            end as Segmentacion_cliente,*/
                                        CASE
                                            WHEN CORREO is NULL OR LTRIM(CORREO) = '' THEN replace(replace(replace(replace(replace(('casaatipico@notiene.com'), char(44), ' '), char(10), ''), char(13), ''), char(59), ''), CHAR(34), '') 
                                        ELSE CORREO
                                        END as E_MAIL_1_EXTRANET,
                                        /*veh.TipSer as tipo_servicio,

                                        CASE 
                                            WHEN (TelefonoFin is NULL or TelefonoFin=' ') and AutEnvioInfo!=0 THEN ' '
                                            WHEN AutEnvioInfo=0 THEN ''
                                            ELSE TelefonoFin
                                        END as MOBILE_PHONE_1,*/
                                        CASE 
                                            WHEN CelularFin is NULL or CelularFin=' ' THEN ' '
                                            WHEN CelularFin IS NOT NULL THEN '+57 '+CelularFin +''
                                            ELSE '+57 '+CelularFin+''
                                        END as MOBILE_PHONE_1,

                                        'SPA' AS CUSTOMER_LANGUAGE,
                                        CASE 
                                            WHEN (veh.FecEntVeh IS not null)  THEN replace(replace(replace(replace((veh.numvin), char(10), ''), char(13), ''), char(59), ''), CHAR(34), '')
                                            else ' '
                                            end as  VIN,
                                        VEH.TipSer,
                                        '' AS REGISTRATION_PLATE,
                                        CASE 
                                            WHEN (veh.FecEntVeh IS not null)  THEN 'REN'
                                            else ' '
                                            end  AS BRAND,
                                        CASE 
                                        WHEN (SUBSTRING (e.CB08_NOMVEH,0, CHARINDEX(' ',e.CB08_NOMVEH,0)) = 'SANDERO' OR SUBSTRING (e.CB08_NOMVEH,0, CHARINDEX(' ',e.CB08_NOMVEH,0)) = 'KOLEOS' /*OR SUBSTRING (e.CB08_NOMVEH,0, CHARINDEX(' ',e.CB08_NOMVEH,0)) = 'LOGAN'*/)
                                        THEN SUBSTRING (e.CB08_NOMVEH,1, CHARINDEX(' ',e.CB08_NOMVEH,1))+ 'II'
                                        WHEN  SUBSTRING (e.CB08_NOMVEH,0, CHARINDEX(' ',e.CB08_NOMVEH,0))= 'KWID'
                                        THEN SUBSTRING (e.CB08_NOMVEH,1, CHARINDEX(' ',e.CB08_NOMVEH,1))+ 'MERCOSUR'
                                        WHEN SUBSTRING (e.CB08_NOMVEH,0, CHARINDEX(' ',e.CB08_NOMVEH,0))= 'OROCH'
                                        THEN 'DUSTER '+SUBSTRING (e.CB08_NOMVEH,1, CHARINDEX(' ',e.CB08_NOMVEH,1))
                                        WHEN (SUBSTRING (e.CB08_NOMVEH,1, CHARINDEX(' ',e.CB08_NOMVEH,1)) = 'NEW')
                                        THEN 'KOKEOS II'
                                        WHEN (SUBSTRING (e.CB08_NOMVEH,1, CHARINDEX(' ',e.CB08_NOMVEH,1)) = 'LOGAN')
                                        THEN 'SYMBOL II/LOGAN II'
                                        WHEN (SUBSTRING (e.CB08_NOMVEH,1, CHARINDEX(' ',e.CB08_NOMVEH,1)) = 'CAPTUR')
                                        THEN 'KAPTUR/CAPTUR'
                                        ELSE SUBSTRING (e.CB08_NOMVEH,1, CHARINDEX(' ',e.CB08_NOMVEH,1))
                                        END AS MODEL ,

                                        CASE 
                                            WHEN (veh.FecEntVeh IS not null)  THEN 
                                            CASE 
                                                    WHEN (veh.TipSer = 'PAR') then 'VP'
                                                    WHEN (veh.TipSer = 'PUB') then 'VU'
                                                    else ' '
                                            end
                                            end as 'TYPE_OF_VEHICLE',
                                        CASE 
                                            WHEN (veh.FecEntVeh IS not null)  THEN 'VN'
                                            else ' '
                                            end as  'VN/VO',
                                        /*CASE 
                                            WHEN (veh.FecEntVeh IS not null)  THEN sal.Cod_Bir

                                            else ' '
                                            end as  DELIVERY_DEALER,
                                            */
                                        veh.motped as razon,

                                        CASE 
                                            WHEN (veh.FecEntVeh IS not null AND sal.Cod_Bir IS NULL)  
                                                THEN ' ' 
                                            else sal.Cod_Bir
                                            end as  DELIVERY_DEALER,

                                        CONVERT (DATE,veh.FecEntVeh) AS DELIVERY_DATE,
                                        --REPLACE(CONVERT(CHAR(10),veh.FecEntVeh, 103),' ',' - ') as Fecha_entrega,
                                        '' AS ID_SALES_ADVISOR,
                                        'THERM' AS FUEL_TYPE,

                                        CASE 
                                            WHEN (cli.NOMCLI LIKE '% SA' OR cli.NOMCLI LIKE '% S.A.' OR cli.NOMCLI LIKE '% SAS' OR cli.NOMCLI LIKE '% S.A.S' OR cli.NOMCLI LIKE '% S A' OR cli.NOMCLI LIKE '% S.A.S.' OR cli.NOMCLI LIKE '% LTDA' OR cli.NOMCLI LIKE 'COOPERATIVA%' OR cli.NOMCLI LIKE 'BANCO%' OR cli.NOMCLI LIKE 'BBVA%%' OR cli.NOMCLI LIKE 'CONSULTORES%' OR cli.NOMCLI LIKE 'TRANSPORTES%' OR cli.NOMCLI LIKE 'SUPERTIENDAS%' OR cli.NOMCLI LIKE 'DROGUERIAS%' OR cli.NOMCLI LIKE 'LEASING%' OR cli.NOMCLI LIKE 'TECNOLOGIA%' OR cli.NOMCLI LIKE 'INVERSORA%') then 'Independent'
                                            WHEN (cli.APECLI LIKE '% SA' OR cli.APECLI LIKE '% S.A.' OR cli.APECLI LIKE '% SAS' OR cli.APECLI LIKE '% S.A.S' OR cli.APECLI LIKE '% S A' OR cli.APECLI LIKE '% S.A.S.' OR cli.APECLI LIKE '% LTDA' OR cli.APECLI LIKE 'COOPERATIVA%' OR cli.APECLI LIKE 'BANCO%' OR cli.APECLI LIKE 'BBVA%%' OR cli.APECLI LIKE 'CONSULTORES%' OR cli.APECLI LIKE 'TRANSPORTES%' OR cli.APECLI LIKE 'SUPERTIENDAS%' OR cli.APECLI LIKE 'DROGUERIAS%' OR cli.APECLI LIKE 'LEASING%' OR cli.APECLI  LIKE 'TECNOLOGIA%' OR cli.APECLI LIKE 'INVERSORA%') then 'Independent'
                                            WHEN (cli.CODTIPPER = 1) THEN ' '  
                                            WHEN (cli.CODTIPPER = 2) THEN 'Independent' 
                                            else ' '
                                            end AS SEGMENTATION_OF_CLIENT,
                                        CASE 
                                            WHEN (cli.CODTIPPER = 1)
                                                THEN 'N'
                                            ELSE ' '
                                        END AS RNLT_GRP_STFF,

                                        CASE 
                                            WHEN (veh.FecEntVeh IS not null)  THEN sal.Nom_Sala
                                            else 'no value'
                                            end as  CONCESION,

                                        CASE 
                                            WHEN (cli.AutEnvioInfo = 0) THEN 'N'  
                                            WHEN (cli.AutEnvioInfo = 1) THEN 'Y' 
                                            else 'no value'
                                            end as  Acuerdo_email,

                                        CASE 
                                            WHEN (cli.AutEnvioInfo = 0) THEN 'N'  
                                            WHEN (cli.AutEnvioInfo = 1) THEN 'Y' 
                                            else 'no value'
                                            end as  Acuerdo_cod_postal,
                                        CASE 
                                            WHEN (cli.AutEnvioInfo = 0) THEN 'N'  
                                            WHEN (cli.AutEnvioInfo = 1) THEN 'Y' 
                                            else 'no value'
                                            end as  Acuerdo_telefono,
                                        CASE 
                                            WHEN (cli.AutEnvioInfo = 0) THEN 'N'  
                                            WHEN (cli.AutEnvioInfo = 1) THEN 'Y' 
                                            else 'no value'
                                            end as  Acuerdo_SMS,
                                        CASE	
                                            WHEN pf.CCA29_DES_POLITICA like '%proximidad%' OR cli.CODTIPPER = 2 THEN 'N'
                                            WHEN (cli.NOMCLI LIKE '% SA' OR cli.NOMCLI LIKE '% S.A.' OR cli.NOMCLI LIKE '% SAS' OR cli.NOMCLI LIKE '% S.A.S' OR cli.NOMCLI LIKE '% S A' OR cli.NOMCLI LIKE '% S.A.S.' OR cli.NOMCLI LIKE '% LTDA' OR cli.NOMCLI LIKE 'COOPERATIVA%' OR cli.NOMCLI LIKE 'BANCO%' OR cli.NOMCLI LIKE 'BBVA%%' OR cli.NOMCLI LIKE 'CONSULTORES%' OR cli.NOMCLI LIKE 'TRANSPORTES%' OR cli.NOMCLI LIKE 'SUPERTIENDAS%' OR cli.NOMCLI LIKE 'DROGUERIAS%' OR cli.NOMCLI LIKE 'LEASING%' OR cli.NOMCLI LIKE 'TECNOLOGIA%' OR cli.NOMCLI LIKE 'INVERSORA%') THEN 'N'
                                            WHEN (cli.APECLI LIKE '% SA' OR cli.APECLI LIKE '% S.A.' OR cli.APECLI LIKE '% SAS' OR cli.APECLI LIKE '% S.A.S' OR cli.APECLI LIKE '% S A' OR cli.APECLI LIKE '% S.A.S.' OR cli.APECLI LIKE '% LTDA' OR cli.APECLI LIKE 'COOPERATIVA%' OR cli.APECLI LIKE 'BANCO%' OR cli.APECLI LIKE 'BBVA%%' OR cli.APECLI LIKE 'CONSULTORES%' OR cli.APECLI LIKE 'TRANSPORTES%' OR cli.APECLI LIKE 'SUPERTIENDAS%' OR cli.APECLI LIKE 'DROGUERIAS%' OR cli.APECLI LIKE 'LEASING%' OR cli.APECLI  LIKE 'TECNOLOGIA%' OR cli.APECLI LIKE 'INVERSORA%') THEN 'N'
                                        ELSE 'Y'
                                        END as tipo_de_flotilla,

                                        CCA29_DES_POLITICA as descrip_politica

                                        FROM SGS.dbo.VEH_Vehiculos veh
                                        inner join d on veh.NumVin = d.vin
                                        inner join [SISC].[dbo].[GEN_CLIENTES] cli on veh.NumIdeCli = cli.NUMIDECLI and veh.CODTIPIDE=cli.CodTipIde
                                        inner join SISC.dbo.GEN_CIUDADES ciu on ciu.CODCIU = cli.CODCIU
                                        inner join SISC.dbo.GEN_DEPARTAMENTOS dep on dep.coddep = ciu.CODDEP
                                        inner join Conexion.dbo.TB08_VEHICULO tb08 on tb08.CB08_CODVEH = veh.TipVehSap
                                        left join Conexion.dbo.EXTT_DetalleVentaJuridica dj on dj.codVehiculo = veh.NumVin
                                        left join Conexion.dbo.EXTT_ContactoPersonaJuridica cj on cj.Ide_Cedula = dj.idContactoUno
                                        left join [Conexion].[dbo].[EXTT_Salas] as sal on veh.ide_Sala = sal.ide_Sala
                                        left join conexion.dbo.TCA29_POLITICAS_FLOTILLAS as pf on veh.Politica_Flotillas=pf.CCA29_COD_POLITICA
                                        left join Conexion.dbo.vin_excluir_cierre ex on veh.NumVin=ex.vin
                                        INNER JOIN e on  e.CB08_CODVEH = veh.TipVehSap
                                        where ex.vin is null )


                                        select distinct ID_CUSTOMER AS 'N° Identificación'
                                                        ,ISNULL (CUSTOMER_NAME_1,'') AS 'Nombre Cliente'
                                                        ,ISNULL (CUSTOMER_SURNAME_1,'') AS 'Apellido Cliente '
                                                        ,ISNULL (E_MAIL_1_EXTRANET,'') AS 'Correo Extranet'
                                                        ,CMDM.VIN AS 'VIN'
                                                        ,TipSer AS 'Tipo Servicio'
                                                        ,ISNULL(DELIVERY_DEALER,'') AS 'Código BIR'
                                                        ,ISNULL(CONCESION,'') AS 'Concesionario' 
                                                        ,ISNULL(DELIVERY_DATE,'') AS 'Fecha de entrega'
                                                        ,ISNULL(razon,'') AS 'Razon'
                                                        ,ISNULL(Acuerdo_email,'') AS 'Acuerdo Email'
                                                        ,ISNULL(Acuerdo_cod_postal,'') AS 'Acuerdo Postal'
                                                        ,ISNULL(Acuerdo_telefono,'') AS 'Acuerdo Telefono'
                                                        ,ISNULL(Acuerdo_SMS,'') AS 'Acuerdo SMS'
                                                        ,ISNULL(TYPE_OF_CLIENT,'') AS 'Tipo Persona'
                                                        ,ISNULL(descrip_politica,'') AS 'Describción Política'
                                                        ,fecha_entrega AS 'Fecha de entrega DDA'
                                        from SDI_CMDM AS CMDM
                                        LEFT JOIN [DATASTEWARD].[dbo].[reporte_dda] AS DDA
                                            ON CMDM.VIN = DDA.VIN
                               """
            self.__cursor.execute(sql_query_email,lista_vin)
            res_lista_vin = self.__cursor.fetchall()

            return {'exito':True
                    ,'data':res_lista_vin}

        except Exception as ex:
            return {'exito':False
                    ,'error':ex}

    def fn_validar_vin_dda_publicos(self):
        """
        Valida los VINs de servicio público entregados en DDA.

        Returns:
        --------
        dict: {'exito': True, 'data': lista_data_ser_publico} o {'exito': False, 'error': ex}
        """
        lista_data_ser_publico = []
        try:
            sql_query = f""" SELECT top 500 CMDM.SDI_VHCL_VIN
                             FROM DATASTEWARD.[dbo].[delta_cmdm_file] AS CMDM
                                INNER JOIN DATASTEWARD.[dbo].[reporte_dda] AS DDA
                                    ON CMDM.SDI_VHCL_VIN = DDA.vin
                            WHERE CMDM.estado=0
                            AND CMDM.SDI_VHCL_VHCL_TYP_CD <> 'VP'
                         """
            self.__cursor.execute(sql_query)
            lista_data_ser_publico = self.__cursor.fetchall()

            return {'exito':True
                    ,'data':lista_data_ser_publico}
        except Exception as ex:
            return {'exito':False
                    ,'error':ex}
