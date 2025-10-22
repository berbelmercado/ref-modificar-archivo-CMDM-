"""
Módulo controlador_gestion_correos.py

Este módulo define la clase ControladorGestionCorreos, encargada de gestionar el envío de correos electrónicos relacionados con modificaciones y errores en el procesamiento de archivos CMDM. 
Integra la consulta de destinatarios, el envío de correos y el registro de eventos en el log.

Clases:
-------
ControladorGestionCorreos
    - Encapsula la lógica para enviar correos de modificaciones y errores, consultando los destinatarios y registrando los resultados en el log.

Dependencias:
-------------
- ConsultaCorreosDestinatarios: Clase para consultar los correos de los destinatarios.
- correo_modificacion_encuestas: Función para enviar correos de modificaciones.
- fn_correo_errores: Función para enviar correos de errores.
- crea_log: Función para registrar eventos en el log.

Métodos:
--------
- fn_correo_modificaciones(self):
    Consulta los destinatarios y envía el correo de modificaciones. 
    Registra en el log si el envío fue exitoso o si ocurrió un error.

- fn_correo_error(self):
    Consulta los destinatarios y envía el correo de errores.
    Registra en el log si el envío fue exitoso o si ocurrió un error.

Notas:
------
- Ambos métodos validan que la consulta de destinatarios sea exitosa antes de intentar enviar el correo.
- El resultado de cada operación se registra en el log para trazabilidad.
"""
from servicios.consulta_correos_destinatarios import  ConsultaCorreosDestinatarios
from vista.envio_correo_modificaciones import correo_modificacion_encuestas
from vista.envio_correo_errores import fn_correo_errores
from vista.crear_log import crea_log

class ControladorGestionCorreos:
    """
    Clase para la gestión del envío de correos electrónicos de modificaciones y errores.

    Métodos:
    --------
    - fn_correo_modificaciones: Envía correo de modificaciones a los destinatarios consultados.
    - fn_correo_error: Envía correo de errores a los destinatarios consultados.
    """
    def fn_correo_modificaciones(self):
        """
        Consulta los destinatarios y envía el correo de modificaciones.
        Registra el resultado en el log.
        """
        dic_restorno_correo_destinatarios = ConsultaCorreosDestinatarios().fn_consulta_correos()
        if dic_restorno_correo_destinatarios['exito']:
            dic_retorno_envio_correo  = correo_modificacion_encuestas(dic_restorno_correo_destinatarios['data'])
            if dic_retorno_envio_correo['exito']:
                crea_log('Se envía correo de modificaciones correctamente\n')
            else:
                crea_log(F'Error - No fue posible enviar correo de modificaciones')

    def  fn_correo_error(self):
        """
        Consulta los destinatarios y envía el correo de errores.
        Registra el resultado en el log.
        """
        dic_restorno_correo_destinatarios = ConsultaCorreosDestinatarios().fn_consulta_correos()
        if dic_restorno_correo_destinatarios['exito']:
            dic_retorno_envio_correo  = fn_correo_errores(dic_restorno_correo_destinatarios['data'])
            if dic_retorno_envio_correo['exito']:
                crea_log('Se envía correo de errores correctamente')
            else:
                crea_log(F'Error - No fue posible enviar correo de modificaciones')
