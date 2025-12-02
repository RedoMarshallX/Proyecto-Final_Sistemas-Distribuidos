"""
PROCESO 1: SERVICIO DE OPERACIONES MATEMÁTICAS
Este proceso implementa un servidor gRPC que realiza operaciones básicas:
suma, resta, multiplicación y división con números flotantes.
"""

import grpc
from concurrent import futures
import time
import services_pb2
import services_pb2_grpc

class RelojLamport:
    """
    Reloj de Lamport: mantiene el orden lógico de eventos en sistemas distribuidos.
    Cada proceso tiene su propio reloj que se incrementa con cada evento.
    """
    def __init__(self):
        self.tiempo = 0  # Inicializa el reloj en 0
    
    def incrementar(self):
        """Incrementa el reloj en 1 (usado para eventos internos y envíos)"""
        self.tiempo += 1
        return self.tiempo
    
    def actualizar(self, tiempo_recibido):
        """
        Actualiza el reloj cuando recibe un mensaje.
        Regla de Lamport: tiempo = max(tiempo_local, tiempo_recibido) + 1
        """
        self.tiempo = max(self.tiempo, tiempo_recibido) + 1
        return self.tiempo
    
    def obtener_tiempo(self):
        """Retorna el valor actual del reloj"""
        return self.tiempo


class Bitacora:
    """
    Sistema de registro de eventos (bitácora).
    Registra todos los eventos del proceso con formato claro.
    """
    @staticmethod
    def registrar(tipo_evento, detalles, valor_reloj):
        """
        Registra un evento en la bitácora.
        tipo_evento: INTERNAL, SEND, RECEIVE
        detalles: descripción del evento
        valor_reloj: valor del reloj de Lamport en ese momento
        """
        marca_temporal = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{marca_temporal}] [{tipo_evento}] {detalles} clock={valor_reloj}")


class ServicioMatematicas(services_pb2_grpc.MathServiceServicer):
    """
    Implementación del servicio de matemáticas.
    Hereda de la clase generada por gRPC.
    """
    
    def __init__(self, id_proceso):
        """
        Constructor del servicio.
        id_proceso: identificador único de este proceso (ej: "P1_MATH")
        """
        self.id_proceso = id_proceso
        self.reloj = RelojLamport()  # Crea su propio reloj de Lamport
    
    def Add(self, peticion, contexto):
        """
        Método RPC para SUMAR dos números.
        peticion: contiene sender_id, num1, num2, timestamp
        contexto: información de la conexión gRPC
        """
        # 1. Actualizar el reloj con el timestamp recibido
        self.reloj.actualizar(peticion.timestamp)
        
        # 2. Registrar que recibimos una petición
        Bitacora.registrar("RECEIVE", 
                          f"{self.id_proceso} <- {peticion.sender_id} operacion=SUMAR({peticion.num1}, {peticion.num2})",
                          self.reloj.obtener_tiempo())
        
        # 3. Realizar la operación matemática
        resultado = peticion.num1 + peticion.num2
        
        # 4. Incrementar el reloj (evento interno: cálculo realizado)
        self.reloj.incrementar()
        Bitacora.registrar("INTERNAL", 
                          f"{self.id_proceso} calculó SUMA resultado={resultado}",
                          self.reloj.obtener_tiempo())
        
        # 5. Retornar la respuesta con el resultado y timestamp actualizado
        return services_pb2.MathResponse(
            result=resultado,
            timestamp=self.reloj.obtener_tiempo(),
            status="OK"
        )
    
    def Subtract(self, peticion, contexto):
        """Método RPC para RESTAR dos números"""
        # Actualizar reloj con mensaje recibido
        self.reloj.actualizar(peticion.timestamp)
        Bitacora.registrar("RECEIVE", 
                          f"{self.id_proceso} <- {peticion.sender_id} operacion=RESTAR({peticion.num1}, {peticion.num2})",
                          self.reloj.obtener_tiempo())
        
        # Realizar resta
        resultado = peticion.num1 - peticion.num2
        
        # Incrementar reloj y registrar evento interno
        self.reloj.incrementar()
        Bitacora.registrar("INTERNAL", 
                          f"{self.id_proceso} calculó RESTA resultado={resultado}",
                          self.reloj.obtener_tiempo())
        
        return services_pb2.MathResponse(
            result=resultado,
            timestamp=self.reloj.obtener_tiempo(),
            status="OK"
        )
    
    def Multiply(self, peticion, contexto):
        """Método RPC para MULTIPLICAR dos números"""
        self.reloj.actualizar(peticion.timestamp)
        Bitacora.registrar("RECEIVE", 
                          f"{self.id_proceso} <- {peticion.sender_id} operacion=MULTIPLICAR({peticion.num1}, {peticion.num2})",
                          self.reloj.obtener_tiempo())
        
        # Realizar multiplicación
        resultado = peticion.num1 * peticion.num2
        
        self.reloj.incrementar()
        Bitacora.registrar("INTERNAL", 
                          f"{self.id_proceso} calculó MULTIPLICACIÓN resultado={resultado}",
                          self.reloj.obtener_tiempo())
        
        return services_pb2.MathResponse(
            result=resultado,
            timestamp=self.reloj.obtener_tiempo(),
            status="OK"
        )
    
    def Divide(self, peticion, contexto):
        """Método RPC para DIVIDIR dos números"""
        self.reloj.actualizar(peticion.timestamp)
        Bitacora.registrar("RECEIVE", 
                          f"{self.id_proceso} <- {peticion.sender_id} operacion=DIVIDIR({peticion.num1}, {peticion.num2})",
                          self.reloj.obtener_tiempo())
        
        # Verificar división por cero
        if peticion.num2 == 0:
            return services_pb2.MathResponse(
                result=0,
                timestamp=self.reloj.obtener_tiempo(),
                status="ERROR: División por cero"
            )
        
        # Realizar división
        resultado = peticion.num1 / peticion.num2
        
        self.reloj.incrementar()
        Bitacora.registrar("INTERNAL", 
                          f"{self.id_proceso} calculó DIVISIÓN resultado={resultado}",
                          self.reloj.obtener_tiempo())
        
        return services_pb2.MathResponse(
            result=resultado,
            timestamp=self.reloj.obtener_tiempo(),
            status="OK"
        )


def iniciar_servidor():
    """
    Función principal que inicia el servidor gRPC.
    El servidor escucha en el puerto 50051.
    """
    id_proceso = "P1_MATH"
    
    # Crear servidor con pool de 10 hilos
    servidor = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Registrar el servicio de matemáticas en el servidor
    services_pb2_grpc.add_MathServiceServicer_to_server(
        ServicioMatematicas(id_proceso), servidor
    )
    
    # Escuchar en el puerto 50051 (todos los interfaces)
    servidor.add_insecure_port('[::]:50051')
    
    # Iniciar el servidor
    servidor.start()
    print(f"✓ {id_proceso} servidor iniciado en puerto 50051")
    Bitacora.registrar("INTERNAL", f"{id_proceso} inicializado", 0)
    
    # Mantener el servidor corriendo
    servidor.wait_for_termination()


# Punto de entrada del programa
if __name__ == '__main__':
    iniciar_servidor()
