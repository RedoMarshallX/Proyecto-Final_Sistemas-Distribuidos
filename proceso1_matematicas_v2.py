"""
PROCESO 1: SERVICIO DE OPERACIONES MATEMÁTICAS (CON COMUNICACIÓN)
Este proceso:
1. Evento interno: Operaciones matemáticas
2. Envía mensaje a P3
3. Evento interno: generar número aleatorio
"""

import grpc
from concurrent import futures
import time
import random
import services_pb2
import services_pb2_grpc
import threading

class RelojLamport:
    """Reloj de Lamport con thread-safety"""
    def __init__(self):
        self.tiempo = 0
        self.lock = threading.Lock()
    
    def incrementar(self):
        with self.lock:
            self.tiempo += 1
            return self.tiempo
    
    def actualizar(self, tiempo_recibido):
        with self.lock:
            self.tiempo = max(self.tiempo, tiempo_recibido) + 1
            return self.tiempo
    
    def obtener_tiempo(self):
        with self.lock:
            return self.tiempo


class Bitacora:
    """Sistema de registro de eventos"""
    @staticmethod
    def registrar(tipo_evento, detalles, valor_reloj):
        marca_temporal = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{marca_temporal}] [{tipo_evento}] {detalles} clock={valor_reloj}")


class ServicioMatematicas(services_pb2_grpc.MathServiceServicer):
    """Implementación del servicio de matemáticas"""
    
    def __init__(self, id_proceso, reloj):
        self.id_proceso = id_proceso
        self.reloj = reloj
    
    def Add(self, peticion, contexto):
        self.reloj.actualizar(peticion.timestamp)
        Bitacora.registrar("RECEIVE", 
                          f"{self.id_proceso} <- {peticion.sender_id} operacion=SUMAR({peticion.num1}, {peticion.num2})",
                          self.reloj.obtener_tiempo())
        
        resultado = peticion.num1 + peticion.num2
        self.reloj.incrementar()
        Bitacora.registrar("INTERNAL", 
                          f"{self.id_proceso} calculó SUMA resultado={resultado}",
                          self.reloj.obtener_tiempo())
        
        return services_pb2.MathResponse(
            result=resultado,
            timestamp=self.reloj.obtener_tiempo(),
            status="OK"
        )
    
    def Subtract(self, peticion, contexto):
        self.reloj.actualizar(peticion.timestamp)
        Bitacora.registrar("RECEIVE", 
                          f"{self.id_proceso} <- {peticion.sender_id} operacion=RESTAR({peticion.num1}, {peticion.num2})",
                          self.reloj.obtener_tiempo())
        
        resultado = peticion.num1 - peticion.num2
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
        self.reloj.actualizar(peticion.timestamp)
        Bitacora.registrar("RECEIVE", 
                          f"{self.id_proceso} <- {peticion.sender_id} operacion=MULTIPLICAR({peticion.num1}, {peticion.num2})",
                          self.reloj.obtener_tiempo())
        
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
        self.reloj.actualizar(peticion.timestamp)
        Bitacora.registrar("RECEIVE", 
                          f"{self.id_proceso} <- {peticion.sender_id} operacion=DIVIDIR({peticion.num1}, {peticion.num2})",
                          self.reloj.obtener_tiempo())
        
        if peticion.num2 == 0:
            return services_pb2.MathResponse(
                result=0,
                timestamp=self.reloj.obtener_tiempo(),
                status="ERROR: División por cero"
            )
        
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


class ServicioMensajes(services_pb2_grpc.MessageServiceServicer):
    """Servicio para recibir mensajes de otros procesos"""
    
    def __init__(self, id_proceso, reloj):
        self.id_proceso = id_proceso
        self.reloj = reloj
    
    def SendMessage(self, peticion, contexto):
        # Actualizar reloj al recibir mensaje
        self.reloj.actualizar(peticion.timestamp)
        Bitacora.registrar("RECEIVE", 
                          f"{self.id_proceso} <- {peticion.sender_id} mensaje='{peticion.message}'",
                          self.reloj.obtener_tiempo())
        
        return services_pb2.MessageResponse(
            status="ACK",
            timestamp=self.reloj.obtener_tiempo()
        )


def enviar_mensaje_a_proceso(id_origen, id_destino, mensaje, timestamp, host, puerto):
    """Función para enviar mensaje a otro proceso"""
    try:
        canal = grpc.insecure_channel(f'{host}:{puerto}')
        cliente = services_pb2_grpc.MessageServiceStub(canal)
        
        peticion = services_pb2.MessageRequest(
            sender_id=id_origen,
            receiver_id=id_destino,
            message=mensaje,
            timestamp=timestamp
        )
        
        respuesta = cliente.SendMessage(peticion, timeout=5.0)
        return respuesta.timestamp
    except Exception as e:
        print(f"[ERROR] No se pudo enviar mensaje a {id_destino}: {e}")
        return timestamp


def tarea_proceso1(id_proceso, reloj):
    """
    Tarea específica del Proceso 1:
    1. Evento interno: Calcular suma, resta, multiplicación y división de 2 números
    2. Envía mensaje a P3
    3. Evento interno: generar número aleatorio
    """
    # Esperar a que el servidor esté listo
    time.sleep(3)
    
    # 1. EVENTO INTERNO: Operaciones matemáticas con 2 números
    reloj.incrementar()
    num1 = 25.5
    num2 = 10.3
    suma = num1 + num2
    resta = num1 - num2
    multiplicacion = num1 * num2
    division = num1 / num2
    Bitacora.registrar("INTERNAL", 
                      f"{id_proceso} operaciones: {num1}+{num2}={suma:.2f}, {num1}-{num2}={resta:.2f}, {num1}*{num2}={multiplicacion:.2f}, {num1}/{num2}={division:.2f}",
                      reloj.obtener_tiempo())
    
    time.sleep(1)
    
    # 2. ENVIAR MENSAJE A P3
    reloj.incrementar()
    Bitacora.registrar("SEND", 
                      f"{id_proceso} -> P3_MATRIX mensaje='Hola P3, operaciones completadas'",
                      reloj.obtener_tiempo())
    
    timestamp_recibido = enviar_mensaje_a_proceso(
        id_proceso, "P3_MATRIX", 
        f"Hola P3, operaciones completadas: suma={suma:.2f}",
        reloj.obtener_tiempo(),
        "proceso3", 50053  # En Docker: hostname proceso3
    )
    
    # Actualizar reloj con respuesta
    reloj.actualizar(timestamp_recibido)
    
    time.sleep(1)
    
    # 3. EVENTO INTERNO: Generar número aleatorio
    reloj.incrementar()
    numero_aleatorio = random.randint(1, 100)
    Bitacora.registrar("INTERNAL", 
                      f"{id_proceso} generó número aleatorio = {numero_aleatorio}",
                      reloj.obtener_tiempo())


def iniciar_servidor():
    """Función principal que inicia el servidor gRPC"""
    id_proceso = "P1_MATH"
    reloj = RelojLamport()
    
    # Crear servidor con pool de hilos
    servidor = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Registrar AMBOS servicios
    services_pb2_grpc.add_MathServiceServicer_to_server(
        ServicioMatematicas(id_proceso, reloj), servidor
    )
    services_pb2_grpc.add_MessageServiceServicer_to_server(
        ServicioMensajes(id_proceso, reloj), servidor
    )
    
    # Escuchar en el puerto 50051
    servidor.add_insecure_port('[::]:50051')
    servidor.start()
    
    print(f"{id_proceso} servidor iniciado en puerto 50051")
    Bitacora.registrar("INTERNAL", f"{id_proceso} inicializado", reloj.obtener_tiempo())
    
    # Iniciar tarea de comunicación en un hilo separado
    threading.Thread(target=tarea_proceso1, args=(id_proceso, reloj), daemon=True).start()
    
    # Mantener el servidor corriendo
    try:
        servidor.wait_for_termination()
    except KeyboardInterrupt:
        print(f"\n{id_proceso} detenido")
        servidor.stop(0)


if __name__ == '__main__':
    iniciar_servidor()
