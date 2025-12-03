"""
PROCESO 2: SERVICIO DE CÁLCULO DE PROMEDIO (CON COMUNICACIÓN)
Este proceso:
1. Evento interno: calcular factorial(8)
2. Envía mensaje a P1
3. Recibe mensaje de P4
"""

import grpc
from concurrent import futures
import time
import random
import math
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


class ServicioPromedio(services_pb2_grpc.AverageServiceServicer):
    """Implementación del servicio de cálculo de promedio"""
    
    def __init__(self, id_proceso, reloj):
        self.id_proceso = id_proceso
        self.reloj = reloj
    
    def CalculateAverage(self, peticion, contexto):
        self.reloj.actualizar(peticion.timestamp)
        Bitacora.registrar("RECEIVE", 
                          f"{self.id_proceso} <- {peticion.sender_id} operacion=CALCULAR_PROMEDIO(50 números)",
                          self.reloj.obtener_tiempo())
        
        self.reloj.incrementar()
        numeros = [random.uniform(0, 10) for _ in range(50)]
        Bitacora.registrar("INTERNAL", 
                          f"{self.id_proceso} generó 50 números aleatorios",
                          self.reloj.obtener_tiempo())
        
        self.reloj.incrementar()
        promedio = sum(numeros) / len(numeros)
        Bitacora.registrar("INTERNAL", 
                          f"{self.id_proceso} calculó PROMEDIO resultado={promedio:.4f}",
                          self.reloj.obtener_tiempo())
        
        return services_pb2.AverageResponse(
            numbers=numeros,
            average=promedio,
            timestamp=self.reloj.obtener_tiempo()
        )


class ServicioMensajes(services_pb2_grpc.MessageServiceServicer):
    """Servicio para recibir mensajes de otros procesos"""
    
    def __init__(self, id_proceso, reloj):
        self.id_proceso = id_proceso
        self.reloj = reloj
    
    def SendMessage(self, peticion, contexto):
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


def tarea_proceso2(id_proceso, reloj):
    """
    Tarea específica del Proceso 2:
    1. Evento interno: calcular factorial(8)
    2. Envía mensaje a P1
    3. Recibe mensaje de P4 (el servidor lo recibe automáticamente)
    """
    time.sleep(4)
    
    # 1. EVENTO INTERNO: Calcular factorial(8)
    reloj.incrementar()
    factorial = math.factorial(8)
    Bitacora.registrar("INTERNAL", 
                      f"{id_proceso} calculó factorial(8) = {factorial}",
                      reloj.obtener_tiempo())
    
    time.sleep(1)
    
    # 2. ENVIAR MENSAJE A P1
    reloj.incrementar()
    Bitacora.registrar("SEND", 
                      f"{id_proceso} -> P1_MATH mensaje='Hola P1, factorial: {factorial}'",
                      reloj.obtener_tiempo())
    
    timestamp_recibido = enviar_mensaje_a_proceso(
        id_proceso, "P1_MATH", 
        f"Hola P1, factorial: {factorial}",
        reloj.obtener_tiempo(),
        "proceso1", 50051
    )
    
    reloj.actualizar(timestamp_recibido)
    
    # 3. El mensaje de P4 se recibe automáticamente por el servidor
    print(f"[INFO] {id_proceso} esperando mensaje de P4...")


def iniciar_servidor():
    """Función principal que inicia el servidor gRPC"""
    id_proceso = "P2_AVG"
    reloj = RelojLamport()
    
    servidor = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    services_pb2_grpc.add_AverageServiceServicer_to_server(
        ServicioPromedio(id_proceso, reloj), servidor
    )
    services_pb2_grpc.add_MessageServiceServicer_to_server(
        ServicioMensajes(id_proceso, reloj), servidor
    )
    
    servidor.add_insecure_port('[::]:50052')
    servidor.start()
    
    print(f"✓ {id_proceso} servidor iniciado en puerto 50052")
    Bitacora.registrar("INTERNAL", f"{id_proceso} inicializado", reloj.obtener_tiempo())
    
    threading.Thread(target=tarea_proceso2, args=(id_proceso, reloj), daemon=True).start()
    
    try:
        servidor.wait_for_termination()
    except KeyboardInterrupt:
        print(f"\n{id_proceso} detenido")
        servidor.stop(0)


if __name__ == '__main__':
    iniciar_servidor()
