"""
PROCESO 3: SERVICIO DE MULTIPLICACIÓN DE MATRICES (CON COMUNICACIÓN)
Este proceso:
1. Recibe mensaje de P1
2. Evento interno: Matriz resultante del producto de 2 matrices de 2×2 con números aleatorios (0 a 10)
3. Envía mensaje a P5
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


class ServicioMatrices(services_pb2_grpc.MatrixServiceServicer):
    """Implementación del servicio de multiplicación de matrices"""
    
    def __init__(self, id_proceso, reloj):
        self.id_proceso = id_proceso
        self.reloj = reloj
    
    def generar_matriz_2x2(self):
        return [random.uniform(0, 10) for _ in range(4)]
    
    def multiplicar_matrices_2x2(self, A, B):
        a00, a01, a10, a11 = A
        b00, b01, b10, b11 = B
        
        c00 = a00 * b00 + a01 * b10
        c01 = a00 * b01 + a01 * b11
        c10 = a10 * b00 + a11 * b10
        c11 = a10 * b01 + a11 * b11
        
        return [c00, c01, c10, c11]
    
    def MultiplyMatrices(self, peticion, contexto):
        self.reloj.actualizar(peticion.timestamp)
        Bitacora.registrar("RECEIVE", 
                          f"{self.id_proceso} <- {peticion.sender_id} operacion=MULTIPLICAR_MATRICES(2x2)",
                          self.reloj.obtener_tiempo())
        
        self.reloj.incrementar()
        matriz_a = self.generar_matriz_2x2()
        matriz_b = self.generar_matriz_2x2()
        Bitacora.registrar("INTERNAL", 
                          f"{self.id_proceso} generó matrices A y B aleatorias",
                          self.reloj.obtener_tiempo())
        
        self.reloj.incrementar()
        resultado = self.multiplicar_matrices_2x2(matriz_a, matriz_b)
        Bitacora.registrar("INTERNAL", 
                          f"{self.id_proceso} multiplicó matrices A * B",
                          self.reloj.obtener_tiempo())
        
        return services_pb2.MatrixResponse(
            matrix_a=services_pb2.Matrix2x2(values=matriz_a),
            matrix_b=services_pb2.Matrix2x2(values=matriz_b),
            result=services_pb2.Matrix2x2(values=resultado),
            timestamp=self.reloj.obtener_tiempo()
        )


class ServicioMensajes(services_pb2_grpc.MessageServiceServicer):
    """Servicio para recibir mensajes de otros procesos"""
    
    def __init__(self, id_proceso, reloj, evento_recibido):
        self.id_proceso = id_proceso
        self.reloj = reloj
        self.evento_recibido = evento_recibido
    
    def SendMessage(self, peticion, contexto):
        self.reloj.actualizar(peticion.timestamp)
        Bitacora.registrar("RECEIVE", 
                          f"{self.id_proceso} <- {peticion.sender_id} mensaje='{peticion.message}'",
                          self.reloj.obtener_tiempo())
        
        # Señalar que recibimos el mensaje de P1
        self.evento_recibido.set()
        
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


def tarea_proceso3(id_proceso, reloj, evento_recibido):
    """
    Tarea específica del Proceso 3:
    1. Recibe mensaje de P1 (espera)
    2. Evento interno: Matriz resultante del producto de 2 matrices de 2×2 con números aleatorios (0 a 10)
    3. Envía mensaje a P5
    """
    # 1. ESPERAR MENSAJE DE P1
    print(f"[INFO] {id_proceso} esperando mensaje de P1...")
    evento_recibido.wait(timeout=10)  # Espera hasta 10 segundos
    
    time.sleep(1)
    
    # 2. EVENTO INTERNO: Multiplicar matrices 2x2 con números aleatorios (0-10)
    reloj.incrementar()
    # Generar matrices con números aleatorios de 0 a 10
    matriz_a = [random.uniform(0, 10) for _ in range(4)]
    matriz_b = [random.uniform(0, 10) for _ in range(4)]
    
    # Multiplicar matrices 2x2
    a00, a01, a10, a11 = matriz_a
    b00, b01, b10, b11 = matriz_b
    resultado = [
        a00 * b00 + a01 * b10,
        a00 * b01 + a01 * b11,
        a10 * b00 + a11 * b10,
        a10 * b01 + a11 * b11
    ]
    
    Bitacora.registrar("INTERNAL", 
                      f"{id_proceso} multiplicó matrices 2x2 (valores 0-10), resultado=[{resultado[0]:.2f}, {resultado[1]:.2f}, {resultado[2]:.2f}, {resultado[3]:.2f}]",
                      reloj.obtener_tiempo())
    
    time.sleep(1)
    
    # 3. ENVIAR MENSAJE A P5
    reloj.incrementar()
    Bitacora.registrar("SEND", 
                      f"{id_proceso} -> P5_SEARCH mensaje='Hola P5, matrices multiplicadas'",
                      reloj.obtener_tiempo())
    
    timestamp_recibido = enviar_mensaje_a_proceso(
        id_proceso, "P5_SEARCH", 
        "Hola P5, matrices multiplicadas",
        reloj.obtener_tiempo(),
        "proceso5", 50055
    )
    
    reloj.actualizar(timestamp_recibido)


def iniciar_servidor():
    """Función principal que inicia el servidor gRPC"""
    id_proceso = "P3_MATRIX"
    reloj = RelojLamport()
    evento_recibido = threading.Event()
    
    servidor = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    services_pb2_grpc.add_MatrixServiceServicer_to_server(
        ServicioMatrices(id_proceso, reloj), servidor
    )
    services_pb2_grpc.add_MessageServiceServicer_to_server(
        ServicioMensajes(id_proceso, reloj, evento_recibido), servidor
    )
    
    servidor.add_insecure_port('[::]:50053')
    servidor.start()
    
    print(f"{id_proceso} servidor iniciado en puerto 50053")
    Bitacora.registrar("INTERNAL", f"{id_proceso} inicializado", reloj.obtener_tiempo())
    
    threading.Thread(target=tarea_proceso3, args=(id_proceso, reloj, evento_recibido), daemon=True).start()
    
    try:
        servidor.wait_for_termination()
    except KeyboardInterrupt:
        print(f"\n{id_proceso} detenido")
        servidor.stop(0)


if __name__ == '__main__':
    iniciar_servidor()
