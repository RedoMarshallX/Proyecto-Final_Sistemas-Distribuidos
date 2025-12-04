"""
PROCESO 4: SERVICIO DE ORDENAMIENTO QUICK SORT (CON COMUNICACIÓN)
Este proceso:
1. Evento interno: Ordenar 100 números aleatorios (0 a 100) usando Quick Sort
2. Envía mensaje a P2
3. Recibe mensaje de P5
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


class ServicioOrdenamiento(services_pb2_grpc.SortServiceServicer):
    """Implementación del servicio de ordenamiento Quick Sort"""
    
    def __init__(self, id_proceso, reloj):
        self.id_proceso = id_proceso
        self.reloj = reloj
    
    def quick_sort(self, arr):
        if len(arr) <= 1:
            return arr
        
        pivote = arr[len(arr) // 2]
        menores = [x for x in arr if x < pivote]
        iguales = [x for x in arr if x == pivote]
        mayores = [x for x in arr if x > pivote]
        
        return self.quick_sort(menores) + iguales + self.quick_sort(mayores)
    
    def QuickSort(self, peticion, contexto):
        self.reloj.actualizar(peticion.timestamp)
        Bitacora.registrar("RECEIVE", 
                          f"{self.id_proceso} <- {peticion.sender_id} operacion=QUICKSORT(100 números)",
                          self.reloj.obtener_tiempo())
        
        self.reloj.incrementar()
        numeros_originales = [random.randint(0, 100) for _ in range(100)]
        Bitacora.registrar("INTERNAL", 
                          f"{self.id_proceso} generó 100 números aleatorios",
                          self.reloj.obtener_tiempo())
        
        self.reloj.incrementar()
        numeros_ordenados = self.quick_sort(numeros_originales.copy())
        Bitacora.registrar("INTERNAL", 
                          f"{self.id_proceso} ordenó números con Quick Sort",
                          self.reloj.obtener_tiempo())
        
        return services_pb2.SortResponse(
            original_numbers=numeros_originales,
            sorted_numbers=numeros_ordenados,
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


def tarea_proceso4(id_proceso, reloj):
    """
    Tarea específica del Proceso 4:
    1. Evento interno: Ordenar 100 números aleatorios (0 a 100) usando Quick Sort
    2. Envía mensaje a P2
    3. Recibe mensaje de P5 (el servidor lo recibe automáticamente)
    """
    time.sleep(5)
    
    # 1. EVENTO INTERNO: Ordenar 100 números aleatorios (0-100) con Quick Sort
    reloj.incrementar()
    numeros = [random.randint(0, 100) for _ in range(100)]
    
    # Función Quick Sort interna
    def quick_sort_local(arr):
        if len(arr) <= 1:
            return arr
        pivote = arr[len(arr) // 2]
        menores = [x for x in arr if x < pivote]
        iguales = [x for x in arr if x == pivote]
        mayores = [x for x in arr if x > pivote]
        return quick_sort_local(menores) + iguales + quick_sort_local(mayores)
    
    numeros_ordenados = quick_sort_local(numeros.copy())
    
    Bitacora.registrar("INTERNAL", 
                      f"{id_proceso} ordenó 100 números (0-100) con Quick Sort, primeros 5: {numeros_ordenados[:5]}, últimos 5: {numeros_ordenados[-5:]}",
                      reloj.obtener_tiempo())
    
    time.sleep(1)
    
    # 2. ENVIAR MENSAJE A P2
    reloj.incrementar()
    Bitacora.registrar("SEND", 
                      f"{id_proceso} -> P2_AVG mensaje='Hola P2, ordenamiento completado'",
                      reloj.obtener_tiempo())
    
    timestamp_recibido = enviar_mensaje_a_proceso(
        id_proceso, "P2_AVG", 
        f"Hola P2, ordenamiento completado: {len(numeros_ordenados)} números",
        reloj.obtener_tiempo(),
        "proceso2", 50052
    )
    
    reloj.actualizar(timestamp_recibido)
    
    # 3. El mensaje de P5 se recibe automáticamente por el servidor
    print(f"[INFO] {id_proceso} esperando mensaje de P5...")


def iniciar_servidor():
    """Función principal que inicia el servidor gRPC"""
    id_proceso = "P4_SORT"
    reloj = RelojLamport()
    
    servidor = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    services_pb2_grpc.add_SortServiceServicer_to_server(
        ServicioOrdenamiento(id_proceso, reloj), servidor
    )
    services_pb2_grpc.add_MessageServiceServicer_to_server(
        ServicioMensajes(id_proceso, reloj), servidor
    )
    
    servidor.add_insecure_port('[::]:50054')
    servidor.start()
    
    print(f"{id_proceso} servidor iniciado en puerto 50054")
    Bitacora.registrar("INTERNAL", f"{id_proceso} inicializado", reloj.obtener_tiempo())
    
    threading.Thread(target=tarea_proceso4, args=(id_proceso, reloj), daemon=True).start()
    
    try:
        servidor.wait_for_termination()
    except KeyboardInterrupt:
        print(f"\n{id_proceso} detenido")
        servidor.stop(0)


if __name__ == '__main__':
    iniciar_servidor()
