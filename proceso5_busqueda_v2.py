"""
PROCESO 5: SERVICIO DE BÚSQUEDA LINEAL (CON COMUNICACIÓN)
Este proceso:
1. Recibe mensaje de P3
2. Evento interno: promedio de 50 números aleatorios
3. Envía mensaje a P4
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


class ServicioBusqueda(services_pb2_grpc.SearchServiceServicer):
    """Implementación del servicio de búsqueda lineal"""
    
    def __init__(self, id_proceso, reloj):
        self.id_proceso = id_proceso
        self.reloj = reloj
        self.numeros_objetivo = [3, 22, 50]
    
    def busqueda_lineal(self, arr, objetivo):
        for i, valor in enumerate(arr):
            if valor == objetivo:
                return i
        return -1
    
    def LinearSearch(self, peticion, contexto):
        self.reloj.actualizar(peticion.timestamp)
        Bitacora.registrar("RECEIVE", 
                          f"{self.id_proceso} <- {peticion.sender_id} operacion=BUSQUEDA_LINEAL([3, 22, 50])",
                          self.reloj.obtener_tiempo())
        
        self.reloj.incrementar()
        numeros = [random.randint(0, 100) for _ in range(200)]
        Bitacora.registrar("INTERNAL", 
                          f"{self.id_proceso} generó 200 números aleatorios",
                          self.reloj.obtener_tiempo())
        
        resultados = []
        for objetivo in self.numeros_objetivo:
            self.reloj.incrementar()
            posicion = self.busqueda_lineal(numeros, objetivo)
            encontrado = posicion != -1
            
            resultado = services_pb2.SearchResult(
                value=objetivo,
                position=posicion if encontrado else -1,
                found=encontrado
            )
            resultados.append(resultado)
            
            estado = "ENCONTRADO" if encontrado else "NO ENCONTRADO"
            pos_texto = f"posición={posicion}" if encontrado else "no existe"
            Bitacora.registrar("INTERNAL", 
                              f"{self.id_proceso} buscó {objetivo}: {estado} ({pos_texto})",
                              self.reloj.obtener_tiempo())
        
        return services_pb2.SearchResponse(
            numbers=numeros,
            results=resultados,
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
        
        # Señalar que recibimos el mensaje de P3
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


def tarea_proceso5(id_proceso, reloj, evento_recibido):
    """
    Tarea específica del Proceso 5:
    1. Recibe mensaje de P3 (espera)
    2. Evento interno: promedio de 50 números aleatorios
    3. Envía mensaje a P4
    """
    # 1. ESPERAR MENSAJE DE P3
    print(f"[INFO] {id_proceso} esperando mensaje de P3...")
    evento_recibido.wait(timeout=15)  # Espera hasta 15 segundos
    
    time.sleep(1)
    
    # 2. EVENTO INTERNO: Promedio de 50 números
    reloj.incrementar()
    numeros = [random.uniform(0, 10) for _ in range(50)]
    promedio = sum(numeros) / len(numeros)
    
    Bitacora.registrar("INTERNAL", 
                      f"{id_proceso} calculó promedio de 50 números = {promedio:.4f}",
                      reloj.obtener_tiempo())
    
    time.sleep(1)
    
    # 3. ENVIAR MENSAJE A P4
    reloj.incrementar()
    Bitacora.registrar("SEND", 
                      f"{id_proceso} -> P4_SORT mensaje='Hola P4, promedio: {promedio:.4f}'",
                      reloj.obtener_tiempo())
    
    timestamp_recibido = enviar_mensaje_a_proceso(
        id_proceso, "P4_SORT", 
        f"Hola P4, promedio: {promedio:.4f}",
        reloj.obtener_tiempo(),
        "proceso4", 50054
    )
    
    reloj.actualizar(timestamp_recibido)


def iniciar_servidor():
    """Función principal que inicia el servidor gRPC"""
    id_proceso = "P5_SEARCH"
    reloj = RelojLamport()
    evento_recibido = threading.Event()
    
    servidor = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    services_pb2_grpc.add_SearchServiceServicer_to_server(
        ServicioBusqueda(id_proceso, reloj), servidor
    )
    services_pb2_grpc.add_MessageServiceServicer_to_server(
        ServicioMensajes(id_proceso, reloj, evento_recibido), servidor
    )
    
    servidor.add_insecure_port('[::]:50055')
    servidor.start()
    
    print(f"✓ {id_proceso} servidor iniciado en puerto 50055")
    Bitacora.registrar("INTERNAL", f"{id_proceso} inicializado", reloj.obtener_tiempo())
    
    threading.Thread(target=tarea_proceso5, args=(id_proceso, reloj, evento_recibido), daemon=True).start()
    
    try:
        servidor.wait_for_termination()
    except KeyboardInterrupt:
        print(f"\n{id_proceso} detenido")
        servidor.stop(0)


if __name__ == '__main__':
    iniciar_servidor()
