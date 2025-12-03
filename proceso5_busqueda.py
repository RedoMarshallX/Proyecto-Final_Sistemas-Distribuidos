"""
PROCESO 5: SERVICIO DE BÚSQUEDA LINEAL
Este proceso implementa un servidor gRPC que busca los números 3, 22 y 50
en un arreglo de 200 números aleatorios (rango 0-100) usando búsqueda lineal.
"""

import grpc
from concurrent import futures
import time
import random
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


class ServicioBusqueda(services_pb2_grpc.SearchServiceServicer):
    """
    Implementación del servicio de búsqueda lineal.
    Hereda de la clase generada por gRPC.
    """
    
    def __init__(self, id_proceso):
        """
        Constructor del servicio.
        id_proceso: identificador único de este proceso (ej: "P5_SEARCH")
        """
        self.id_proceso = id_proceso
        self.reloj = RelojLamport()  # Crea su propio reloj de Lamport
        self.numeros_objetivo = [3, 22, 50]  # Números a buscar
    
    def busqueda_lineal(self, arr, objetivo):
        """
        Implementación de búsqueda lineal.
        Busca un valor en el arreglo y retorna su posición.
        Retorna -1 si no se encuentra.
        """
        for i, valor in enumerate(arr):
            if valor == objetivo:
                return i
        return -1
    
    def LinearSearch(self, peticion, contexto):
        """
        Método RPC para buscar los números 3, 22 y 50 en un arreglo
        de 200 números aleatorios usando búsqueda lineal.
        peticion: contiene sender_id y timestamp
        contexto: información de la conexión gRPC
        """
        # 1. Actualizar el reloj con el timestamp recibido
        self.reloj.actualizar(peticion.timestamp)
        
        # 2. Registrar que recibimos una petición
        Bitacora.registrar("RECEIVE", 
                          f"{self.id_proceso} <- {peticion.sender_id} operacion=BUSQUEDA_LINEAL([3, 22, 50])",
                          self.reloj.obtener_tiempo())
        
        # 3. Generar 200 números aleatorios entre 0 y 100
        self.reloj.incrementar()
        numeros = [random.randint(0, 100) for _ in range(200)]
        Bitacora.registrar("INTERNAL", 
                          f"{self.id_proceso} generó 200 números aleatorios",
                          self.reloj.obtener_tiempo())
        
        # 4. Buscar cada número objetivo
        resultados = []
        for objetivo in self.numeros_objetivo:
            self.reloj.incrementar()
            posicion = self.busqueda_lineal(numeros, objetivo)
            encontrado = posicion != -1
            
            # Crear resultado de búsqueda
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
        
        # 5. Retornar la respuesta con los números y resultados de búsqueda
        return services_pb2.SearchResponse(
            numbers=numeros,
            results=resultados,
            timestamp=self.reloj.obtener_tiempo()
        )


def iniciar_servidor():
    """
    Función principal que inicia el servidor gRPC.
    El servidor escucha en el puerto 50055.
    """
    id_proceso = "P5_SEARCH"
    
    # Crear servidor con pool de 10 hilos
    servidor = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Registrar el servicio de búsqueda en el servidor
    services_pb2_grpc.add_SearchServiceServicer_to_server(
        ServicioBusqueda(id_proceso), servidor
    )
    
    # Escuchar en el puerto 50055 (todos los interfaces)
    servidor.add_insecure_port('[::]:50055')
    
    # Iniciar el servidor
    servidor.start()
    print(f"✓ {id_proceso} servidor iniciado en puerto 50055")
    Bitacora.registrar("INTERNAL", f"{id_proceso} inicializado", 0)
    
    # Mantener el servidor corriendo
    servidor.wait_for_termination()


# Punto de entrada del programa
if __name__ == '__main__':
    iniciar_servidor()
