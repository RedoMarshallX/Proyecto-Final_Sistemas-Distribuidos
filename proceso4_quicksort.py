"""
PROCESO 4: SERVICIO DE ORDENAMIENTO QUICK SORT
Este proceso implementa un servidor gRPC que ordena 100 números aleatorios
en un rango de 0 a 100 usando el algoritmo Quick Sort.
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


class ServicioOrdenamiento(services_pb2_grpc.SortServiceServicer):
    """
    Implementación del servicio de ordenamiento Quick Sort.
    Hereda de la clase generada por gRPC.
    """
    
    def __init__(self, id_proceso):
        """
        Constructor del servicio.
        id_proceso: identificador único de este proceso (ej: "P4_SORT")
        """
        self.id_proceso = id_proceso
        self.reloj = RelojLamport()  # Crea su propio reloj de Lamport
    
    def quick_sort(self, arr):
        """
        Implementación del algoritmo Quick Sort.
        Ordena un arreglo de números de forma recursiva.
        """
        if len(arr) <= 1:
            return arr
        
        # Elegir el pivote (elemento del medio)
        pivote = arr[len(arr) // 2]
        
        # Dividir en tres partes: menores, iguales y mayores al pivote
        menores = [x for x in arr if x < pivote]
        iguales = [x for x in arr if x == pivote]
        mayores = [x for x in arr if x > pivote]
        
        # Ordenar recursivamente y concatenar
        return self.quick_sort(menores) + iguales + self.quick_sort(mayores)
    
    def QuickSort(self, peticion, contexto):
        """
        Método RPC para ordenar 100 números aleatorios usando Quick Sort.
        peticion: contiene sender_id y timestamp
        contexto: información de la conexión gRPC
        """
        # 1. Actualizar el reloj con el timestamp recibido
        self.reloj.actualizar(peticion.timestamp)
        
        # 2. Registrar que recibimos una petición
        Bitacora.registrar("RECEIVE", 
                          f"{self.id_proceso} <- {peticion.sender_id} operacion=QUICKSORT(100 números)",
                          self.reloj.obtener_tiempo())
        
        # 3. Generar 100 números aleatorios entre 0 y 100
        self.reloj.incrementar()
        numeros_originales = [random.randint(0, 100) for _ in range(100)]
        Bitacora.registrar("INTERNAL", 
                          f"{self.id_proceso} generó 100 números aleatorios",
                          self.reloj.obtener_tiempo())
        
        # 4. Aplicar Quick Sort
        self.reloj.incrementar()
        numeros_ordenados = self.quick_sort(numeros_originales.copy())
        Bitacora.registrar("INTERNAL", 
                          f"{self.id_proceso} ordenó números con Quick Sort",
                          self.reloj.obtener_tiempo())
        
        # 5. Retornar la respuesta con los números originales y ordenados
        return services_pb2.SortResponse(
            original_numbers=numeros_originales,
            sorted_numbers=numeros_ordenados,
            timestamp=self.reloj.obtener_tiempo()
        )


def iniciar_servidor():
    """
    Función principal que inicia el servidor gRPC.
    El servidor escucha en el puerto 50054.
    """
    id_proceso = "P4_SORT"
    
    # Crear servidor con pool de 10 hilos
    servidor = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Registrar el servicio de ordenamiento en el servidor
    services_pb2_grpc.add_SortServiceServicer_to_server(
        ServicioOrdenamiento(id_proceso), servidor
    )
    
    # Escuchar en el puerto 50054 (todos los interfaces)
    servidor.add_insecure_port('[::]:50054')
    
    # Iniciar el servidor
    servidor.start()
    print(f"{id_proceso} servidor iniciado en puerto 50054")
    Bitacora.registrar("INTERNAL", f"{id_proceso} inicializado", 0)
    
    # Mantener el servidor corriendo
    servidor.wait_for_termination()


# Punto de entrada del programa
if __name__ == '__main__':
    iniciar_servidor()
