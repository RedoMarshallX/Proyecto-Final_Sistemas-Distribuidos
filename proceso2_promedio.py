"""
PROCESO 2: SERVICIO DE CÁLCULO DE PROMEDIO
Este proceso genera 50 números aleatorios entre 0 y 10,
y calcula su promedio.
"""

import grpc
from concurrent import futures
import time
import random  # Para generar números aleatorios
import services_pb2
import services_pb2_grpc

class RelojLamport:
    """
    Reloj de Lamport para ordenar eventos en el sistema distribuido.
    """
    def __init__(self):
        self.tiempo = 0
    
    def incrementar(self):
        """Incrementa el reloj en 1"""
        self.tiempo += 1
        return self.tiempo
    
    def actualizar(self, tiempo_recibido):
        """
        Actualiza el reloj al recibir un mensaje.
        Usa la regla de Lamport: max(local, recibido) + 1
        """
        self.tiempo = max(self.tiempo, tiempo_recibido) + 1
        return self.tiempo
    
    def obtener_tiempo(self):
        """Retorna el valor actual del reloj"""
        return self.tiempo


class Bitacora:
    """
    Sistema de registro de eventos (bitácora).
    """
    @staticmethod
    def registrar(tipo_evento, detalles, valor_reloj):
        """Registra un evento en la bitácora con timestamp"""
        marca_temporal = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{marca_temporal}] [{tipo_evento}] {detalles} clock={valor_reloj}")


class ServicioPromedio(services_pb2_grpc.AverageServiceServicer):
    """
    Implementación del servicio de cálculo de promedio.
    Genera números aleatorios y calcula su promedio.
    """
    
    def __init__(self, id_proceso):
        """
        Constructor del servicio.
        id_proceso: identificador único (ej: "P2_AVERAGE")
        """
        self.id_proceso = id_proceso
        self.reloj = RelojLamport()
    
    def CalculateAverage(self, peticion, contexto):
        """
        Método RPC que calcula el promedio de 50 números aleatorios.
        peticion: contiene sender_id y timestamp
        retorna: lista de números generados y su promedio
        """
        # 1. Actualizar reloj con el timestamp del mensaje recibido
        self.reloj.actualizar(peticion.timestamp)
        Bitacora.registrar("RECEIVE", 
                          f"{self.id_proceso} <- {peticion.sender_id} operacion=CALCULAR_PROMEDIO",
                          self.reloj.obtener_tiempo())
        
        # 2. Generar 50 números aleatorios entre 0 y 10
        # random.uniform(a, b) genera un número flotante aleatorio entre a y b
        numeros = [random.uniform(0, 10) for _ in range(50)]
        
        # 3. Calcular el promedio
        # sum() suma todos los elementos, len() cuenta cuántos hay
        promedio = sum(numeros) / len(numeros)
        
        # 4. Incrementar reloj (evento interno: cálculo completado)
        self.reloj.incrementar()
        Bitacora.registrar("INTERNAL", 
                          f"{self.id_proceso} generó 50 números aleatorios, promedio={promedio:.2f}",
                          self.reloj.obtener_tiempo())
        
        # 5. Retornar respuesta con los números y el promedio
        return services_pb2.AverageResponse(
            numbers=numeros,           # Lista de los 50 números
            average=promedio,          # Valor del promedio
            timestamp=self.reloj.obtener_tiempo()
        )


def iniciar_servidor():
    """
    Función principal que inicia el servidor gRPC.
    Este servidor escucha en el puerto 50052.
    """
    id_proceso = "P2_AVERAGE"
    
    # Crear servidor gRPC con pool de 10 hilos
    servidor = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Registrar el servicio de promedio en el servidor
    services_pb2_grpc.add_AverageServiceServicer_to_server(
        ServicioPromedio(id_proceso), servidor
    )
    
    # Escuchar en el puerto 50052
    servidor.add_insecure_port('[::]:50052')
    
    # Iniciar el servidor
    servidor.start()
    print(f"✓ {id_proceso} servidor iniciado en puerto 50052")
    Bitacora.registrar("INTERNAL", f"{id_proceso} inicializado", 0)
    
    # Mantener el servidor corriendo indefinidamente
    servidor.wait_for_termination()


# Punto de entrada del programa
if __name__ == '__main__':
    iniciar_servidor()
