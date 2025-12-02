"""
PROCESO 3: SERVICIO DE MULTIPLICACIÓN DE MATRICES
Este proceso genera dos matrices 2x2 con números aleatorios entre 0 y 10,
y las multiplica.
"""

import grpc
from concurrent import futures
import time
import random
import services_pb2
import services_pb2_grpc

class RelojLamport:
    """
    Reloj de Lamport para mantener el orden lógico de eventos.
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
        Regla: tiempo = max(tiempo_local, tiempo_recibido) + 1
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
        """Registra un evento con timestamp"""
        marca_temporal = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{marca_temporal}] [{tipo_evento}] {detalles} clock={valor_reloj}")


class ServicioMatrices(services_pb2_grpc.MatrixServiceServicer):
    """
    Implementación del servicio de multiplicación de matrices 2x2.
    """
    
    def __init__(self, id_proceso):
        """
        Constructor del servicio.
        id_proceso: identificador único (ej: "P3_MATRIX")
        """
        self.id_proceso = id_proceso
        self.reloj = RelojLamport()
    
    def generar_matriz_aleatoria_2x2(self):
        """
        Genera una matriz 2x2 con valores aleatorios entre 0 y 10.
        
        La matriz se representa como una lista de 4 elementos:
        [a00, a01, a10, a11]
        
        Esto representa:
        | a00  a01 |
        | a10  a11 |
        """
        return [random.uniform(0, 10) for _ in range(4)]
    
    def multiplicar_matrices_2x2(self, matriz_a, matriz_b):
        """
        Multiplica dos matrices 2x2.
        
        Matriz A:           Matriz B:
        | a00  a01 |    *   | b00  b01 |
        | a10  a11 |        | b10  b11 |
        
        Resultado:
        | c00  c01 |
        | c10  c11 |
        
        Fórmulas de multiplicación de matrices:
        c00 = a00*b00 + a01*b10
        c01 = a00*b01 + a01*b11
        c10 = a10*b00 + a11*b10
        c11 = a10*b01 + a11*b11
        """
        # Desempaquetar valores de matriz A
        a00, a01, a10, a11 = matriz_a
        
        # Desempaquetar valores de matriz B
        b00, b01, b10, b11 = matriz_b
        
        # Calcular cada elemento de la matriz resultado
        c00 = a00 * b00 + a01 * b10
        c01 = a00 * b01 + a01 * b11
        c10 = a10 * b00 + a11 * b10
        c11 = a10 * b01 + a11 * b11
        
        # Retornar matriz resultado como lista de 4 elementos
        return [c00, c01, c10, c11]
    
    def MultiplyMatrices(self, peticion, contexto):
        """
        Método RPC que genera y multiplica dos matrices 2x2.
        peticion: contiene sender_id y timestamp
        retorna: las dos matrices generadas y su producto
        """
        # 1. Actualizar reloj con timestamp recibido
        self.reloj.actualizar(peticion.timestamp)
        Bitacora.registrar("RECEIVE", 
                          f"{self.id_proceso} <- {peticion.sender_id} operacion=MULTIPLICAR_MATRICES",
                          self.reloj.obtener_tiempo())
        
        # 2. Generar dos matrices aleatorias 2x2
        matriz_a = self.generar_matriz_aleatoria_2x2()
        matriz_b = self.generar_matriz_aleatoria_2x2()
        
        # 3. Multiplicar las matrices
        matriz_resultado = self.multiplicar_matrices_2x2(matriz_a, matriz_b)
        
        # 4. Incrementar reloj (evento interno: cálculo completado)
        self.reloj.incrementar()
        
        # 5. Registrar en bitácora con formato legible de matrices
        Bitacora.registrar("INTERNAL", 
                          f"{self.id_proceso} multiplicó matrices:\n" +
                          f"  Matriz A: [[{matriz_a[0]:.2f}, {matriz_a[1]:.2f}], [{matriz_a[2]:.2f}, {matriz_a[3]:.2f}]]\n" +
                          f"  Matriz B: [[{matriz_b[0]:.2f}, {matriz_b[1]:.2f}], [{matriz_b[2]:.2f}, {matriz_b[3]:.2f}]]\n" +
                          f"  Resultado: [[{matriz_resultado[0]:.2f}, {matriz_resultado[1]:.2f}], [{matriz_resultado[2]:.2f}, {matriz_resultado[3]:.2f}]]",
                          self.reloj.obtener_tiempo())
        
        # 6. Retornar respuesta con las tres matrices
        return services_pb2.MatrixResponse(
            matrix_a=services_pb2.Matrix2x2(values=matriz_a),
            matrix_b=services_pb2.Matrix2x2(values=matriz_b),
            result=services_pb2.Matrix2x2(values=matriz_resultado),
            timestamp=self.reloj.obtener_tiempo()
        )


def iniciar_servidor():
    """
    Función principal que inicia el servidor gRPC.
    Este servidor escucha en el puerto 50053.
    """
    id_proceso = "P3_MATRIX"
    
    # Crear servidor gRPC con pool de 10 hilos
    servidor = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Registrar el servicio de matrices en el servidor
    services_pb2_grpc.add_MatrixServiceServicer_to_server(
        ServicioMatrices(id_proceso), servidor
    )
    
    # Escuchar en el puerto 50053
    servidor.add_insecure_port('[::]:50053')
    
    # Iniciar el servidor
    servidor.start()
    print(f"✓ {id_proceso} servidor iniciado en puerto 50053")
    Bitacora.registrar("INTERNAL", f"{id_proceso} inicializado", 0)
    
    # Mantener el servidor corriendo indefinidamente
    servidor.wait_for_termination()


# Punto de entrada del programa
if __name__ == '__main__':
    iniciar_servidor()

