"""
CLIENTE DE PRUEBA
Este script prueba los 3 procesos enviando peticiones RPC
y verificando que el reloj de Lamport funciona correctamente.
"""

import grpc
import services_pb2
import services_pb2_grpc
import time

class RelojLamportCliente:
    """Reloj de Lamport para el cliente"""
    def __init__(self):
        self.tiempo = 0
    
    def incrementar(self):
        """Incrementa el reloj antes de enviar"""
        self.tiempo += 1
        return self.tiempo
    
    def actualizar(self, tiempo_recibido):
        """Actualiza al recibir respuesta"""
        self.tiempo = max(self.tiempo, tiempo_recibido) + 1
        return self.tiempo
    
    def obtener_tiempo(self):
        return self.tiempo


def probar_proceso1_matematicas():
    """Prueba el proceso 1: operaciones matemáticas"""
    print("\n" + "="*60)
    print("PROBANDO PROCESO 1: OPERACIONES MATEMATICAS")
    print("="*60)
    
    reloj = RelojLamportCliente()
    
    # Conectar al servidor del proceso 1 (puerto 50051)
    canal = grpc.insecure_channel('localhost:50051')
    cliente = services_pb2_grpc.MathServiceStub(canal)
    
    try:
        # Prueba 1: SUMA
        print("\n[ENVIANDO] 15.5 + 7.3")
        reloj.incrementar()
        peticion = services_pb2.MathRequest(
            sender_id="CLIENTE",
            num1=15.5,
            num2=7.3,
            timestamp=reloj.obtener_tiempo()
        )
        respuesta = cliente.Add(peticion)
        reloj.actualizar(respuesta.timestamp)
        print(f"[RESPUESTA] {respuesta.result} | clock_cliente={reloj.obtener_tiempo()}")
        
        # Prueba 2: RESTA
        print("\n[ENVIANDO] 100.0 - 45.7")
        reloj.incrementar()
        peticion = services_pb2.MathRequest(
            sender_id="CLIENTE",
            num1=100.0,
            num2=45.7,
            timestamp=reloj.obtener_tiempo()
        )
        respuesta = cliente.Subtract(peticion)
        reloj.actualizar(respuesta.timestamp)
        print(f"[RESPUESTA] {respuesta.result} | clock_cliente={reloj.obtener_tiempo()}")
        
        # Prueba 3: MULTIPLICACIÓN
        print("\n[ENVIANDO] 6.5 * 4.2")
        reloj.incrementar()
        peticion = services_pb2.MathRequest(
            sender_id="CLIENTE",
            num1=6.5,
            num2=4.2,
            timestamp=reloj.obtener_tiempo()
        )
        respuesta = cliente.Multiply(peticion)
        reloj.actualizar(respuesta.timestamp)
        print(f"[RESPUESTA] {respuesta.result} | clock_cliente={reloj.obtener_tiempo()}")
        
        # Prueba 4: DIVISIÓN
        print("\n[ENVIANDO] 50.0 / 2.5")
        reloj.incrementar()
        peticion = services_pb2.MathRequest(
            sender_id="CLIENTE",
            num1=50.0,
            num2=2.5,
            timestamp=reloj.obtener_tiempo()
        )
        respuesta = cliente.Divide(peticion)
        reloj.actualizar(respuesta.timestamp)
        print(f"[RESPUESTA] {respuesta.result} | clock_cliente={reloj.obtener_tiempo()}")
        
        # Prueba 5: DIVISIÓN POR CERO (error esperado)
        print("\n[ENVIANDO] 10.0 / 0 (probando manejo de error)")
        reloj.incrementar()
        peticion = services_pb2.MathRequest(
            sender_id="CLIENTE",
            num1=10.0,
            num2=0,
            timestamp=reloj.obtener_tiempo()
        )
        respuesta = cliente.Divide(peticion)
        reloj.actualizar(respuesta.timestamp)
        print(f"[RESPUESTA] {respuesta.status} | clock_cliente={reloj.obtener_tiempo()}")
        
        print("\n[OK] PROCESO 1: TODAS LAS PRUEBAS COMPLETADAS")
        
    except grpc.RpcError as e:
        print(f"[ERROR] {e.details()}")
        print("[AVISO] Asegurate de que el proceso1_matematicas.py este corriendo")


def probar_proceso2_promedio():
    """Prueba el proceso 2: cálculo de promedio"""
    print("\n" + "="*60)
    print("PROBANDO PROCESO 2: CALCULO DE PROMEDIO")
    print("="*60)
    
    reloj = RelojLamportCliente()
    
    # Conectar al servidor del proceso 2 (puerto 50052)
    canal = grpc.insecure_channel('localhost:50052')
    cliente = services_pb2_grpc.AverageServiceStub(canal)
    
    try:
        print("\n[ENVIANDO] Solicitando calculo de promedio de 50 numeros aleatorios...")
        reloj.incrementar()
        peticion = services_pb2.AverageRequest(
            sender_id="CLIENTE",
            timestamp=reloj.obtener_tiempo()
        )
        respuesta = cliente.CalculateAverage(peticion)
        reloj.actualizar(respuesta.timestamp)
        
        print(f"\n[RESPUESTA] Respuesta recibida:")
        print(f"   - Cantidad de numeros: {len(respuesta.numbers)}")
        print(f"   - Promedio calculado: {respuesta.average:.4f}")
        print(f"   - Primeros 10 numeros: {[f'{n:.2f}' for n in respuesta.numbers[:10]]}")
        print(f"   - clock_cliente={reloj.obtener_tiempo()}")
        
        print("\n[OK] PROCESO 2: PRUEBA COMPLETADA")
        
    except grpc.RpcError as e:
        print(f"[ERROR] {e.details()}")
        print("[AVISO] Asegurate de que el proceso2_promedio.py este corriendo")


def probar_proceso3_matrices():
    """Prueba el proceso 3: multiplicación de matrices"""
    print("\n" + "="*60)
    print("PROBANDO PROCESO 3: MULTIPLICACION DE MATRICES")
    print("="*60)
    
    reloj = RelojLamportCliente()
    
    # Conectar al servidor del proceso 3 (puerto 50053)
    canal = grpc.insecure_channel('localhost:50053')
    cliente = services_pb2_grpc.MatrixServiceStub(canal)
    
    try:
        print("\n[ENVIANDO] Solicitando multiplicacion de matrices 2x2...")
        reloj.incrementar()
        peticion = services_pb2.MatrixRequest(
            sender_id="CLIENTE",
            timestamp=reloj.obtener_tiempo()
        )
        respuesta = cliente.MultiplyMatrices(peticion)
        reloj.actualizar(respuesta.timestamp)
        
        # Formatear matrices para mostrar
        ma = respuesta.matrix_a.values
        mb = respuesta.matrix_b.values
        res = respuesta.result.values
        
        print(f"\n[RESPUESTA] Respuesta recibida:")
        print(f"\n   Matriz A:")
        print(f"   | {ma[0]:6.2f}  {ma[1]:6.2f} |")
        print(f"   | {ma[2]:6.2f}  {ma[3]:6.2f} |")
        
        print(f"\n   Matriz B:")
        print(f"   | {mb[0]:6.2f}  {mb[1]:6.2f} |")
        print(f"   | {mb[2]:6.2f}  {mb[3]:6.2f} |")
        
        print(f"\n   Resultado (A * B):")
        print(f"   | {res[0]:6.2f}  {res[1]:6.2f} |")
        print(f"   | {res[2]:6.2f}  {res[3]:6.2f} |")
        
        print(f"\n   - clock_cliente={reloj.obtener_tiempo()}")
        
        print("\n[OK] PROCESO 3: PRUEBA COMPLETADA")
        
    except grpc.RpcError as e:
        print(f"[ERROR] {e.details()}")
        print("[AVISO] Asegurate de que el proceso3_matrices.py este corriendo")


def menu_principal():
    """Menú interactivo para probar los procesos"""
    print("\n" + "="*60)
    print("CLIENTE DE PRUEBA - SISTEMA DISTRIBUIDO")
    print("="*60)
    print("\nOpciones:")
    print("1. Probar Proceso 1 (Operaciones Matematicas)")
    print("2. Probar Proceso 2 (Calculo de Promedio)")
    print("3. Probar Proceso 3 (Multiplicacion de Matrices)")
    print("4. Probar TODOS los procesos")
    print("5. Salir")
    print("="*60)


if __name__ == '__main__':
    while True:
        menu_principal()
        opcion = input("\n> Selecciona una opcion: ")
        
        if opcion == '1':
            probar_proceso1_matematicas()
        elif opcion == '2':
            probar_proceso2_promedio()
        elif opcion == '3':
            probar_proceso3_matrices()
        elif opcion == '4':
            probar_proceso1_matematicas()
            time.sleep(1)
            probar_proceso2_promedio()
            time.sleep(1)
            probar_proceso3_matrices()
        elif opcion == '5':
            print("\n[SALIR] Hasta luego!")
            break
        else:
            print("\n[ERROR] Opcion invalida")
        
        input("\n[ENTER] Presiona ENTER para continuar...")
