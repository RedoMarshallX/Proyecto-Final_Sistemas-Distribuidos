# Dockerfile para los procesos del sistema distribuido
FROM python:3.11-slim

# Establecer directorio de trabajo
WORKDIR /app

# Instalar dependencias de gRPC
RUN pip install --no-cache-dir grpcio grpcio-tools

# Copiar archivos proto y Python
COPY services.proto .
COPY services_pb2.py .
COPY services_pb2_grpc.py .

# Los archivos de proceso se copiarán desde docker-compose

# Exponer puertos (50051-50055)
EXPOSE 50051 50052 50053 50054 50055

# El comando se especificará en docker-compose
CMD ["python", "-u"]
