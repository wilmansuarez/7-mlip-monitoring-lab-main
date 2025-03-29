from kafka import KafkaConsumer
from prometheus_client import Counter, Histogram, start_http_server
import time

# TODO: Update the Kafka topic to the movie log of your team
# topic = 'movielogN'
topic = 'recommendations'

start_http_server(8765)  # Inicia el servidor HTTP para Prometheus

# Métrica para contar las solicitudes por código de estado HTTP
REQUEST_COUNT = Counter(
    'request_count', 
    'Recommendation Request Count', 
    ['http_status']  # Etiqueta para el código de estado HTTP
)

# Métrica para latencia de las solicitudes
REQUEST_LATENCY = Histogram(
    'request_latency_seconds', 
    'Request latency in seconds'
)

def main():
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        group_id=topic,
        enable_auto_commit=True,
        auto_commit_interval_ms=1000
    )

    for message in consumer:
        event = message.value.decode('utf-8')
        values = event.split(',')

        if 'recommendation request' in values[2]:
            # Extraemos el código de estado HTTP
            # Suponemos que el código de estado está en una posición específica en el mensaje, por ejemplo, en values[3]
            status_code = values[3].strip()  # Asegúrate de ajustar el índice según el formato de tu mensaje

            # Incrementar el contador de solicitudes con el código de estado
            REQUEST_COUNT.labels(http_status=status_code).inc()

            # Actualizar la latencia de la solicitud
            time_taken_str = values[-1].strip()  # Obtener el valor de la latencia
            # Eliminar la unidad "ms" y convertir el valor a float
            time_taken = float(time_taken_str.replace('ms', '').strip())  # Eliminar 'ms' y convertir a float

            # Observamos la latencia en segundos
            REQUEST_LATENCY.observe(time_taken / 1000)  # Convertir milisegundos a segundos

if __name__ == "__main__":
    main()
