import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='master_queue')

channel.basic_publish(exchange='', routing_key='master_queue', body="Hola")
print("Sent message: Hola")

channel.basic_publish(exchange='', routing_key='master_queue', body="Hola1")
print("Sent message: Hola1")

channel.basic_publish(exchange='', routing_key='master_queue', body="Hola2")
print("Sent message: Hola2")

connection.close()
