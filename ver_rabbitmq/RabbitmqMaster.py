import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='master_queue')


def callback(ch, method, properties, body):
    print(f"{body}")


channel.basic_consume(queue='master_queue', auto_ack=True, on_message_callback=callback)

print("Waiting for messages...")

try:
    print("Use Ctrl+c to exit")
    channel.start_consuming()
except KeyboardInterrupt:
    print("Exiting")
    channel.stop_consuming()
    connection.close()
    exit()
