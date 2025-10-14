from kafka import KafkaProducer
from time import sleep

producer = KafkaProducer(bootstrap_servers='localhost:9092')

while True:
    messagem = input('Digite uma mensagem para o kafka :')
    if messagem.lower() == 'sair':
        break
    producer.send(topic='Renan_teste', value=messagem.encode('utf-8'))
    producer.flush()
    print(f"Mensagem enviada: {messagem}")
    sleep(0.5)