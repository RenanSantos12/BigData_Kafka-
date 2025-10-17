from kafka import KafkaProducer
from time import sleep

producer = KafkaProducer(bootstrap_servers='localhost:9092')
arquivo_producer = r'C:\Users\raraujo\Desktop\Praticando Kafka\dados_exemplo (1).txt'

with open(arquivo_producer, 'r') as file:
    texto = file.readlines()
    print(texto)

for mesagem in texto:
    producer.send('Renan_teste', mesagem.encode('utf-8'))