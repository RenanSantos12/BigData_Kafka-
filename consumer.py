from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'Renan_teste',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        group_id='grupo-renan'         # nome do grupo do consumidor

)

print("Aguardando mensagens...\n")

for message in consumer:
    print(f"Mensagem recebida: {message.value.decode('utf-8')}")