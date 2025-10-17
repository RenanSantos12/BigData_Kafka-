from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic

admin = AdminClient({'bootstrap.servers': 'localhost:9092'})


def cria_topic(topico, replication, partition):

    novo_topic = NewTopic(topico, replication, partition)
    cria = admin.create_topics([novo_topic])
    for topic_, resultado in cria.items():
        resultado.result()
        print(resultado.result())
        print(f'Topic {topic_} cadastrado com sucesso!')


def deleta_topic(topico):
    try:
        resposta = admin.delete_topics([topico])
        for topico, futuro in resposta.items():
            futuro.result()
            print(futuro.result())# Aguarda o resultado da exclusão
        print(f'Deletei o tópico chamado {topico}')
    except Exception as e:
        print(f'Ocorreu um erro ao tentar deletar o tópico {topico}: {e}')



#topic = 'Renan_Kafka'
# deleta_topic(topic)
# cria_topic(topic, replication=1, partition=1)
topico_ = admin.list_topics()
for topic in topico_.topics:
    print(topic)
