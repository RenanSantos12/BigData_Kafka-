import  json
import producer_settings
from confluent_kafka import Producer

class Kafka(object):

    @staticmethod
    def json_producer(broker, name, topic):
        p = Producer(producer_settings.producer_settings_jason(broker))
        dados = name

        for dado in dados:
            try:
                p.poll(0)
                p.produce(
                    topic=topic,
                    value=json.dump(dado).encode('utf-8')
                )
            except Exception as e:
                print(e)
        p.flush()


