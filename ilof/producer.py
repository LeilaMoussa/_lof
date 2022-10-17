from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import time, sys

def gen(infile: str):
    with open(infile) as f:
        yield f.readline()

if __name__ == '__main__':
    [_, topic_name, source_file, interval_sec] = sys.argv
    interval = int(interval_sec)
    data = gen(source_file)

    admin = KafkaAdminClient(bootstrap_servers='localhost:9092')

    try:
        topic = NewTopic(name=topic_name,
                        num_partitions=1,
                        replication_factor=1)
        admin.create_topics([topic])
    except TopicAlreadyExistsError:
        pass

    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    for line in data:
        print("at", line, '.')
        if not line:
            break
        producer.send(topic_name, line.strip().strip('\n').encode())
        time.sleep(interval)

    producer.close()
