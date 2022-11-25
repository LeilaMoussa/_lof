from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import time, sys

# python3 producer.py mouse-source-topic ../mouse.txt 0 2
if __name__ == '__main__':
    [_, topic_name, source_file, interval_sec, d] = sys.argv
    interval = float(interval_sec)
    d = int(d)

    admin = KafkaAdminClient(bootstrap_servers='localhost:9092')

    try:
        topic = NewTopic(name=topic_name,
                        num_partitions=1,
                        replication_factor=1)
        admin.create_topics([topic])
    except TopicAlreadyExistsError:
        pass

    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    with open(source_file) as f:
        count = 0
        while True:
            line = f.readline()
            if not line:
                break
            line = line.strip().strip('\n')
            words = line.split()
            line = ""
            for i in range(d):
                line += words[i] + " "
            line.strip()
            print(count + 1, " - ", line)
            producer.send(topic_name, line.encode())
            count += 1
            time.sleep(interval)

    producer.close()
