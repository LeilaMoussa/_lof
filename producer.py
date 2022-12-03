from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import time, sys

# python3 producer.py mouse-source-topic rtlofs/labeled.keyed.mouse 0 2
# python3 producer.py kdd9910pc-source-topic rtlofs/sampled.labeled.keyed.kddcup._10_percent 0 41
# python3 producer.py dummy-topic ../tiny-dummy.txt 0 2
# python3 producer.py shuttle-topic rtlofs/labeled.keyed.shuttle 0 9
# python3 producer.py wilt-topic rtlofs/labeled.keyed.wilt 0 5
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

    _in = 0
    _out = 0
    with open(source_file) as f:
        count = 0
        while True:
            line = f.readline()
            if not line:
                break
            line = line.strip().strip('\n')
            words = line.split()
            if len(words) > d + 1:  # if labeled
                if words[d + 1] == "0":
                    _in += 1
                elif words[d + 1] == "1":
                    _out += 1
            line = ""
            key = words[0]
            for i in range(1, d+1): # account for key at idx 0
                line += words[i] + " "
            line.strip()
            print(count + 1, " - ", key, " - ", line)
            producer.send(topic_name, value=line.encode(), key=key.encode())
            count += 1
            time.sleep(interval)
    print("in, out", _in, _out)

    producer.close()
