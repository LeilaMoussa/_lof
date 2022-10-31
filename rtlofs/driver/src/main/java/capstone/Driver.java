package capstone;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import com.google.common.collect.MinMaxPriorityQueue;

import java.util.Arrays;
import java.util.Properties;

import io.github.cdimascio.dotenv.Dotenv;
import io.github.cdimascio.dotenv.DotenvException;

public class Driver {

    public static void main(String args[]) {
        Dotenv dotenv = Dotenv.load();
        // .configure()
        // .directory("../../../../.env")
        // .ignoreIfMalformed()
        // .ignoreIfMissing()
        // .load();

    dotenv.get("KAFKA_APP_ID");

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, dotenv.get("KAFKA_APP_ID"));
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, dotenv.get("KAFKA_BROKER"));
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, String> rawData = builder.stream(dotenv.get("SOURCE_TOPIC"));

    KStream<String, Point> data = rawData.flatMapValues(value -> Arrays.asList(Parser.parse(value,
                                                                                " ",
                                                                                Integer.parseInt(dotenv.get("DIMENSIONS")))));

    final String algorithm = dotenv.get("ALGORITHM");
    switch (algorithm) {
        case "ILOF":
            ILOF.detectOutliers(data, Integer.parseInt(dotenv.get("k")),
                                    Integer.parseInt(dotenv.get("topN")),
                                    Double.parseDouble(dotenv.get("LOF_THRESHOLD")),
                                    dotenv.get("DISTANCE_MEASURE"));
            //data.foreach(ILOF.process());
            break;
        case "RLOF":
            //
            break;
        case "MILOF":
            //
            break;
        case "DILOF":
            //
            break;
        case "C_LOF":
            //
            break;
        default:
            throw new DotenvException("No Algorithm called " + algorithm + ".\n");
    }

    KafkaStreams streams = new KafkaStreams(builder.build(), props);
    // streams.cleanUp(); // ?
    streams.start();
    }
}
