package com.tfs.poc.kafkastreams;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by srikumar.chari on 4/25/16.
 */
public class KafkaStreamsPoc {

    public static Logger logger = LogManager.getLogger(KafkaStreamsPoc.class.getSimpleName());

    public static void main(String[] args) {
        Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.JOB_ID_CONFIG, "wordcount-lambda-example");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Where to find the corresponding ZooKeeper ensemble.
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        streamsConfiguration.put(StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        streamsConfiguration.put(StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        streamsConfiguration.put(StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Set up serializers and deserializers, which we will use for overriding the default serdes
        // specified above.
        //
        // Heads up:  To improve the developer experience we are planning to unify serializers and
        // deserializers in a future version of Kafka Streams to streamline the API (no pun
        // intended).  For example, you would only need to specify the unified serde for `Long`
        // instead of having to specify its serializer and its deserializer separately.  This will
        // also reduce the number of API parameters when calling functions such as `countByKey` as
        // shown below.
        final Serializer<String> stringSerializer = new StringSerializer();
        final Deserializer<String> stringDeserializer = new StringDeserializer();
        final Serializer<Long> longSerializer = new LongSerializer();
        final Deserializer<Long> longDeserializer = new LongDeserializer();

        // In the subsequent lines we define the processing topology of the Streams application.
        KStreamBuilder builder = new KStreamBuilder();

        // Construct a `KStream` from the input topic "TextLinesTopic", where message values
        // represent lines of text (for the sake of this example, we ignore whatever may be stored
        // in the message keys).
        //
        // Note: We could also just call `builder.stream("TextLinesTopic")` if we wanted to leverage
        // the default serdes specified in the Streams configuration above, because these defaults
        // match what's in the actual topic.  However we explicitly set the deserializers in the
        // call to `stream()` below in order to show how that's done, too.
        KStream<String, String> textLines = builder.stream(stringDeserializer, stringDeserializer, "TextLinesTopic");

        KStream<String, Long> wordCounts = textLines
                // Split each text line, by whitespace, into words.  The text lines are the message
                // values, i.e. we can ignore whatever data is in the message keys and thus invoke
                // `flatMapValues` instead of the more generic `flatMap`.
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                // We will subsequently invoke `countByKey` to count the occurrences of words, so we use
                // `map` to ensure the words are available as message keys, too.
                .map((key, value) -> new KeyValue<>(value, value))
                // Count the occurrences of each word (message key).
                //
                // This will change the stream type from `KStream<String, String>` to
                // `KTable<String, Long>` (word -> count), hence we must provide serdes for `String`
                // and `Long`.
                //
                .countByKey(stringSerializer, longSerializer, stringDeserializer, longDeserializer, "Counts")
                // Convert the `KTable<String, Long>` into a `KStream<String, Long>`.
                .toStream();

        // Write the `KStream<String, Long>` to the output topic.
        wordCounts.to("WordsWithCountsTopic", stringSerializer, longSerializer);

        // Now that we have finished the definition of the processing topology we can actually run
        // it via `start()`.  The Streams application as a whole can be launched just like any
        // normal Java application that has a `main()` method.
        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();
    }
}
