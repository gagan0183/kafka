package io.learn;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerdemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerdemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        System.out.println("Producer with callback");

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"5xqfPFJimcVP6MHlXHbEhN\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI1eHFmUEZKaW1jVlA2TUhsWEhiRWhOIiwib3JnYW5pemF0aW9uSWQiOjcwMDA4LCJ1c2VySWQiOjgwOTQzLCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJmNDJjNDBiZC1kZmM5LTRhZjctYTdlMi05ZGZhNTBkMjllYjEifX0.iJSVn1b_kzQE0fbjw2q6iToR0T_ISU8sbvbav5viblo\";");
        properties.setProperty("sasl.mechanism", "PLAIN");
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        properties.setProperty("batch.size", "400");

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int j = 0; j < 10; j++) {

            for (int i = 0; i < 30; i++) {
                // create the producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world: " + i);

                // send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        // execute when a record is successfully sent or exception is thrown
                        if (exception == null) {
                            log.info("Received new metadata \n" +
                                    "Topic: " + metadata.topic() + "\n" +
                                    "Partition: " + metadata.partition() + "\n" +
                                    "Offset: " + metadata.offset() + "\n" +
                                    "Timestamp: " + metadata.timestamp()
                            );
                        } else {
                            log.error("Error while producing", exception);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }


        // tell the producer to send all data and block until complete synchronous
        producer.flush();

        // close
        producer.close();
    }
}
