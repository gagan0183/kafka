package io.learn;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerdemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerdemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {
        System.out.println("Consumer");
        String groupId = "java-application";
        String topic = "demo_java";

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"5xqfPFJimcVP6MHlXHbEhN\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI1eHFmUEZKaW1jVlA2TUhsWEhiRWhOIiwib3JnYW5pemF0aW9uSWQiOjcwMDA4LCJ1c2VySWQiOjgwOTQzLCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJmNDJjNDBiZC1kZmM5LTRhZjctYTdlMi05ZGZhNTBkMjllYjEifX0.iJSVn1b_kzQE0fbjw2q6iToR0T_ISU8sbvbav5viblo\";");
        properties.setProperty("sasl.mechanism", "PLAIN");
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");

        //set producer properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // get a reference to main
        final Thread mainThread = Thread.currentThread();

        // add a shutdown
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Shutdown detect. Let' exit by calling");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            // subscribe to topic
            consumer.subscribe(Arrays.asList(topic));

            // poll
            while (true) {

                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                for(ConsumerRecord<String, String> consumerRecord: consumerRecords) {
                    System.out.println("key: " + consumerRecord.key() + " | value: " + consumerRecord.value() + " | partition: " + consumerRecord.partition() + " | offset: " + consumerRecord.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shutdown");
        } catch (Exception e) {
            log.info("Unexpected exception: ", e);
        } finally {
            consumer.close();
            log.info("Consume is not gracefully shutdown");
        }
    }
}
