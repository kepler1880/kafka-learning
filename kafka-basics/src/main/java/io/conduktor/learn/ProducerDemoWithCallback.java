package io.conduktor.learn;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am Kafka Producer !!");

        //create Producer properties
        Properties properties =new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:51774");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create Producer
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0; i<10;i++){
            String topic ="java_demo";
            String value = "test " +i;

            //create Producer Record
        ProducerRecord<String,String> producerRecord =
                new ProducerRecord<String, String>(topic,value);

            //send data - asynchronous
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if( exception == null){
                        log.info("Received new metaData  \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " +metadata.partition() + "\n" +
                                "Timestamp: " +metadata.timestamp() );
                    }
                    else{
                        log.info("Error in sending data is " + exception);
                    }
                }
            });
        }

        //flush data - synchronous
        producer.flush();

        //flush and close the Producer
        producer.close();
    }
}
