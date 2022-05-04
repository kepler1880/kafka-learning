package io.conduktor.learn;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperativeStickyAssignor {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperativeStickyAssignor.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am Kafka Consumer !!");
        String bootstrapServers = "[::1]:9092";
        String groupId= "my-third-application";
        String topic = "java_demo";

        //create Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //This is to make the consumer run in only cooperative sticky assignor strategy
        //Cooperative Rebalance : RangeAssignor, RoundRobin, Sticky Assignor, Cooperative Sticky Assignor
        //EagerRebalance is default
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        //For Static assignment
        //properties.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,"");


        //create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //get reference to current thread
        final Thread mainThread = Thread.currentThread();

        //adding shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup() ..");
                consumer.wakeup();

                //join main thread to allow execution of the code in the main thread
                try{
                    mainThread.join();
                }
                catch (InterruptedException ie){
                    ie.printStackTrace();
                }
            }
        });

        try{
            //subscribe consumer to our topic(s)
            consumer.subscribe(Arrays.asList(topic));

            //poll for new data
            while (true){
               // log.info("Polling");
                ConsumerRecords<String,String> records =
                        consumer.poll(Duration.ofMillis(1000));

                for(ConsumerRecord<String,String> record : records){
                    log.info("Key :" + record.key() + ", Value " + record.value());
                    log.info("Partition :" + record.partition() + ", Offset " + record.offset());

                }
            }
        }catch (WakeupException we){
            log.info("Wakeup Exception !");
            //we ignore this as this is an expected exception when closing a consumer
        } catch (Exception e){
            log.error("Unexpected Exception");
        } finally {
            consumer.close(); //this will also commit offsets if need be
            log.info(" The consumer is now gracefully closed");
        }


    }
}
