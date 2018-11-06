package com.bnsf.kafkatest.chatclient;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Service;



@Service
public class MessageConsumer {
	public MessageConsumer(){};
	
	public  void startConsumer(){

        ExecutorService executorService = Executors
                .newSingleThreadExecutor();
        Runnable runnableTask = () -> {
            System.out.println("Starting kafka consumer..");

            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");

            props.put("group.id", "group-1");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList("all.messages"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);

                for (ConsumerRecord<String, String> record : records){
                    if(null != record && record.value() != null){
                        System.out.printf("Message = %s", record.value());
                        System.out.println();
                    }

                }
            }
        };

        executorService.execute(runnableTask);
    }
}

