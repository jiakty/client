package com.bnsf.kafkatest.chatclient;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;



@Service
public class MessageConsumer {
    @KafkaListener(topics = "all.messages")
    public void onReceiving(ConsumerRecord<?, ?> consumerRecord) {
    	System.out.println(consumerRecord.value());
    	//System.out.println("Receiver on topic1: "+consumerRecord.toString());
    }
}

