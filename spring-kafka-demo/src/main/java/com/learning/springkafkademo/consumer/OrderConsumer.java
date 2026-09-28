package com.learning.springkafkademo.consumer;

import com.learning.springkafkademo.model.Order;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {

    @KafkaListener(topics = "orders.created",groupId = "order-service")
    public void consumeOrder(Order order){
        System.out.println("Message reçu :"+ order);
    }

}
