package com.learning.springkafkademo.consumer;

import com.learning.springkafkademo.model.Order;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {

    @KafkaListener(topics = "orders.spring",groupId = "order-service", concurrency = "2")
    public void consumeOrder(Order order){
        System.out.println("Message reçu :"+ order);
        System.out.println("Commande traitée : " + order.getId());
    }

}
