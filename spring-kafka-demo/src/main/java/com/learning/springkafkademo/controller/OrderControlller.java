package com.learning.springkafkademo.controller;

import com.learning.springkafkademo.model.Order;
import com.learning.springkafkademo.producer.OrderProducer;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderControlller {

    private final OrderProducer orderProducer;

    public OrderControlller(OrderProducer orderProducer) {
        this.orderProducer = orderProducer;
    }
    @PostMapping
    public String createOrder(@RequestBody Order order){
        orderProducer.sendOrder(order);
        return "order send to kafka";
    }
}
