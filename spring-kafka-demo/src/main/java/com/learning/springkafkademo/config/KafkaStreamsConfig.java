package com.learning.springkafkademo.config;

import com.learning.springkafkademo.model.Order;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import tools.jackson.databind.ObjectMapper;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    @Bean
    public KStream<String, Order> orderStream(StreamsBuilder streamsBuilder) {

        Serde<Order> orderSerde = new OrderSerde();

        KStream<String, Order> stream = streamsBuilder.stream(
                "orders.spring",
                Consumed.with(Serdes.String(), orderSerde)
        );

        stream.foreach((key, order) ->
                System.out.println("Order reçue par Kafka Streams : " + order)
        );

        return stream;
    }

    static class OrderSerde implements Serde<Order> {

        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public Serializer<Order> serializer() {
            return (topic, data) -> {
                try {
                    return data == null ? null : objectMapper.writeValueAsBytes(data);
                } catch (Exception e) {
                    throw new RuntimeException("Erreur de sérialisation", e);
                }
            };
        }

        @Override
        public Deserializer<Order> deserializer() {
            return (topic, data) -> {
                try {
                    return data == null ? null :
                            objectMapper.readValue(data, Order.class);
                } catch (Exception e) {
                    throw new RuntimeException("Erreur de désérialisation", e);
                }
            };
        }
    }
}
