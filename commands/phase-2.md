# Phase 2 — Spring Boot + Kafka

## Objectif

Intégrer Kafka dans une application Spring Boot afin de mettre en pratique les concepts étudiés durant la Phase 1.

Cette phase permet d'utiliser Kafka depuis une application Java avec Spring Kafka, notamment à travers un Producer, un Consumer et une API REST.

---

# 1. Prérequis

Technologies utilisées :

* Java 21
* Spring Boot
* Spring Kafka
* Maven
* Docker
* Apache Kafka

L'application Spring Boot communique avec Kafka exécuté dans un conteneur Docker.

Kafka est accessible depuis l'application via :

```text
localhost:9092
```

---

# 2. Structure du projet

L'application Spring Boot est intégrée directement dans le repository `kafka-learning`, dans le dossier `spring-kafka-demo`.

```text
kafka-learning/
├── docker-compose.yml
├── README.md
├── commands/
│   ├── phase-1.md
│   └── phase-2.md
│
└── spring-kafka-demo/
    ├── pom.xml
    └── src/
        └── main/
            ├── java/
            └── resources/
```

La documentation des expériences est conservée dans `commands`, tandis que le code de l'application se trouve dans `spring-kafka-demo`.

---

# 3. Architecture de l'application

L'application expose une API REST permettant de créer une commande et de l'envoyer à Kafka.

```text
             Postman
                |
                | POST /orders
                v
       +-------------------+
       |  OrderController  |
       +-------------------+
                |
                v
       +-------------------+
       |   OrderProducer   |
       +-------------------+
                |
                | KafkaTemplate
                v
       +-------------------+
       |       Kafka       |
       |  orders.created   |
       +-------------------+
                |
                v
       +-------------------+
       |   OrderConsumer   |
       +-------------------+
                |
                v
          Objet Order
```

---

# 4. Configuration de Kafka

Kafka est lancé avec Docker à partir du fichier `docker-compose.yml` situé à la racine du repository.

Démarrer Kafka :

```cmd
docker compose up -d
```

Vérifier le conteneur :

```cmd
docker ps
```

L'application utilise le serveur Kafka suivant :

```text
localhost:9092
```

Le topic utilisé pour les événements de commande est :

```text
orders.created
```

---

# 5. Configuration Spring Kafka

La configuration de la connexion et des sérialiseurs est définie dans `application.properties`.

Configuration utilisée :

```properties
spring.application.name=spring-kafka-demo
spring.kafka.bootstrap-servers=localhost:9092

spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer

spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer

spring.kafka.consumer.properties.spring.json.trusted.packages=com.learning.springkafkademo.model
spring.kafka.consumer.properties.spring.json.value.default.type=com.learning.springkafkademo.model.Order
```

Les serializers convertissent les données Java en données transmissibles à Kafka. Les deserializers permettent au consumer de reconstruire les objets Java à partir des messages reçus.

Le type par défaut est notamment utile pour les messages JSON envoyés par le console producer, qui ne contiennent pas les en-têtes de type Spring Kafka.

---

# 6. Modèle Order

Un objet `Order` représente une commande.

Il contient les attributs suivants :

```text
id
product
quantity
```

Exemple de données :

```json
{
  "id": 1,
  "product": "Laptop",
  "quantity": 2
}
```

Ce modèle est utilisé par le contrôleur, le producer et le consumer.

---

# 7. Producer avec KafkaTemplate

Le producer utilise `KafkaTemplate` pour envoyer les objets `Order` au topic Kafka.

```java
@Service
public class OrderProducer {

    private final KafkaTemplate<String, Order> kafkaTemplate;

    public OrderProducer(KafkaTemplate<String, Order> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrder(Order order) {
        kafkaTemplate.send("orders.created", order);
    }
}
```

Le `KafkaTemplate` facilite l'envoi des messages depuis l'application Spring Boot.

Le topic ciblé est `orders.created`.

---

# 8. Consumer avec @KafkaListener

Le consumer utilise l'annotation `@KafkaListener` pour écouter le topic.

```java
@Service
public class OrderConsumer {

    @KafkaListener(
        topics = "orders.created",
        groupId = "order-service"
    )
    public void consumeOrder(Order order) {
        System.out.println("Message reçu :" + order);
    }
}
```

Le consumer appartient au groupe :

```text
order-service
```

Lorsqu'un message est disponible, Spring Kafka appelle la méthode `consumeOrder`.

---

# 9. API REST avec OrderController

Le contrôleur expose un endpoint permettant de transmettre une commande au producer.

```java
@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderProducer orderProducer;

    public OrderController(OrderProducer orderProducer) {
        this.orderProducer = orderProducer;
    }

    @PostMapping
    public String createOrder(@RequestBody Order order) {
        orderProducer.sendOrder(order);
        return "Order sent to Kafka";
    }
}
```

Endpoint :

```text
POST http://localhost:8080/orders
```

Le contrôleur reçoit l'objet JSON, puis appelle le producer pour l'envoyer à Kafka.

---

# 10. Tester l'application avec Postman

Démarrer l'application Spring Boot depuis IntelliJ.

Dans Postman, envoyer une requête :

```text
POST http://localhost:8080/orders
```

Body, au format JSON :

```json
{
  "id": 1,
  "product": "Laptop",
  "quantity": 2
}
```

Réponse attendue :

```text
Order sent to Kafka
```

Dans la console de l'application Spring Boot, vérifier la réception du message par le consumer.

Exemple :

```text
Message reçu :Order{id=1, product='Laptop', quantity=2}
```

Cette expérience valide le flux entre l'API REST, le producer, le topic Kafka et le consumer.

---

# 11. Sérialisation et désérialisation JSON

Le producer utilise `JsonSerializer` pour transformer les objets `Order` en messages JSON.

Le consumer utilise `JsonDeserializer` pour reconstruire les objets Java.

Une difficulté a été rencontrée lors de la désérialisation de messages envoyés manuellement depuis le console producer.

Ces messages JSON ne contenaient pas les en-têtes de type utilisés par Spring Kafka.

La configuration suivante a permis de définir le type Java par défaut :

```properties
spring.kafka.consumer.properties.spring.json.value.default.type=com.learning.springkafkademo.model.Order
```

Les packages autorisés sont également configurés :

```properties
spring.kafka.consumer.properties.spring.json.trusted.packages=com.learning.springkafkademo.model
```

---

# 12. Utilisation du console producer

En complément de Postman, des messages JSON ont été envoyés directement à Kafka avec le console producer.

Commande :

```cmd
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --topic orders.created --bootstrap-server localhost:9092
```

Exemples de messages :

```json
{"id":6,"product":"Tablet","quantity":2}
{"id":7,"product":"Mouse","quantity":4}
{"id":8,"product":"Keyboard","quantity":1}
```

Ces messages ont permis de tester la réception de JSON produits indépendamment de l'application Spring Boot.

---

# 13. Consumer Group

Le consumer Spring Kafka utilise le groupe :

```text
order-service
```

La configuration est définie dans l'annotation :

```java
@KafkaListener(
    topics = "orders.created",
    groupId = "order-service"
)
```

Kafka suit la progression de consommation du groupe, ce qui permet d'observer les offsets et le lag avec les commandes Kafka CLI.

---

# 14. Observer les offsets et le LAG

Pour afficher les informations du consumer group :

```cmd
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group order-service --describe
```

Les principales colonnes observées sont :

* `CURRENT-OFFSET`
* `LOG-END-OFFSET`
* `LAG`

Le LAG représente le retard du consumer group par rapport à la fin du log de la partition.

---

# 15. Expérience : Consumer arrêté

Une expérience a été réalisée en arrêtant l'application Spring Boot, donc son consumer.

Des messages ont ensuite été ajoutés au topic `orders.created` à l'aide du console producer.

Après l'envoi de trois messages, le LAG observé était :

```text
LAG = 3
```

Cette expérience montre que les messages peuvent rester disponibles dans Kafka pendant que le consumer est arrêté.

---

# 16. Redémarrage du Consumer

L'application Spring Boot a ensuite été redémarrée.

Le consumer a repris la consommation des messages en attente dans le topic.

Après leur traitement, le LAG est revenu à :

```text
LAG = 0
```

Cette expérience permet d'observer la reprise de consommation avec le même consumer group.

---

# 17. Expériences réalisées

Durant cette phase, plusieurs expériences pratiques ont été réalisées :

* intégration de Kafka dans une application Spring Boot ;
* configuration de Spring Kafka ;
* création d'un modèle Java `Order` ;
* envoi de messages avec `KafkaTemplate` ;
* réception de messages avec `@KafkaListener` ;
* création d'une API REST pour envoyer les commandes ;
* test de l'API avec Postman ;
* sérialisation et désérialisation JSON ;
* envoi direct de JSON avec le console producer ;
* utilisation du consumer group `order-service` ;
* observation des offsets et du LAG ;
* arrêt du consumer avec accumulation de messages ;
* redémarrage du consumer et retour du LAG à zéro.

---

# 18. Architecture finale

```text
                  Postman
                     |
                     v
             OrderController
                     |
                     v
               OrderProducer
                     |
               KafkaTemplate
                     |
                     v
              Kafka Broker
                     |
                     v
             orders.created
                     |
                     v
              OrderConsumer
                     |
                order-service
                     |
                     v
                Objet Order
```

---

# 19. À retenir

Cette phase a permis de passer de l'utilisation de Kafka en ligne de commande à son intégration dans une application Java avec Spring Boot.

Les principaux éléments pratiqués sont :

```text
Spring Boot
    |
    v
OrderController
    |
    v
KafkaTemplate
    |
    v
Kafka Topic
    |
    v
@KafkaListener
    |
    v
Order
```

La prochaine étape consistera à approfondir le fonctionnement des partitions et du parallélisme avec plusieurs consumers.