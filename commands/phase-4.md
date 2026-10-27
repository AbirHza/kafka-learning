# Phase 4 — Kafka Streams

## Objectif

Découvrir Kafka Streams et construire un flux de traitement des commandes à partir du topic Kafka `orders.spring`, dans l'application Spring Boot existante.

## 1. Prérequis

- Java 21
- Apache Kafka démarré sur `localhost:9092`
- Maven
- Application `spring-kafka-demo`

Kafka fonctionne en mode KRaft, sans ZooKeeper.

## 2. Dépendance Maven

Dépendance ajoutée dans `spring-kafka-demo/pom.xml` :

```xml
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-streams</artifactId>
</dependency>
```

La version est gérée par la configuration Maven du projet.

## 3. Configuration Kafka Streams

Dans `spring-kafka-demo/src/main/resources/application.properties` :

```properties
spring.kafka.streams.application-id=order-streams-app
spring.kafka.streams.bootstrap-servers=localhost:9092
```

- `application-id` identifie l'application Kafka Streams.
- `bootstrap-servers` indique l'adresse du broker Kafka.

## 4. Création du flux

Classe : `KafkaStreamsConfig`

Les éléments implémentés sont :

- `@Configuration` et `@EnableKafkaStreams` pour configurer et activer Kafka Streams.
- `StreamsBuilder` pour construire le flux.
- `KStream<String, Order>` pour représenter les événements du topic `orders.spring`.
- `Consumed.with(Serdes.String(), orderSerde)` pour préciser les sérialiseurs et désérialiseurs utilisés à la lecture.
- Un `Serde<Order>` personnalisé pour convertir les commandes entre objets Java et données JSON.
- `foreach()` pour afficher les commandes reçues dans la console.

### Flux actuel

```text
orders.spring
      |
      v
StreamsBuilder
      |
      v
    KStream
      |
      v
   foreach()
      |
      v
Affichage console
```

## 5. Lancer l'application

Depuis le répertoire du projet :

```cmd
cd spring-kafka-demo
mvn spring-boot:run
```

L'application Spring Boot démarre sur le port `8081`.

## 6. Tester la publication d'une commande

Dans un autre terminal CMD, envoyer une commande à l'API REST :

```cmd
curl -X POST http://localhost:8081/orders -H "Content-Type: application/json" -d "{\"id\":101,\"product\":\"Laptop\",\"quantity\":3}"
```

### Résultat attendu

La commande est publiée dans `orders.spring`, puis le flux Kafka Streams la traite et l'affiche dans les logs.

Exemple de message attendu, selon le format `toString()` de la classe `Order` :

```text
Order reçue par Kafka Streams : Order{id=101, product='Laptop', quantity=3}
```

Le consommateur Spring `@KafkaListener` peut également afficher cette commande dans ses propres logs.

## 7. Étapes suivantes

### Étape 1 — Filtrer avec `filter()`

Conserver les commandes dont la quantité est supérieure ou égale à 2.

```java
KStream<String, Order> filteredStream =
        stream.filter((key, order) -> order.getQuantity() >= 2);
```

### Étape 2 — Transformer avec `mapValues()`

Transformer la valeur des événements sans modifier leur clé Kafka. La transformation exacte sera définie et testée lors de cette étape.

### Étape 3 — Publier avec `to()`

Envoyer le flux traité vers un nouveau topic :

```text
orders.processed
```

### Étape 4 — Vérifier le flux complet

Tester plusieurs commandes et vérifier que le topic `orders.processed` contient uniquement les événements attendus après filtrage et transformation.

## 8. Flux cible

```text
orders.spring
      |
      v
   filter()
      |
      v
  mapValues()
      |
      v
      to()
      |
      v
orders.processed
```

Les étapes `filter()`, `mapValues()` et `to()` restent à implémenter et à valider.