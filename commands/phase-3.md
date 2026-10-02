# Phase 3 — Kafka Advanced Concepts with Spring Kafka

Cette phase permet de pratiquer des concepts avancés de Kafka avec une application Spring Boot / Spring Kafka.

## Objectifs

* Comprendre les partitions et la concurrence
* Utiliser plusieurs instances d'une application Spring Boot
* Comprendre les Consumer Groups
* Observer le rebalance et le failover
* Configurer les retries
* Utiliser un Dead Letter Topic (DLT)
* Reprocesser un message en erreur
* Comprendre les offsets et le lag
* Relire les anciens messages
* Réinitialiser un offset

---

## 1. Partitions et concurrency

Le topic `orders.spring` utilise 3 partitions.

Dans le consumer Spring :

```java
@KafkaListener(
    topics = "orders.spring",
    groupId = "order-service",
    concurrency = "2"
)
```

Avec `concurrency = 2`, une instance Spring Boot peut utiliser deux consumers.

Les partitions sont distribuées entre les consumers du même Consumer Group.

Exemple :

```text
orders.spring
├── partition 0 → consumer-order-service-1
├── partition 1 → consumer-order-service-1
└── partition 2 → consumer-order-service-2
```

---

## 2. Plusieurs instances Spring Boot

Deux instances de l'application ont été lancées avec le même :

```text
groupId = order-service
```

La deuxième instance utilise le port :

```text
8081
```

alors que la première utilise :

```text
8080
```

Les deux instances appartiennent donc au même Consumer Group.

Kafka distribue les partitions entre les consumers disponibles.

---

## 3. Consumer Group

Pour observer les consumers et leurs partitions :

```cmd
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group order-service --describe
```

Pour obtenir les membres et leurs assignments :

```cmd
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group order-service --describe --members --verbose
```

Un Consumer Group permet à plusieurs consumers de travailler ensemble sur un même topic.

Une partition est consommée par un seul consumer du même groupe à un instant donné.

---

## 4. Rebalance et failover

Lorsqu'une instance Spring Boot est arrêtée, Kafka détecte la disparition du consumer.

Le Consumer Group effectue alors un rebalance.

Les partitions peuvent être réassignées aux consumers encore actifs.

Schéma :

```text
Instance A + Instance B
        ↓
   Consumer Group
        ↓
Partitions distribuées
        ↓
Instance B arrêtée
        ↓
      Rebalance
        ↓
Partitions réassignées
```

Un message envoyé après l'arrêt d'une instance a continué à être consommé par l'instance restante.

---

## 5. Retry avec DefaultErrorHandler

Une erreur temporaire a été simulée dans le consumer pour la commande `id = 999`.

Configuration utilisée :

```java
FixedBackOff backOff = new FixedBackOff(1000L, 2L);

return new DefaultErrorHandler(backOff);
```

Cela signifie :

* attente de 1 seconde entre les tentatives ;
* 2 retries après la tentative initiale ;
* donc 3 tentatives au total.

Exemple :

```text
Tentative initiale
      ↓
Attente 1 seconde
      ↓
Retry 1
      ↓
Attente 1 seconde
      ↓
Retry 2
      ↓
Échec final
```

---

## 6. Dead Letter Topic (DLT)

Après épuisement des retries, le message est envoyé vers un Dead Letter Topic.

Configuration :

```java
DeadLetterPublishingRecoverer recoverer =
        new DeadLetterPublishingRecoverer(kafkaTemplate);

FixedBackOff backOff = new FixedBackOff(1000L, 2L);

return new DefaultErrorHandler(recoverer, backOff);
```

Topic DLT utilisé :

```text
orders.spring-dlt
```

Création du topic :

```cmd
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders.spring-dlt --partitions 3 --replication-factor 1
```

Pour lire les messages du DLT :

```cmd
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders.spring-dlt --from-beginning
```

Pour afficher les headers :

```cmd
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders.spring-dlt --from-beginning --property print.headers=true
```

Les headers du DLT contiennent notamment des informations sur :

* l'exception ;
* le message d'erreur ;
* le topic original ;
* la partition originale ;
* l'offset original ;
* le Consumer Group original.

---

## 7. Reprocessing d'un message DLT

Pour simuler une correction, l'erreur temporaire sur `id = 999` a été désactivée.

Le message a ensuite été republié manuellement dans `orders.spring` :

```cmd
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders.spring
```

Message :

```json
{"id":999,"product":"Test DLT","quantity":1}
```

Le message a ensuite été consommé correctement par l'application.

Dans un environnement réel, le reprocessing peut être automatisé.

---

## 8. Offsets et Lag

Pour observer les offsets du Consumer Group :

```cmd
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group order-service --describe
```

Les colonnes importantes sont :

```text
CURRENT-OFFSET
LOG-END-OFFSET
LAG
```

### CURRENT-OFFSET

Position jusqu'à laquelle le consumer a consommé.

### LOG-END-OFFSET

Position du prochain message disponible dans la partition.

### LAG

Différence entre les deux :

```text
LAG = LOG-END-OFFSET - CURRENT-OFFSET
```

Exemple :

```text
CURRENT-OFFSET = 8
LOG-END-OFFSET = 11
LAG = 3
```

Après consommation :

```text
CURRENT-OFFSET = 11
LOG-END-OFFSET = 11
LAG = 0
```

---

## 9. Replay avec un nouveau Consumer Group

Un nouveau Consumer Group a été créé :

```text
order-replay
```

Commande :

```cmd
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders.spring --group order-replay --from-beginning
```

Le nouveau groupe a pu relire les anciens messages indépendamment de :

```text
order-service
```

Chaque Consumer Group possède ses propres offsets.

---

## 10. Offset Reset

Le groupe `order-replay` avait atteint l'offset 11.

Avant le reset :

```text
CURRENT-OFFSET = 11
LOG-END-OFFSET = 11
LAG = 0
```

Le groupe a ensuite été arrêté avant de modifier son offset.

Reset de la partition 0 vers le début :

```cmd
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group order-replay --topic orders.spring:0 --reset-offsets --to-earliest --execute
```

Résultat :

```text
NEW-OFFSET = 0
```

Après le reset :

```text
CURRENT-OFFSET = 0
LOG-END-OFFSET = 11
LAG = 11
```

Le consumer a ensuite été relancé :

```cmd
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders.spring --group order-replay
```

Les 11 messages ont été relus depuis le début.

### Différence entre `--from-beginning` et `--reset-offsets`

`--from-beginning` permet à un consumer de commencer depuis le début lorsque les offsets appropriés ne sont pas encore établis.

`--reset-offsets` permet de modifier explicitement la position d'un Consumer Group existant.

---

## 11. Topics utilisés

### Topic principal

```text
orders.spring
```

### Dead Letter Topic

```text
orders.spring-dlt
```

### Topic Phase 1

```text
orders.created
```

`orders.created` reste le topic utilisé pendant la Phase 1.

Le lag observé sur ce topic n'est pas lié aux tests Spring Kafka de cette phase.

---

## Conclusion

Cette phase a permis de pratiquer les principaux mécanismes Kafka liés à la consommation distribuée et à la gestion des erreurs :

```text
Partitions
    ↓
Concurrency
    ↓
Consumer Groups
    ↓
Rebalance / Failover
    ↓
Retry
    ↓
DLT
    ↓
Reprocessing
    ↓
Offsets
    ↓
Lag
    ↓
Replay
    ↓
Offset Reset
```