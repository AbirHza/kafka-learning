# Kafka Learning

## Objectif

Ce repository documente mon apprentissage d'**Apache Kafka**, depuis la découverte de ses concepts fondamentaux jusqu'à son intégration dans une application **Spring Boot**.

Le parcours est organisé en plusieurs phases pratiques et progressives.

---

## Phases d'apprentissage

### Phase 1 — Kafka Fundamentals

Découverte de Kafka sans Spring Boot, à travers les outils en ligne de commande.

Concepts étudiés :

- Architecture KRaft
- Broker et Topic
- Partitions
- Producer et Consumer
- Consumer Groups
- Offsets et Lag

Documentation : [commands/phase-1.md](commands/phase-1.md)

### Phase 2 — Spring Boot + Kafka

Intégration de Kafka dans une application Spring Boot.

Concepts et outils pratiqués :

- Spring Kafka
- KafkaTemplate
- `@KafkaListener`
- API REST
- Sérialisation et désérialisation JSON
- Consumer Groups
- Offsets et Lag

Documentation : [commands/phase-2.md](commands/phase-2.md)

Application : [spring-kafka-demo](spring-kafka-demo/)

### Phase 3 — Kafka Advanced Concepts with Spring Kafka

Approfondissement de la consommation distribuée, de la gestion des erreurs et des offsets avec Spring Kafka.

Concepts et outils pratiqués :

- Partitions & concurrency
- Multiple Spring Boot instances
- Consumer Groups
- Rebalance & failover
- Retry with `DefaultErrorHandler`
- Dead Letter Topic (DLT)
- DLT reprocessing
- Offsets & Lag
- Message replay
- Offset reset

Documentation : [commands/phase-3.md](commands/phase-3.md)

### Phase 4 — Kafka Streams

Introduction au traitement de flux d'événements Kafka avec Kafka Streams.

Concepts et fonctionnalités :

- `StreamsBuilder` et `KStream`
- Lecture des commandes depuis un topic Kafka
- Sérialisation et désérialisation avec un `Serde<Order>` personnalisé
- Traitement et affichage des événements avec `foreach()`
- Filtrage avec `filter()`
- Transformation avec `mapValues()`
- Publication vers un autre topic avec `to()`

Documentation : [commands/phase-4.md](commands/phase-4.md)

Application : [spring-kafka-demo](spring-kafka-demo/)

---

## Environnement

- Java 21
- Apache Kafka
- Docker et Docker Compose
- Spring Boot
- Spring Kafka
- Kafka Streams
- Maven

Kafka est exécuté dans Docker en mode KRaft, sans ZooKeeper.

Adresse locale du broker :

```text
localhost:9092
```

---

## Structure du repository

```text
kafka-learning/
├── docker-compose.yml
├── README.md
├── commands/
│   ├── phase-1.md
│   ├── phase-2.md
│   ├── phase-3.md
│   └── phase-4.md
└── spring-kafka-demo/
    ├── pom.xml
    └── src/
```

---

## Progression

| Phase | Sujet | Statut |
|---|---|---|
| 1 | Kafka Fundamentals | Terminée |
| 2 | Spring Boot + Kafka | Terminée |
| 3 | Kafka Advanced Concepts with Spring Kafka | Terminée |
| 4 | Kafka Streams | En cours |

La phase 4 sera enrichie progressivement avec le filtrage, la transformation et la publication des événements vers un nouveau topic.

---