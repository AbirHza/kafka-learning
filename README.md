# Kafka Learning

## Objectif

Ce repository documente mon apprentissage d'**Apache Kafka**, depuis la découverte de ses concepts fondamentaux jusqu'à son intégration dans une application **Spring Boot**.

Le parcours est organisé en plusieurs phases pratiques et progressives.

---

## Phases d'apprentissage

### Phase 1 — Kafka Fundamentals

Découverte de Kafka sans Spring Boot, à travers les outils en ligne de commande.

Concepts étudiés :

* Architecture KRaft
* Broker et Topic
* Partitions
* Producer et Consumer
* Consumer Groups
* Offsets et Lag

Documentation : [commands/phase-1.md](commands/phase-1.md)

### Phase 2 — Spring Boot + Kafka

Intégration de Kafka dans une application Spring Boot.

Concepts et outils pratiqués :

* Spring Kafka
* KafkaTemplate
* @KafkaListener
* API REST
* Sérialisation et désérialisation JSON
* Consumer Groups
* Offsets et Lag

Documentation : [commands/phase-2.md](commands/phase-2.md)

Application : [spring-kafka-demo](spring-kafka-demo/)

---

## Environnement

* Java 21
* Apache Kafka
* Docker et Docker Compose
* Spring Boot
* Spring Kafka
* Maven

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
│   └── phase-2.md
└── spring-kafka-demo/
    ├── pom.xml
    └── src/
```

---

## Progression

```text
Phase 1
Kafka Fundamentals
       |
       v
Kafka CLI
       |
       v
Topics, Partitions
       |
       v
Producer, Consumer
       |
       v
Consumer Groups
       |
       v
Offsets, Lag
       |
       v
Phase 2
Spring Boot + Kafka
       |
       v
KafkaTemplate
       |
       v
@KafkaListener
       |
       v
REST API + JSON
       |
       v
Consumer Groups + Lag
```

Ce repository sera enrichi progressivement avec de nouvelles expériences et fonctionnalités Kafka.