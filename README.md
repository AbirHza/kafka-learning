# Kafka Learning Project

Projet personnel d'apprentissage consacré à Apache Kafka, avec une intégration progressive dans l'écosystème Java / Spring Boot.

L'objectif est de comprendre les concepts fondamentaux de Kafka à travers des expérimentations pratiques avant son intégration dans une application Java.

## 🎯 Objectifs

* Comprendre les concepts fondamentaux de Kafka
* Manipuler Kafka avec Docker
* Pratiquer les opérations de base avec Kafka
* Comprendre progressivement le fonctionnement des producers, consumers et consumer groups
* Approfondir Kafka à travers des expérimentations pratiques

## 🛠️ Technologies

* Java 21
* Apache Kafka
* Docker
* Docker Compose
* Git / GitHub

## 📚 Progression

### Phase 1 — Kafka Fundamentals ✅

Première phase réalisée sans Spring Boot afin de comprendre les concepts fondamentaux de Kafka.

Concepts étudiés :

* Architecture KRaft
* Broker
* Topic
* Partition
* Producer
* Consumer
* Consumer Group
* Offset
* Lag

Documentation :

`commands/phase-1.md`

### Prochaines phases

Les prochaines phases seront ajoutées progressivement au fur et à mesure de l'avancement du projet.

## 🐳 Démarrage

Kafka est exécuté dans un conteneur Docker.

Démarrer Kafka :

```cmd id="8p4x2m"
docker compose up -d
```

Vérifier le conteneur :

```cmd id="q9s6hn"
docker ps
```

Kafka est accessible sur :

```text id="j5m2xk"
localhost:9092
```

## 📁 Structure du projet

```text id="3t8v4n"
kafka-learning/
├── docker-compose.yml
├── README.md
└── commands/
    └── phase-1.md
```

## 📌 Progression du projet

Ce repository évolue progressivement au fil de l'apprentissage et des expérimentations pratiques avec Kafka.
