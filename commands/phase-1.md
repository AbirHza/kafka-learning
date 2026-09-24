# Phase 1 — Kafka Fundamentals

## Objectif

Découvrir les concepts fondamentaux de Kafka et pratiquer les opérations de base avec Kafka exécuté dans Docker.

Cette phase est réalisée sans Spring Boot afin de comprendre le fonctionnement de Kafka avant son intégration dans une application Java.

### Concepts étudiés

* Kafka
* Architecture KRaft
* Broker
* Topic
* Partition
* Producer
* Consumer
* Consumer Group
* Offset
* Lag

---

# 1. Prérequis

Versions utilisées :

```text
Java 21.0.12.1 LTS
Docker 28.5.1
Docker Compose v2.40.0
Git 2.45.1
```

Kafka n'est pas installé directement sur Windows. Il est exécuté dans un conteneur Docker.

---

# 2. Architecture utilisée

Kafka est exécuté dans Docker en mode **KRaft**, sans ZooKeeper.

Architecture simplifiée :

```text
                    Docker
                      |
                      v
              +---------------+
              |     Kafka     |
              |               |
              | Broker        |
              | Controller    |
              |               |
              | :9092         |
              +---------------+
```

Le port `9092` est utilisé pour les communications avec les clients Kafka.

---

# 3. Lancer Kafka avec Docker

Kafka est configuré dans :

```text
docker-compose.yml
```

Démarrer Kafka :

```cmd
docker compose up -d
```

Vérifier le conteneur :

```cmd
docker ps
```

Vérifier les logs :

```cmd
docker logs kafka
```

Kafka est accessible depuis la machine hôte via :

```text
localhost:9092
```

---

# 4. Créer un topic

Le premier topic utilisé est :

```text
orders.created
```

Création :

```cmd
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic orders.created --bootstrap-server localhost:9092
```

Vérifier les topics disponibles :

```cmd
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

Résultat :

```text
orders.created
```

### À retenir

Un **topic** est une catégorie logique dans laquelle Kafka stocke les messages.

Dans notre exemple, `orders.created` représente des événements indiquant qu'une commande a été créée.

---

# 5. Examiner un topic

Afficher les informations du topic :

```cmd
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --describe --topic orders.created --bootstrap-server localhost:9092
```

Cette commande permet notamment d'observer :

* le nombre de partitions ;
* le facteur de réplication ;
* le leader de chaque partition ;
* les replicas présentes dans l'ISR.

---

# 6. Comprendre les partitions

Un deuxième topic a été créé pour pratiquer les partitions :

```text
orders
```

avec 3 partitions :

```cmd
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic orders --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
```

Vérifier sa configuration :

```cmd
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --describe --topic orders --bootstrap-server localhost:9092
```

Structure :

```text
orders
├── Partition 0
├── Partition 1
└── Partition 2
```

### À retenir

Une partition est une unité de stockage et d'organisation des messages dans un topic.

Les partitions permettent notamment à Kafka de répartir les messages et de permettre le traitement parallèle par plusieurs consumers au sein d'un consumer group.

---

# 7. Producer

Kafka fournit un producer en ligne de commande.

Lancer le producer :

```cmd
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --topic orders.created --bootstrap-server localhost:9092
```

Messages envoyés :

```text
Order 1 created
Order 2 created
Order 3 created
```

Le producer publie les messages dans le topic Kafka.

Architecture :

```text
Producer
    |
    | message
    v
Kafka topic
orders.created
```

---

# 8. Consumer

Dans un autre terminal, lancer le consumer :

```cmd
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --topic orders.created --bootstrap-server localhost:9092 --from-beginning
```

Le consumer récupère les messages présents dans le topic.

Messages reçus :

```text
Order 1 created
Order 2 created
Order 3 created
```

Architecture :

```text
Producer
    |
    v
Kafka
    |
    v
orders.created
    |
    v
Consumer
```

Une nouvelle expérience a également été réalisée en laissant le consumer actif pendant l'envoi de nouveaux messages afin d'observer leur réception en temps réel.

---

# 9. Consumer Group

Un consumer peut appartenir à un **consumer group**.

Exemple :

```text
order-service
```

Lancer le consumer avec ce group :

```cmd
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --topic orders.created --bootstrap-server localhost:9092 --group order-service
```

Le consumer group permet à Kafka de suivre la progression de consommation pour ce groupe.

### À retenir

Le `group.id` identifie le groupe auquel appartient le consumer.

Plusieurs consumers appartenant au même group peuvent se répartir les partitions d'un topic.

---

# 10. Offsets

Chaque message possède une position appelée **offset** au sein de sa partition.

Exemple :

```text
Partition 0

Offset 0 → Order 1
Offset 1 → Order 2
Offset 2 → Order 3
Offset 3 → Order 4
```

L'offset permet de représenter la position d'un message dans une partition et permet notamment à Kafka de suivre la progression de consommation d'un consumer group.

### À retenir

Les offsets sont propres à chaque partition.

```text
Partition 0 → Offset 0, 1, 2, 3...
Partition 1 → Offset 0, 1, 2, 3...
Partition 2 → Offset 0, 1, 2, 3...
```

---

# 11. Reprise de consommation avec un Consumer Group

Une expérience a été réalisée avec le consumer group :

```text
order-service
```

Le consumer a été arrêté, puis de nouveaux messages ont été envoyés.

Le consumer a ensuite été relancé avec le même `group.id`.

Cette expérience permet d'observer que Kafka conserve la progression du consumer group et permet au consumer de reprendre sa consommation à partir de la position suivie pour ce groupe.

---

# 12. Observer un Consumer Group

Afficher les informations du consumer group :

```cmd
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group order-service --describe
```

Les principales informations à observer sont :

```text
CURRENT-OFFSET
LOG-END-OFFSET
LAG
```

### CURRENT-OFFSET

Position de consommation enregistrée pour le consumer group.

### LOG-END-OFFSET

Position correspondant à la fin actuelle du log de la partition.

### LAG

Le lag représente le retard du consumer group par rapport aux messages disponibles.

Exemple conceptuel :

```text
CURRENT-OFFSET = 5
LOG-END-OFFSET = 10
LAG            = 5
```

Le consumer group accuse donc un retard de 5 positions dans cette situation.

---

# 13. Expériences réalisées

Durant cette phase, plusieurs expériences pratiques ont été réalisées :

* démarrage de Kafka avec Docker ;
* utilisation de Kafka en mode KRaft ;
* création d'un topic ;
* liste des topics ;
* description d'un topic ;
* création d'un topic avec plusieurs partitions ;
* envoi de messages avec un producer ;
* réception de messages avec un consumer ;
* réception de messages en temps réel ;
* utilisation d'un consumer group ;
* arrêt et redémarrage d'un consumer ;
* observation des offsets ;
* observation du lag.

---

# 14. Architecture fondamentale

Le fonctionnement étudié peut être résumé ainsi :

```text
                    Kafka
                      |
                  orders.created
                      |
              +-------+-------+
              |               |
        Partition 0      Partition 1
              |
              v
       Consumer Group
        order-service
              |
              v
          Consumer
```

Pour un topic comportant plusieurs partitions :

```text
orders
  |
  +── Partition 0
  +── Partition 1
  +── Partition 2
```

Plusieurs consumers appartenant au même consumer group peuvent se partager ces partitions.

---

# 15. À retenir

À la fin de cette phase, les concepts fondamentaux suivants ont été pratiqués :

```text
Topic
  ↓
Partition
  ↓
Message
  ↓
Offset
  ↓
Consumer
  ↓
Consumer Group
  ↓
Lag
```

Le fonctionnement de base étudié est :

```text
Producer
    |
    v
  Topic
    |
    +── Partition 0
    +── Partition 1
    +── Partition 2
    |
    v
Consumer Group
    |
    v
Consumer
```

La prochaine phase pourra consister à intégrer Kafka dans une application **Spring Boot** avec un producer et un consumer.
