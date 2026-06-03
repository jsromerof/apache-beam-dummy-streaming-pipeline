# STEPS TO CREATE LOCAL INFRASTRUCTURE TO DEV ENVIRONMENT:

### 1 . Create local infra: Execute docker compose command into the folder where **docker-compose.yaml** is contained :

```bash
docker compose -f pipelines/global_supplier_to_retio/dev-resources/docker-compose.yaml up
```

### 2 . Delete local infra: Execute docker compose command into the folder where **docker-compose.yaml** is contained :

```bash
docker compose -f pipelines/global_supplier_to_retio/dev-resources/docker-compose.yaml down -v
```

### 3 .  Produce message to kafka:

```bash
python -m pipelines.global_supplier_to_retio.dev-resources.kafka_producer
```

### 4 .  Consume message from kafka:

```bash
python -m pipelines.global_supplier_to_retio.dev-resources.kafka_consumer
```