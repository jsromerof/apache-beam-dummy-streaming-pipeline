# STEPS TO EXECUTE PIPELINE:

### 1 . Create local infra: Execute docker compose command into the folder where **docker-compose.yaml** is contained :

```bash
docker compose -f pipelines/global_supplier_to_retio/dev-resources/docker-compose.yaml up
```

### 2 . Delete local infra: Execute docker compose command into the folder where **docker-compose.yaml** is contained :

```bash
docker compose -f pipelines/global_supplier_to_retio/dev-resources/docker-compose.yaml down -v
```

### 3 . Create and activate virtual environment in python :

```bash
python3 -m venv .env
source .env/bin/activate
```

### 4 . Install dependencies in python :

```bash
pip install -r pipelines/global_supplier_to_retio/requirements.txt
```


### 5 . Run apache beam pipeline with local runner :

```bash
python -m pipelines.global_supplier_to_retio.pipeline
```

### 6 .  Produce message to kafka:

```bash
python -m pipelines.global_supplier_to_retio.dev-resources.kafka_producer
```

### 7 .  Consume message from kafka:

```bash
python -m pipelines.global_supplier_to_retio.dev-resources.kafka_consumer
```
