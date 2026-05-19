# STEPS T  O EXECUTE PIPELINE:

### 1 . Create local infra: Execute docker compose command into the folder where **docker-compose.yaml** is contained :

```bash
docker compose up
```

### 2 . Create and activate virtual environment in python :

```bash
python3 -m venv .env
source .env/bin/activate
```

### 3 . Install dependencies in python :

```bash
pip install -r requirements.txt
```


### 4 . Run apache beam pipeline with local runner :

```bash
python pipeline.py
```

### 5 . Delete local infra: Execute docker compose command into the folder where **docker-compose.yaml** is contained :

```bash
docker compose down -v
```