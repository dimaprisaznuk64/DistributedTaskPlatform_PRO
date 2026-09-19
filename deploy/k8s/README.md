# Розгортання в Kubernetes

Маніфести піднімають увесь стек платформи в кластері: Postgres (StatefulSet + PVC),
RabbitMQ, Redis, backend API (з init-container для міграцій) і воркери з HPA.

## 1. Збірка образу

```bash
docker build -t distributed-task-platform:1.0.0 ./backend
```

У мінікубі образ підхоплюється з локального registry без публікації:

```bash
minikube image load distributed-task-platform:1.0.0
```

У реальному кластері образ треба спершу запушити в registry і замінити
`image:` + `imagePullPolicy: IfNotPresent` у `06-backend.yaml` / `07-worker.yaml`.

## 2. Застосування

```bash
kubectl apply -k deploy/k8s
kubectl -n tasks-platform rollout status deployment/backend
kubectl -n tasks-platform rollout status deployment/worker
```

## 3. Доступ

```bash
# API + дашборд
kubectl -n tasks-platform port-forward svc/backend 8000:8000

# Grafana-подібний UI RabbitMQ (platform/platform із Secret)
kubectl -n tasks-platform port-forward svc/rabbitmq 15672:15672
```

Якщо в поточному кластері Prometheus/Grafana не встановлені — `/metrics` усе одно
доступний на `backend:8000/metrics` (Service `backend`).

## 4. Масштабування

Worker і backend мають HPA (CPU-базовані, `08-hpa.yaml`):

```bash
kubectl -n tasks-platform get hpa
kubectl -n tasks-platform scale deployment worker --replicas=5   # ручний оверрайд
```

HPA повертає кількість реплік до min після спаду навантаження. Для масштабування
саме за глибиною черги підійде KEDA з RabbitMQ-тригером — це зовнішній компонент
і тут не налаштований.

## 5. Креденшели

Усі секрети — у `01-secrets.yaml` (`stringData`, демонстраційні значення).
Для продакшена змініть через:

```bash
kubectl -n tasks-platform create secret generic platform-credentials \
  --from-literal=DATABASE_URL='postgresql+asyncpg://user:pass@postgres:5432/db' \
  --from-literal=RABBITMQ_URL='amqp://user:pass@rabbitmq:5672/' \
  --from-literal=REDIS_URL='redis://redis:6379/0' \
  --from-literal=RABBITMQ_USER=... --from-literal=RABBITMQ_PASS=... \
  --from-literal=POSTGRES_USER=... --from-literal=POSTGRES_PASSWORD=... \
  --from-literal=POSTGRES_DB=... \
  --dry-run=client -o yaml | kubectl apply -f -
```

## 6. Rollback та телеметрія

```bash
kubectl -n tasks-platform rollout undo deployment/backend
kubectl -n tasks-platform logs -l app=worker --tail=100
```

При `LOG_JSON=true` логи агрегуються як JSON-рядки (Loki/Elastic шар підключається ззовні).