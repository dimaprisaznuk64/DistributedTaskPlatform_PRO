# DistributedTaskPlatform_PRO

Надійна платформа виконання фонових задач: створення → постановка в чергу →
виконання → збереження результату → помилки з можливістю повтору → статус у UI.

Принцип: **PostgreSQL — джерело правди про стан задачі**. RabbitMQ відповідає
за доставку, а не за стан. Для гарантії доставки використовується
transactional outbox: подія записується в БД разом із задачею і публікується
в брокер окремим релеєм.

## Версіонування платформи

| Версія | Що входить |
|---|---|
| **0.1** | FastAPI, PostgreSQL, RabbitMQ, один worker, цикл create→queue→run→result, базові статуси, outbox, docker-compose |
| **0.2** | auto-retry з exponential backoff, retry-планувальник, DLQ (`dead_letter`), task attempts, timeout, cancellation, idempotency (replay за ключем) |
| **0.3** | кілька worker'ів, heartbeat, детекція мертвих worker'ів + відновлення завислих задач, пріоритетні черги, scheduled tasks, lease-виконання (claim/renew/recovery) |
| **0.4** | Redis pub/sub як канал live-подій, WebSocket `/api/v1/ws/events`, операційний дашборд (статистика, воркери, live-стрічка), `GET /api/v1/stats`, RabbitMQ queue depth |
| **0.5** | Prometheus `/metrics` + Grafana (provisioned dashboard), structured JSON-логи, HTTP-метрики, CI/CD (GitHub Actions), load/failure-тести |
| **1.0** | Kubernetes-маніфести, HPA (worker/backend), посібник розгортання, консолідована версія 1.0.0, CORS через env, self-healing зниклих `queued`-задач, фінальна документація |
| **1.1** | Auth: users, bcrypt, JWT access+refresh, ролі (admin/operator/viewer), RBAC на API/REST/WS, rate limiting, захищений дашборд |
| **1.2** | Rate limiting для всього API (per-user/per-IP, middleware `RateLimitMiddleware`), refresh token ротація з revoke (таблиця `refresh_tokens`, `/auth/logout`), зміна пароля і деактивація з revoke всіх refresh-токенів, KEDA + RabbitMQ-тригер для HPA воркера |
| **1.3** | Розподілений rate limiter у Redis (Lua sliding window, спільний для всіх реплік) з фолбеком на in-process локальний при недоступності Redis; автоматичний DLQ-retry за розкладом (dead-letter задачі повертаються в чергу до `DLQ_RETRY_MAX_CYCLES` циклів з інтервалом `DLQ_RETRY_INTERVAL_SECONDS`, далі лишаються terminal) |
| **1.4** | DLQ-моніторинг у дашборді (панель dead letter + ручний retry), refresh-токен-ліміт для WS, IP-ліміт повторного створення, CI-пайплайн (GitHub Actions: ruff + 68 тестів + compose-валідація) |
| **1.5 (поточна)** | _план_ |

## Швидкий старт

**Docker (рекомендовано):**

```bash
cp .env.example .env     # необов'язково — є dev-дефолти
docker compose up -d --build
docker compose up -d --scale worker=3   # кілька worker'ів
```

**Локальний запуск без Docker** (потрібні PostgreSQL 16+, RabbitMQ 4+, Redis 7+):

```bash
python -m venv .venv
.venv\Scripts\activate        # Windows; на Linux/macOS: source .venv/bin/activate
pip install -r backend/requirements.txt
python -m app.worker.main &   # воркер (окремий термінал)
uvicorn app.main:app --host 0.0.0.0 --port 8000 --app-dir backend
```

**Верифікація середовища** (Python 3.13+):

```bash
python -m pytest backend/tests -q       # 68 тестів: flow, retry, DLQ, scheduler, dashboard, load/failure, delivery semantics, auth + rate/rotation/password-rotation
python -m ruff check backend/app backend/tests backend/alembic
docker compose config -q                # валідність compose-файлу
curl http://localhost:8000/health       # {"status":"ok","database":true}
```

Сервіси:

| Сервіс | URL |
|---|---|
| API | http://localhost:8000 |
| Dashboard | http://localhost:8000 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (admin/admin) |
| Swagger | http://localhost:8000/docs |
| RabbitMQ UI | http://localhost:15672 (guest/guest) |
| Redis | localhost:6379 |

> Примітка: черга `task_executions` тепер оголошується з `x-max-priority`.
> Якщо черга вже існує без цього аргументу — видали її
> (`docker compose down -v` або RabbitMQ UI → Delete queue) до запуску.

## Повний цикл задачі

```text
POST /api/v1/tasks              -> PostgreSQL: queued (+ outbox event)
  |   (+ optional schedule_at -> scheduled, чекає часу)
        ↓
Outbox relay                    -> RabbitMQ: task_created (з priority)
        ↓
Worker (heartbeat у workers)    -> PostgreSQL: running + task_attempts[started]
        ↓
Executor (зареєстрований handler)
    │
    ├── success             -> PostgreSQL: success + результат в task_attempts
    ├── retryable error
    │     ├─ спроб менш як max_attempts → retry_scheduled (+ backoff delay)
    │     │       └─ Координатор після затримки → queued (+ outbox event)
    │     └─ спроби вичерпано          → dead_letter (DLQ)
    └── не-retryable error  -> PostgreSQL: failed

Worker помер (нема heartbeat) => Координатор мітить dead
  і повертає його завислу running-задачу в чергу → виконується іншим worker
```

### Статуси

`created → queued → running → success | failed | dead_letter | cancelled`
`created → scheduled → queued` (запланована задача)
`running → retry_scheduled → queued` (ретравня, знову в чергу)

- **failed** — постійна (не-retryable) помилка, напр. невідомий `task_type`
- **dead_letter** — retryable-помилка після вичерпання `max_attempts`
- **retry_scheduled** — задача чекає повторного запуску за backoff
- **scheduled** — запланована; запуститься після `schedule_at`
- **cancelled** — скасована (created/queued/running/retry_scheduled/scheduled)

## API

> **Авторизація (v1.1):** усі ендпоінти `/api/v1/*` (окрім `/auth/*` і `/health`) вимагають
> `Authorization: Bearer <access_token>`. Ролі: `admin` (усе + зміна ролей),
> `operator` (створення/retry/cancel задач), `viewer` (читання). WS приймає токен
> через query `?token=` (тільки access-токен; refresh і невалідні — `4401`). WS-підключення
> обмежені на користувача (`WS_CONNECT_LIMIT_MAX` за `WS_CONNECT_LIMIT_WINDOW_SECONDS`,
> перевищення — `4429`). За замовчуванням створюється admin (`ADMIN_USERNAME`/`ADMIN_PASSWORD`).
> Усі `/auth/*` обмежені rate limiter'ом (in-process, за IP), а весь API обмежений
> окремим rate limiter'ом (per-user за access-токеном або per-IP для анонімних;
> `API_RATE_LIMIT_MAX` запитів на `API_RATE_LIMIT_WINDOW_SECONDS`). API-лімітер —
> **розподілений**: sliding window тримається в Redis (Lua-скрипт), тому спільний
> для всіх реплік backend; при недоступності Redis автоматично вмикається
> локальний in-process фолбек (`RATE_LIMIT_REDIS_ENABLED=false` вимикає Redis).
> Створення задач додатково обмежене за IP (`TASK_CREATE_LIMIT_MAX` запитів на
> `TASK_CREATE_LIMIT_WINDOW_SECONDS`).

| Метод | Шлях | Опис | Доступ |
|---|---|---|---|
| POST | `/api/v1/auth/register` | Реєстрація (роль `viewer`) | public (rate-limited) |
| POST | `/api/v1/auth/login` | Логін → access + refresh токени | public (rate-limited) |
| POST | `/api/v1/auth/refresh` | Обмін refresh → новий access + ротація refresh (старий анулюється) | public (rate-limited) |
| POST | `/api/v1/auth/logout` | Відкликати refresh-токен (revoke) | public (rate-limited) |
| POST | `/api/v1/auth/change-password` | Зміна пароля → revoke всіх refresh-токенів користувача | авторизовані |
| GET | `/api/v1/auth/me` | Поточний користувач | авторизовані |
| POST | `/api/v1/auth/users/{id}/role` | Зміна ролі користувача | admin |
| POST | `/api/v1/auth/users/{id}/active` | Активувати/деактивувати (`is_active`); деактивація revoke всіх refresh-токенів | admin |
| POST | `/api/v1/tasks` | Створити задачу (поле `schedule_at` для відкладеного запуску; idempotent: повторний `idempotency_key` → 200 і та сама задача) | operator/admin |
| GET | `/api/v1/tasks` | Список + фільтри `status`, `task_type`, `priority` | авторизовані |
| GET | `/api/v1/tasks/{id}` | Деталі: спроби, історія подій | авторизовані |
| POST | `/api/v1/tasks/{id}/retry` | Повторно запустити failed/cancelled/dead_letter | operator/admin |
| POST | `/api/v1/tasks/{id}/cancel` | Скасувати created/queued/running/retry_scheduled/scheduled | operator/admin |
| GET | `/api/v1/tasks/{id}/events` | Історія переходів статусів | авторизовані |
| GET | `/api/v1/workers` | Список воркерів + стан heartbeat | авторизовані |
| GET | `/api/v1/stats` | Агрегована статистика (задачі, спроби, воркери, глибина черги) | авторизовані |
| WS | `/api/v1/ws/events?token=...` | Live-стрічка подій (проксіює Redis pub/sub) | авторизовані |
| GET | `/metrics` | Prometheus-метрики (стан + HTTP + виконання) | public (для скрейпу) |
| GET | `/` | Операційний дашборд (vanilla JS, з формою входу) | public (дані — через авторизований API) |
| GET | `/health` | Перевірка живучості | public |

### Дашборд та live-події (v0.4)

- API/воркери/координатор публікують події в канал Redis (`TASK_PLATFORM_EVENTS`) **після коміту** в транзакцію БД — фантомних подій немає
- `GET /api/v1/stats` віддає зріз із БД (статистика за статусами/пріоритетами/типами, воркери, спроби) + глибину RabbitMQ-черги (best-effort, `None` якщо недоступна)
- WebSocket `/api/v1/ws/events` підписується на Redis-канал і публікує кадри дашборду
- дашборд на `/` — одна статична HTML-сторінка: лічильники, розбивка за пріоритетом, таблиці воркерів і задач, live-події з авто-перепідключенням WS
- публікації best-effort (`REDIS_EVENTS_ENABLED=false` повністю вимикає канал); відсутність Redis не ламає основний флоу задач

### Метрики та спостережуваність (v0.5)

- `GET /metrics` віддає Prometheus-метрики: `tasks_created_total`, `attempts_total{outcome}`, `retries_total`, `lease_timeouts_total`, `task_duration_seconds{task_type}`, гаджі `tasks_status{status}`, `workers_active`, `outbox_pending`, `rabbitmq_queue_depth`, HTTP-метрики (`http_requests_total`, `http_request_duration_seconds`)
- HTTP-метрики агрегуються middleware по шаблонах роутів (без високої кардинальності по id)
- structured logging: `LOG_JSON=true` → логи в один рядок JSON (`ts`, `level`, `logger`, `message`, `exception`, кастомні поля); інакше текст
- `docker compose up` підіймає Prometheus (скрепить `backend:8000/metrics`) і Grafana з provisioned-дашбордом `deploy/grafana/provisioning/dashboards/tasks-platform.json`
- CI у `.github/workflows/ci.yml`: ruff + pytest + збірка Docker-образу на кожен push/PR

### Розгортання в Kubernetes (v1.0)

Повний стек у кластері (`deploy/k8s/`): Postgres StatefulSet + PVC, RabbitMQ, Redis,
backend з init-container для `alembic upgrade head`, воркер з HPA (CPU 60%, 1–10 подів)
і backend HPA (CPU 70%, 1–5 подів). Застосування:

```bash
docker build -t distributed-task-platform:1.2.0 backend/   # minikube image load ...
kubectl apply -k deploy/k8s
kubectl -n tasks-platform rollout status deployment/worker
kubectl -n tasks-platform port-forward svc/backend 8000:8000
```

Крок за кроком — у `deploy/k8s/README.md`. Для масштабування за глибиною черги
замість CPU-тригера підійде KEDA + RabbitMQ (зовнішній компонент).

### Пріоритети

`priority` задачі (critical/high/normal/low) впливає на порядок у черзі:

| priority | RabbitMQ priority |
|---|---|
| critical | 9 |
| high | 6 |
| normal | 3 |
| low | 1 |

### Ретраї

- транзитні помилки (timeout, будь-який виняток) вважаються retryable
- затримка між спробами — експоненційний backoff: `base · factor^(attempts−1)`, обмежений `max_delay`
- `max_attempts` на задачу (1..10); після вичерпання → `dead_letter`
- `POST /{id}/retry` вручну повертає у чергу навіть dead_letter

### Воркери та heartbeat (lease)

- кожен процес реєструється в `workers` з унікальним `worker_id` (`<id>-<pid>`)
- heartbeat оновлюється кожні `WORKER_HEARTBEAT_SECONDS`
- воркер, у якого немає heartbeat понад `WORKER_HEARTBEAT_TIMEOUT_SECONDS`, мітиться `dead`
- кожна задача має **lease**: воркером потрібно її спершу «зарезервувати» (`try_acquire_lease`), а координатор повертає в чергу задачі з простроченим `lease_expires_at` (для `running` — попередній спроба стає `failed`)
- під час виконання воркер **подовжує lease** (`renew_lease`) кожні `TASK_LEASE_SECONDS / 3`, тому довгі задачі не скасовуються
- кілька воркерів тягнуть з однієї черги; координатор (requeue/відновлення) використовує `FOR UPDATE SKIP LOCKED` у Postgres, щоб не дублювати задачі

### Типи задач

Безпечний зареєстрований реєстр (без довільного коду):

| task_type | payload | Результат |
|---|---|---|
| `echo` | `{"message": "hi"}` | `{"message": "hi", "task_type": "echo"}` |
| `sleep` | `{"seconds": 2}` (≤30) | `{"slept": 2}` |
| `fail` | `{"reason": "demo"}` | падає — демонструє ретраї та DLQ |

Приклад:

```bash
curl -X POST localhost:8000/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{"task_type":"sleep","payload":{"seconds":2}}'

curl localhost:8000/api/v1/tasks
# через ~5 секунд статус стане success, результат — у task_attempts
```

## Структура

```text
backend/
├── alembic/                # міграції (0001: tasks/attempts/events/outbox; 0002: workers; 0003: task lease)
├── app/
│   ├── api/routes/         # tasks, workers, stats, metrics, health, ws/events — REST/WS API
│   ├── core/               # config, logging (JSON), metrics (Prometheus + HTTP middleware)
│   ├── db/                 # сесія, Base, Redis (канал live-подій)
│   ├── models/             # task, attempt, event, outbox, worker
│   ├── schemas/            # Pydantic
│   ├── services/           # tasks (state machine + outbox запис), workers, events (Redis pub/sub), stats, queue_stats, outbox relay
│   ├── static/             # dashboard.html (vanilla JS)
│   └── worker/             # consumer, executors (реєстр handler'ів), attempts, координатор (requeue + відновлення), heartbeat
├── tests/                 # unit + flow + scheduling + dashboard + load/failure
└── Dockerfile              # один образ для backend i worker (різні CMD)
```

```text
deploy/
├── prometheus/prometheus.yml        # скрейп backend:8000/metrics
├── grafana/
│   ├── provisioning/datasources/    # пром-джерело
│   └── provisioning/dashboards/     # provider + tasks-platform.json
└── k8s/                             # маніфести кластера + посібник розгортання
```

## Гарантії (принципи, що їх дотримується система)

- at-least-once доставка; виконання не рівно один раз → ідемпотентність (за задумом, для створення — replay за `idempotency_key`)
- retryable-помилки автоматично повторюються з exponential backoff, після `max_attempts` — у DLQ (`dead_letter`)
- кожна спроба — окремий запис у `task_attempts`
- усі переходи статусів — у `task_events`, через єдиний state machine
- задача не втрачається при падінні RabbitMQ (outbox зберігає і повторює)
- зависла `running`-задача (worker помер) повертається в чергу координатором, а не застрягає назавжди