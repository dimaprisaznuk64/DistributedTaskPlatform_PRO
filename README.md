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
| **0.2 (поточна)** | auto-retry з exponential backoff, retry-планувальник, DLQ (`dead_letter`), task attempts, timeout, cancellation, idempotency (replay за ключем) |
| 0.3 | кілька worker'ів, heartbeat, пріоритетні черги, scheduled tasks, Redis |
| 0.4 | Next.js Dashboard, WebSocket, realtime-статуси |
| 0.5 | Prometheus, Grafana, structured logging, CI/CD, load/failure tests |
| 1.0 | Kubernetes, HPA, повна документація |

## Швидкий старт

```bash
cp .env.example .env     # необов'язково — є dev-дефолти
docker compose up -d --build
```

Сервіси:

| Сервіс | URL |
|---|---|
| API | http://localhost:8000 |
| Swagger | http://localhost:8000/docs |
| RabbitMQ UI | http://localhost:15672 (guest/guest) |

## Повний цикл задачі

```text
POST /api/v1/tasks          -> PostgreSQL: queued  (+ outbox event)
        ↓
Outbox relay                -> RabbitMQ: task_created
        ↓
Worker                      -> PostgreSQL: running + task_attempts[started]
        ↓
Executor (зареєстрований handler)
    │
    ├── success             -> PostgreSQL: success + результат в task_attempts
    ├── retryable error
    │     ├─ спроб менш як max_attempts → retry_scheduled (+ backoff delay)
    │     │       └─ Retry-планувальник після затримки → queued (+ outbox event)
    │     └─ спроби вичерпано          → dead_letter (DLQ)
    └── не-retryable error  -> PostgreSQL: failed
```

### Статуси

`created → queued → running → success | failed | dead_letter | cancelled`
`running → retry_scheduled → queued` (ретравня, знову в чергу)

- **failed** — постійна (не-retryable) помилка, напр. невідомий `task_type`
- **dead_letter** — retryable-помилка після вичерпання `max_attempts`
- **retry_scheduled** — задача чекає повторного запуску за backoff
- **cancelled** — скасована (доступна для завдань created/queued/running/retry_scheduled)

## API

| Метод | Шлях | Опис |
|---|---|---|
| POST | `/api/v1/tasks` | Створити задачу (idempotent: повторний `idempotency_key` → 200 і та сама задача) |
| GET | `/api/v1/tasks` | Список + фільтри `status`, `task_type`, `priority` |
| GET | `/api/v1/tasks/{id}` | Деталі: спроби, історія подій |
| POST | `/api/v1/tasks/{id}/retry` | Повторно запустити failed/cancelled/dead_letter |
| POST | `/api/v1/tasks/{id}/cancel` | Скасувати created/queued/running/retry_scheduled |
| GET | `/api/v1/tasks/{id}/events` | Історія переходів статусів |

### Ретраї

- транзитні помилки (timeout, будь-який виняток) вважаються retryable
- затримка між спробами — експоненційний backoff: `base · factor^(attempts−1)`, обмежений `max_delay`
- `max_attempts` на задачу (1..10); після вичерпання → `dead_letter`
- `POST /{id}/retry` вручну повертає у чергу навіть dead_letter

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
├── alembic/                # міграції (0001: tasks/attempts/events/outbox)
├── app/
│   ├── api/routes/tasks.py # REST API
│   ├── core/config.py      # налаштування (env)
│   ├── db/                 # сесія, Base
│   ├── models/             # task, attempt, event, outbox
│   ├── schemas/            # Pydantic
│   ├── services/           # tasks (state machine + outbox запис), outbox relay
│   └── worker/             # consumer, executors (реєстр handler'ів), попытки, retry-планувальник
├── tests/
└── Dockerfile              # один образ для backend i worker (різні CMD)
```

## Гарантії (принципи, що їх дотримується система)

- at-least-once доставка; виконання не рівно один раз → ідемпотентність (за задумом, для створення — replay за `idempotency_key`)
- retryable-помилки автоматично повторюються з exponential backoff, після `max_attempts` — у DLQ (`dead_letter`)
- кожна спроба — окремий запис у `task_attempts`
- усі переходи статусів — у `task_events`, через єдиний state machine
- задача не втрачається при падінні RabbitMQ (outbox зберігає і повторює)