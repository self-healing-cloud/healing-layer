# Comprehensive 360-Degree Codebase Review: Niramay

## Executive Summary
Niramay is a cloud monitoring and self-healing infrastructure tool that provides a unified observation, detection, and healing pipeline. Overall, the codebase demonstrates a strong domain-driven structure, utilizing a multi-stage event-driven architecture with RabbitMQ and Redis. However, the current implementation suffers from architectural bottlenecks (mixing sync/async workloads, tightly coupling API and background workers), security risks (hardcoded credentials, missing API authentication), and performance inefficiencies (synchronous database writes during ingestion, aggressive HTTP polling). Addressing these technical debts will transform Niramay into a highly scalable, production-ready system.

## Critical Action Items
1. **Synchronous OpenSearch Writes in Ingestion Pipeline (`backend/app/ingestion/rabbitmq_consumer.py:46`)**
   - **Risk:** Severe performance bottleneck. The RabbitMQ consumer thread synchronously calls `opensearch_writer.write_normalized_log()` for every single log message before acknowledging it and pushing it to Redis. If OpenSearch experiences latency or throttling, the entire ingestion pipeline will back up, causing message queue buildup and delaying real-time detection.
   - **Fix:** Decouple OpenSearch writes by utilizing a bulk-write background task or asynchronous execution (`asyncio.create_task` or a separate dedicated OpenSearch writer worker).
2. **Development Server in Production Dockerfile (`backend/Dockerfile:8`)**
   - **Risk:** Severe performance and stability issue. The `CMD` runs Uvicorn with the `--reload` flag (`CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]`). This enables a file-watcher (watchfiles) that degrades performance, consumes excess memory, and is strongly discouraged for production.
   - **Fix:** Remove `--reload` from the Dockerfile. Use multiple Gunicorn workers with Uvicorn worker class for production.
3. **Failing Asynchronous Unit Tests (`backend/tests/test_analyser_worker.py` & `test_dispatcher_worker.py`)**
   - **Risk:** Tests are broken out of the box because the `pytest.mark.asyncio` decorator is not resolving properly with the test suite configuration, leading to `async def functions are not natively supported` errors. This leaves critical pipeline stages unchecked.
   - **Fix:** Properly configure `pytest-asyncio` in `pytest.ini` or `pyproject.toml` (e.g., `asyncio_mode = auto`) and ensure consistent event loop scoping.

## Architectural Recommendations
1. **Decouple API Process from Background Workers**
   - Currently, `main.py` launches the FastAPI server alongside all background workers (`detection_worker`, `analyser_worker`, `dispatcher_worker`, `rabbitmq_consumer`). If the API scales horizontally to handle more frontend load, it will spawn duplicate consumers and workers, leading to race conditions and duplicate processing.
   - **Recommendation:** Extract background workers into a separate entry point (e.g., `worker_main.py` or Celery/FastStream integration) to allow independent scaling of the API and worker tiers.
2. **Replace Aggressive HTTP Polling with WebSockets**
   - The frontend's `useNiramayData.ts` fires 6 separate HTTP requests every 3 seconds to fetch logs, anomalies, and stats. This pattern places excessive read-load on the FastAPI server and Redis instance.
   - **Recommendation:** Implement WebSockets or Server-Sent Events (SSE) in FastAPI to push real-time updates to the React frontend, eliminating polling overhead.
3. **Consolidate Message Brokers**
   - The architecture currently uses RabbitMQ for the first ingestion step, but immediately pivots to Redis queues (`brpop`, `lpush`) for stages 2, 3, and 4.
   - **Recommendation:** Standardize on a single broker to simplify infrastructure and reduce operational overhead. Redis Streams or RabbitMQ alone can handle the entire pipeline.
4. **Secure Failure Injection Middleware**
   - `FailureSimulationMiddleware` intercepts all requests to inject errors. If accidentally enabled in production, this would cause devastating outages.
   - **Recommendation:** Ensure failure injection is physically stripped from production builds or protected by strict authentication and environment-variable locks (`if not env.is_dev`).

## Quick Wins
1. **Remove Hardcoded Credentials:** The `docker-compose.yml` and `config.py` contain hardcoded secrets (`OPENSEARCH_PASSWORD: admin`, `RABBITMQ_PASSWORD: guest`). Move all credentials to a `.env` file excluded via `.gitignore`.
2. **Lock Dependencies:** Replace `requirements.txt` with a modern dependency manager like Poetry or UV (`pyproject.toml`, `uv.lock`) to ensure deterministic builds and automatically audit for vulnerable packages.
3. **Add API Authentication:** Add API Key or JWT middleware to secure the FastAPI endpoints. Currently, any user who can reach the API can access all data and trigger system resets.
4. **Fix Frontend Warnings:** The frontend test suite throws React Router future-flag warnings and Vite/esbuild deprecation warnings. Update the Vite config and React Router wrapper to resolve these noisy console logs.

## Refactored Code
Here is a refactored version of the RabbitMQ consumer (`backend/app/ingestion/rabbitmq_consumer.py`) that migrates it from a blocking, synchronous thread to a fully asynchronous pipeline using `aio-pika`. This resolves the critical OpenSearch blocking bottleneck and aligns the consumer with the rest of the application's asynchronous architecture.

```python
\"\"\"
Refactored Stage 1 — Async RabbitMQ Consumer
Migrated from synchronous pika threading to aio-pika.
Prevents OpenSearch writes from blocking the ingestion pipeline.
\"\"\"
import json
import asyncio
import structlog
import aio_pika
from app.core.config import settings
from app.core.redis_client import get_async_redis
from app.ingestion.normalizer import normalize_log
from app.ingestion.opensearch_client import opensearch_writer

logger = structlog.get_logger(__name__)

REDIS_OBSERVATION_LOGS = "observation:logs"
REDIS_PENDING_DETECTION = "observation:pending_detection"
REDIS_LOGS_CAP = 1000

async def _process_message(message: aio_pika.IncomingMessage, redis_client):
    \"\"\"Process a single message asynchronously.\"\"\"
    async with message.process():
        try:
            raw_message = message.body.decode("utf-8")
        except Exception:
            raw_message = str(message.body)

        # 1. Normalize
        normalized = normalize_log(raw_message)

        # 2. Write to OpenSearch in the background (Non-blocking)
        # Offload sync opensearch_writer to a threadpool if it doesn't support async natively
        asyncio.to_thread(opensearch_writer.write_normalized_log, normalized)

        # 3. Push to Redis Pipelines (Async)
        log_json = json.dumps(normalized)

        try:
            pipeline = redis_client.pipeline()
            pipeline.lpush(REDIS_OBSERVATION_LOGS, log_json)
            pipeline.ltrim(REDIS_OBSERVATION_LOGS, 0, REDIS_LOGS_CAP - 1)
            pipeline.rpush(REDIS_PENDING_DETECTION, log_json)
            await pipeline.execute()
        except Exception as e:
            logger.warning("Failed to push to Redis", error=str(e))

async def _consumer_loop():
    \"\"\"Async consumer loop with reconnection logic.\"\"\"
    retry_delay = 1
    redis_client = await get_async_redis()

    while True:
        try:
            connection = await aio_pika.connect_robust(
                host=settings.RABBITMQ_HOST,
                port=settings.RABBITMQ_PORT,
                login=settings.RABBITMQ_USER,
                password=settings.RABBITMQ_PASSWORD,
            )

            async with connection:
                channel = await connection.channel()
                await channel.set_qos(prefetch_count=100)
                queue = await channel.declare_queue(
                    settings.RABBITMQ_QUEUE_NAME,
                    durable=True
                )

                logger.info("Async RabbitMQ consumer started", queue=queue.name)
                retry_delay = 1 # Reset backoff

                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        await _process_message(message, redis_client)

        except Exception as e:
            logger.error("RabbitMQ consumer error, retrying...", error=str(e), retry_in=retry_delay)
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)

def start_rabbitmq_consumer():
    \"\"\"Start the RabbitMQ consumer as an asyncio task instead of a thread.\"\"\"
    asyncio.create_task(_consumer_loop())
    logger.info("Async RabbitMQ consumer task launched")
```
