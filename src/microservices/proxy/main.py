import os
import random
import logging
from contextlib import asynccontextmanager
from typing import Optional
import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ProxyService:
    def __init__(self):
        self.monolith_url = os.getenv("MONOLITH_URL", "http://monolith:8080")
        self.movies_service_url = os.getenv("MOVIES_SERVICE_URL", "http://movies:8081")
        self.movies_migration_percent = int(os.getenv("MOVIES_MIGRATION_PERCENT", "0"))
        self.client: Optional[httpx.AsyncClient] = None

    async def start(self):
        self.client = httpx.AsyncClient(timeout=30.0)
        logger.info(f"Proxy service started on port {os.getenv('PORT', '8000')}")
        logger.info(f"Movies migration percent: {self.movies_migration_percent}%")
        logger.info(f"Monolith URL: {self.monolith_url}")
        logger.info(f"Movies service URL: {self.movies_service_url}")

    async def stop(self):
        if self.client:
            await self.client.aclose()

    def should_route_to_movies(self) -> bool:
        """Определяет, нужно ли направлять запрос в movies service"""
        return random.randint(1, 100) <= self.movies_migration_percent

    async def proxy_request(self, target_url: str, request: Request) -> Response:
        """Проксирование запроса к целевому сервису"""
        try:
            # Подготовка URL
            path = request.url.path
            query = request.url.query
            url = f"{target_url}{path}"
            if query:
                url += f"?{query}"

            # Получение тела запроса
            body = await request.body()

            # Подготовка заголовков
            headers = dict(request.headers)
            headers.pop("host", None)

            # Отправка запроса
            response = await self.client.request(
                method=request.method,
                url=url,
                headers=headers,
                content=body
            )

            # Возврат ответа
            return Response(
                content=response.content,
                status_code=response.status_code,
                headers=dict(response.headers)
            )

        except httpx.RequestError as e:
            logger.error(f"Error proxying request to {target_url}: {e}")
            return JSONResponse(
                status_code=502,
                content={"error": f"Service unavailable: {str(e)}"}
            )

    async def handle_movies(self, request: Request) -> Response:
        """Обработка запросов к /api/movies с поддержкой feature flags"""
        if self.should_route_to_movies():
            logger.info(f"Routing /api/movies to movies service ({self.movies_migration_percent}% traffic)")
            return await self.proxy_request(self.movies_service_url, request)
        else:
            logger.info(f"Routing /api/movies to monolith ({100 - self.movies_migration_percent}% traffic)")
            return await self.proxy_request(self.monolith_url, request)

    async def handle_events(self, request: Request) -> Response:
        """Обработка запросов к /api/events"""
        logger.info(f"Routing {request.url.path} to events service")
        events_url = os.getenv("EVENTS_SERVICE_URL", "http://events:8082")
        return await self.proxy_request(events_url, request)

    async def handle_default(self, request: Request) -> Response:
        """Обработка всех остальных запросов"""
        logger.info(f"Routing {request.url.path} to monolith")
        return await self.proxy_request(self.monolith_url, request)


# Создание сервиса
proxy_service = ProxyService()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await proxy_service.start()
    yield
    await proxy_service.stop()


# Создание FastAPI приложения
app = FastAPI(
    title="CinemaAbyss Proxy Service",
    description="API Gateway with Strangler Fig pattern",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "proxy",
        "movies_migration": f"{proxy_service.movies_migration_percent}%"
    }


@app.api_route("/api/movies/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def movies_proxy(request: Request):
    """Прокси для /api/movies с поддержкой feature flags"""
    return await proxy_service.handle_movies(request)


@app.api_route("/api/events/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def events_proxy(request: Request):
    """Прокси для /api/events"""
    return await proxy_service.handle_events(request)


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def default_proxy(request: Request):
    """Прокси для всех остальных запросов"""
    return await proxy_service.handle_default(request)


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)