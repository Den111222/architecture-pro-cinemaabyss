import os
import json
import asyncio
import logging
import uuid
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional
from enum import Enum

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
# KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user-events")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "events-service")


class EventType(str, Enum):
    USER = "user"
    PAYMENT = "payment"
    MOVIE = "movie"

class PaymentStatus(str, Enum):
    COMPLETED = "completed"
    FAILED = "failed"
    PENDING = "pending"


class PaymentMethodType(str, Enum):
    CREDIT_CARD = "credit_card"
    USDT = "usdt"


class UserEvent(BaseModel):
    user_id: int
    action: str
    email: Optional[str] = None
    username: Optional[str] = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class PaymentEvent(BaseModel):
    payment_id: int
    user_id: int
    amount: float
    currency: str = "RUB"
    status: PaymentStatus
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    method_type: PaymentMethodType

class MovieEvent(BaseModel):
    movie_id: int
    user_id: Optional[int] = None
    action: str # viewed
    title: Optional[str] = None
    rating: Optional[int] = Field(None, ge=1, le=10)


class Event(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    data: Dict[str, Any]


class KafkaService:
    def __init__(self):
        self.producer: Optional[AIOKafkaProducer] = None
        self.consumer_task: Optional[asyncio.Task] = None

    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
        )
        await self.producer.start()
        logger.info("Kafka producer started")

        self.consumer_task = asyncio.create_task(self.consume_events())
        logger.info("Kafka consumer task started")

    async def stop(self):
        if self.consumer_task:
            self.consumer_task.cancel()
            try:
                await self.consumer_task
            except asyncio.CancelledError:
                pass

        if self.producer:
            await self.producer.stop()
            logger.info("Kafka producer stopped")

    async def produce_event(self, event_type: EventType, data: Dict[str, Any]) -> str:
        event = Event(event_type=event_type, data=data)

        try:
            await self.producer.send(
                KAFKA_TOPIC,
                value=event.model_dump()
            )
            logger.info(f"Event produced: {event_type.value} - {event.event_id}")
            return event.event_id
        except Exception as e:
            logger.error(f"Failed to produce event: {e}")
            raise

    async def consume_events(self):
        consumer = AIOKafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_CONSUMER_GROUP,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest'
        )

        await consumer.start()
        logger.info("Kafka consumer started")

        try:
            async for msg in consumer:
                event = msg.value
                logger.info(f"Received event: {event}")

                event_type = event.get('event_type')
                data = event.get('data', {})

                if event_type == 'user':
                    logger.info(f"Processing user event: {data.get('action')}")
                elif event_type == 'payment':
                    logger.info(f"Processing payment event: {data.get('status')}")
                elif event_type == 'movie':
                    logger.info(f"Processing movie event: {data.get('action')}")
        except Exception as e:
            logger.error(f"Error in consumer: {e}")
        finally:
            await consumer.stop()


kafka_service = KafkaService()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await kafka_service.start()
    yield
    await kafka_service.stop()


app = FastAPI(
    title="CinemaAbyss Events Service",
    description="Event processing service with Kafka",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/api/events/health")
async def health_check():
    return {"status": True, "service": "events"}


@app.post("/api/events/user", status_code=201)
async def create_user_event(event: UserEvent):
    try:
        event_id = await kafka_service.produce_event(
            EventType.USER,
            event.model_dump()
        )
        return {
            "status": "success",
            "event_id": event_id,
            "message": "User event created"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/events/payment", status_code=201)
async def create_payment_event(event: PaymentEvent):
    try:
        event_id = await kafka_service.produce_event(
            EventType.PAYMENT,
            event.model_dump()
        )
        return {
            "status": "success",
            "event_id": event_id,
            "message": "Payment event created"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/events/movie", status_code=201)
async def create_movie_event(
    event: MovieEvent
):
    try:
        event_id = await kafka_service.produce_event(
            EventType.MOVIE,
            event.model_dump()
        )
        return {
            "status": "success",
            "event_id": event_id,
            "message": "Movie event created"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8082"))
    uvicorn.run(app, host="0.0.0.0", port=port)