import asyncio
import base64
import logging
from typing import Any, Dict, List
from typing import Optional

import uvicorn
from fastapi import FastAPI, File, UploadFile
from fastapi import HTTPException
from pydantic import BaseModel

from eventsourcing.config import ESConfig
from eventsourcing.fastapi_utils import lifespan_manager
from eventsourcing.interfaces import Message
from eventsourcing.log_config import configure_logging
from eventsourcing.middleware import dedupe_middleware
from eventsourcing.middleware import logging_middleware
from eventsourcing.middleware import metrics_middleware
from eventsourcing.middleware import retry_middleware
from eventsourcing.processor.at_least_once_processor import (
    AtLeastOnceProcessor,
)
from eventsourcing.pubsub.in_memory import InMemoryPubSub
from eventsourcing.router import Router
from eventsourcing.store.event_store.postgres import PostgresEventStore
from eventsourcing.store.outbox.in_memory import InMemoryOutbox

# Configure logging level
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("eventsourcing")
logger.setLevel(logging.INFO)

# ─────────────────────────────────────────────────────────────────────────────
# 0. Payload models
# ─────────────────────────────────────────────────────────────────────────────


class BasePayload(BaseModel):
    def to_dict(self) -> Dict[str, Any]:
        serialized = self.model_dump()
        for key, value in serialized.items():
            if isinstance(value, bytes):
                serialized[key] = base64.b64encode(value).decode("utf-8")
        return serialized


class UploadPrescriptionPayload(BasePayload):
    data: bytes


class OCRCompletedPayload(BasePayload):
    text: str


class Interaction(BasePayload):
    pair: List[str]
    severity: str


class InteractionAnalysisPayload(BasePayload):
    interactions: List[Interaction]


class Duplicate(BasePayload):
    drug: str
    count: int


class DuplicatePrescriptionPayload(BasePayload):
    duplicates: List[Duplicate]


class Contraindication(BasePayload):
    drug: str
    issue: str


class ContraindicationAnalysisPayload(BasePayload):
    contraindications: List[Contraindication]


# ─────────────────────────────────────────────────────────────────────────────
# 1. Core infrastructure
# ─────────────────────────────────────────────────────────────────────────────

event_store = PostgresEventStore(
    dsn="postgresql://postgres:postgres@localhost:5432/eventsourcing"
)
outbox = InMemoryOutbox()
broker = InMemoryPubSub(backlog_limit=1000)

cfg = ESConfig(
    publisher=broker,
    subscriber=broker,
    dead_stream="dead",
    polling_interval=0.1,
    json_logging=False,
)
configure_logging(cfg.json_logging)

stop_event: asyncio.Event = asyncio.Event()

processor: AtLeastOnceProcessor = AtLeastOnceProcessor(
    es=event_store,
    outbox=outbox,
    publisher=broker,
    dead_stream=cfg.dead_stream,
    polling_interval=cfg.polling_interval,
    stop_event=stop_event,
)

router: Router = Router(broker, stop_event=stop_event)
router.add_middleware(logging_middleware)
router.add_middleware(retry_middleware)
router.add_middleware(metrics_middleware)
router.add_middleware(dedupe_middleware)


# ─────────────────────────────────────────────────────────────────────────────
# 2. Domain handlers (typed)
# ─────────────────────────────────────────────────────────────────────────────


class OCRHandler:
    async def handle(
        self, msg: Message[UploadPrescriptionPayload]
    ) -> List[Message[Any]]:
        # Dummy OCR logic
        text = "Dummy OCR result: prescription for DrugX 100mg daily."
        payload = OCRCompletedPayload(text=text)
        ev: Message[OCRCompletedPayload] = Message[OCRCompletedPayload](
            name="OCRCompleted",
            payload=payload,
            stream="prescription.ocr",
            correlation_id=msg.correlation_id or msg.id,
            causation_id=msg.id,
        )
        await outbox.enqueue([ev])
        return []


class InteractionFinderHandler:
    async def handle(
        self, msg: Message[OCRCompletedPayload]
    ) -> List[Message[Any]]:
        interactions = [
            Interaction(pair=["DrugX", "DrugY"], severity="moderate")
        ]
        payload = InteractionAnalysisPayload(interactions=interactions)
        ev: Message[InteractionAnalysisPayload] = Message[
            InteractionAnalysisPayload
        ](
            name="InteractionAnalysisCompleted",
            payload=payload,
            stream="prescription.analysis.interactions",
            correlation_id=msg.correlation_id,
            causation_id=msg.id,
        )
        await outbox.enqueue([ev])
        return []


class DoppelverordnungHandler:
    async def handle(
        self, msg: Message[OCRCompletedPayload]
    ) -> List[Message[Any]]:
        duplicates = [Duplicate(drug="DrugX", count=2)]
        payload = DuplicatePrescriptionPayload(duplicates=duplicates)
        ev: Message[DuplicatePrescriptionPayload] = Message[
            DuplicatePrescriptionPayload
        ](
            name="DuplicatePrescriptionDetected",
            payload=payload,
            stream="prescription.analysis.duplicates",
            correlation_id=msg.correlation_id,
            causation_id=msg.id,
        )
        await outbox.enqueue([ev])
        return []


class ContraindicationHandler:
    async def handle(
        self, msg: Message[OCRCompletedPayload]
    ) -> List[Message[Any]]:
        contraindications = [
            Contraindication(drug="DrugX", issue="kidney impairment")
        ]
        payload = ContraindicationAnalysisPayload(
            contraindications=contraindications
        )
        ev: Message[ContraindicationAnalysisPayload] = Message[
            ContraindicationAnalysisPayload
        ](
            name="ContraindicationAnalysisCompleted",
            payload=payload,
            stream="prescription.analysis.contraindications",
            correlation_id=msg.correlation_id,
            causation_id=msg.id,
        )
        await outbox.enqueue([ev])
        return []


class FanOutHandler:
    def __init__(self) -> None:
        self.handlers: List[Any] = [
            InteractionFinderHandler(),
            DoppelverordnungHandler(),
            ContraindicationHandler(),
        ]

    async def handle(
        self, msg: Message[OCRCompletedPayload]
    ) -> List[Message[Any]]:
        for h in self.handlers:
            await h.handle(msg)
        return []


# Register routes
router.add_route("prescriptions.uploaded", OCRHandler())  # type: ignore[arg-type]
router.add_route("prescription.ocr", FanOutHandler())  # type: ignore[arg-type]

# ─────────────────────────────────────────────────────────────────────────────
# 3. FastAPI app
# ─────────────────────────────────────────────────────────────────────────────

app: FastAPI = FastAPI(
    lifespan=lifespan_manager(
        router,
        processor,
        streams=[
            "dead",
            "prescriptions.uploaded",
            "prescription.ocr",
            "prescription.analysis.interactions",
            "prescription.analysis.duplicates",
            "prescription.analysis.contraindications",
        ],
    )
)


@app.post("/upload_prescription/")
async def upload_prescription(file: UploadFile = File(...)) -> Dict[str, str]:
    data: bytes = await file.read()
    payload: UploadPrescriptionPayload = UploadPrescriptionPayload(data=data)
    cmd: Message[UploadPrescriptionPayload] = Message[
        UploadPrescriptionPayload
    ](
        name="UploadPrescription",
        payload=payload,
        stream="prescriptions.uploaded",
    )
    await outbox.enqueue([cmd])
    return {
        "status": "uploaded",
        "correlation_id": cmd.correlation_id or cmd.id,
    }


# Update the list_projections function to handle payload as a dictionary
@app.get("/projections/")
async def list_projections() -> Dict[str, List[Dict[str, Any]]]:
    projections = {}
    streams = [
        "prescription.ocr",
        "prescription.analysis.interactions",
        "prescription.analysis.duplicates",
        "prescription.analysis.contraindications",
    ]

    for stream in streams:
        evs = await event_store.read_stream(stream)
        projections[stream] = [
            {
                **ev.payload,  # Use payload directly as a dictionary
                "id": str(ev.id),
                "correlation_id": str(ev.correlation_id),
                "causation_id": str(ev.causation_id),
            }
            for ev in evs
        ]

    return projections


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
