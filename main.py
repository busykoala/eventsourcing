import asyncio
import base64
import logging
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import uvicorn
from fastapi import FastAPI
from fastapi import File
from fastapi import HTTPException
from fastapi import UploadFile
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
                serialized[key] = base64.b64encode(value).decode(
                    "utf-8"
                )  # Encode bytes to Base64
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


@app.get("/analysis/{correlation_id}")
async def get_analysis(correlation_id: str) -> Dict[str, Any]:
    async def fetch(stream: str) -> Optional[Message[Any]]:
        evs = await event_store.read_stream(stream)
        return next(
            (e for e in evs if e.correlation_id == correlation_id), None
        )

    # 1. OCR result
    ocr_ev = await fetch("prescription.ocr")
    if ocr_ev is None:
        raise HTTPException(status_code=404, detail="OCR analysis not found")

    raw_ocr = ocr_ev.payload
    if hasattr(raw_ocr, "text"):
        ocr_text = raw_ocr.text
    else:
        ocr_text = raw_ocr.get("text", "")

    # 2. Interaction analysis
    inter_ev = await fetch("prescription.analysis.interactions")
    interactions: List[Any] = []
    if inter_ev:
        raw_inter = inter_ev.payload
        if hasattr(raw_inter, "interactions"):
            interactions = raw_inter.interactions
        else:
            interactions = raw_inter.get("interactions", [])

    # 3. Duplicate detection
    dup_ev = await fetch("prescription.analysis.duplicates")
    duplicates: List[Any] = []
    if dup_ev:
        raw_dup = dup_ev.payload
        if hasattr(raw_dup, "duplicates"):
            duplicates = raw_dup.duplicates
        else:
            duplicates = raw_dup.get("duplicates", [])

    # 4. Contraindications
    contra_ev = await fetch("prescription.analysis.contraindications")
    contraindications: List[Any] = []
    if contra_ev:
        raw_contra = contra_ev.payload
        if hasattr(raw_contra, "contraindications"):
            contraindications = raw_contra.contraindications
        else:
            contraindications = raw_contra.get("contraindications", [])

    return {
        "ocr_text": ocr_text,
        "interactions": interactions,
        "duplicates": duplicates,
        "contraindications": contraindications,
    }


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
