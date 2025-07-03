# peter.py
import asyncio

import uvicorn
from fastapi import FastAPI
from fastapi import File
from fastapi import HTTPException
from fastapi import UploadFile

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
from eventsourcing.store.event_store.sqlite import SQLiteEventStore
from eventsourcing.store.outbox.in_memory import InMemoryOutbox

# ─────────────────────────────────────────────────────────────────────────────
# 1. Core infrastructure
# ─────────────────────────────────────────────────────────────────────────────

event_store = SQLiteEventStore(db_path="events.db")
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

stop_event = asyncio.Event()

processor = AtLeastOnceProcessor(
    es=event_store,
    outbox=outbox,
    publisher=broker,
    dead_stream=cfg.dead_stream,
    polling_interval=cfg.polling_interval,
    stop_event=stop_event,
)

router = Router(broker, stop_event=stop_event)
router.add_middleware(logging_middleware)
router.add_middleware(retry_middleware)
router.add_middleware(metrics_middleware)
router.add_middleware(dedupe_middleware)

# ─────────────────────────────────────────────────────────────────────────────
# 2. Domain handlers
# ─────────────────────────────────────────────────────────────────────────────


class OCRHandler:
    """Consumes UploadPrescription → emits OCRCompleted."""

    async def handle(self, msg: Message):
        text = "Dummy OCR result: prescription for DrugX 100mg daily."
        ev = Message(
            name="OCRCompleted",
            payload={"text": text},
            stream="prescription.ocr",
            correlation_id=msg.correlation_id or msg.id,
            causation_id=msg.id,
        )
        await outbox.enqueue([ev])
        return []


class InteractionFinderHandler:
    """Finds drug–drug interactions."""

    async def handle(self, msg: Message):
        interactions = [{"pair": ["DrugX", "DrugY"], "severity": "moderate"}]
        ev = Message(
            name="InteractionAnalysisCompleted",
            payload={"interactions": interactions},
            stream="prescription.analysis.interactions",
            correlation_id=msg.correlation_id,
            causation_id=msg.id,
        )
        await outbox.enqueue([ev])
        return []


class DoppelverordnungHandler:
    """Detects duplicate prescriptions."""

    async def handle(self, msg: Message):
        duplicates = [{"drug": "DrugX", "count": 2}]
        ev = Message(
            name="DuplicatePrescriptionDetected",
            payload={"duplicates": duplicates},
            stream="prescription.analysis.duplicates",
            correlation_id=msg.correlation_id,
            causation_id=msg.id,
        )
        await outbox.enqueue([ev])
        return []


class ContraindicationHandler:
    """Checks contraindications."""

    async def handle(self, msg: Message):
        contraindications = [{"drug": "DrugX", "issue": "kidney impairment"}]
        ev = Message(
            name="ContraindicationAnalysisCompleted",
            payload={"contraindications": contraindications},
            stream="prescription.analysis.contraindications",
            correlation_id=msg.correlation_id,
            causation_id=msg.id,
        )
        await outbox.enqueue([ev])
        return []


class FanOutHandler:
    """After OCR, fan‐out into the three analysis streams."""

    def __init__(self):
        self.handlers = [
            InteractionFinderHandler(),
            DoppelverordnungHandler(),
            ContraindicationHandler(),
        ]

    async def handle(self, msg: Message):
        for h in self.handlers:
            await h.handle(msg)
        return []


# Register routes
router.add_route("prescriptions.uploaded", OCRHandler())
router.add_route("prescription.ocr", FanOutHandler())

# ─────────────────────────────────────────────────────────────────────────────
# 3. FastAPI app
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
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
async def upload_prescription(file: UploadFile = File(...)):
    data = await file.read()
    cmd = Message(
        name="UploadPrescription",
        payload=data,
        stream="prescriptions.uploaded",
    )
    await outbox.enqueue([cmd])
    return {
        "status": "uploaded",
        "correlation_id": cmd.correlation_id or cmd.id,
    }


@app.get("/analysis/{correlation_id}")
async def get_analysis(correlation_id: str):
    async def fetch(stream):
        evs = await event_store.read_stream(stream)
        return next(
            (e for e in evs if e.correlation_id == correlation_id), None
        )

    ocr_ev = await fetch("prescription.ocr")
    if not ocr_ev:
        raise HTTPException(404, "OCR analysis not found")

    inter_ev = await fetch("prescription.analysis.interactions")
    dup_ev = await fetch("prescription.analysis.duplicates")
    contra_ev = await fetch("prescription.analysis.contraindications")

    return {
        "ocr_text": ocr_ev.payload["text"],
        "interactions": inter_ev.payload["interactions"] if inter_ev else [],
        "duplicates": dup_ev.payload["duplicates"] if dup_ev else [],
        "contraindications": contra_ev.payload["contraindications"]
        if contra_ev
        else [],
    }


if __name__ == "__main__":
    uvicorn.run("peter:app", host="0.0.0.0", port=8000, reload=True)
