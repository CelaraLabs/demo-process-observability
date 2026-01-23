from __future__ import annotations

from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator


# Allowed process IDs for normalization
AllowedProcessId = Literal["recruiting", "project-management", "unknown"]

# Allowed step statuses
AllowedStepStatus = Literal["pending", "in_progress", "completed", "blocked", "canceled"]


class EvidenceRef(BaseModel):
    message_id: str
    timestamp: Optional[str] = None
    snippet: str = Field(..., min_length=1, max_length=240)


class StepSignal(BaseModel):
    step_name: Optional[str] = None
    direction: Literal["started", "completed", "blocked", "mentioned"]
    details: Optional[str] = None


AllowedEventType = Literal[
    "process_signal",
    "status_update",
    "blocker",
    "decision",
    "scheduling",
    "handoff",
    "doc_shared",
    "request",
    "unrelated",
]


class Pass1Event(BaseModel):
    # Required
    message_id: str
    event_type: AllowedEventType
    confidence: float
    evidence: EvidenceRef

    # Optional
    thread_id: Optional[str] = None
    candidate_client: Optional[str] = None
    candidate_process: Optional[str] = None
    candidate_role: Optional[str] = None
    status: Optional[str] = None
    step_signals: Optional[List[StepSignal]] = None
    entities: Optional[Dict] = None
    notes: Optional[str] = None

    @field_validator("confidence")
    @classmethod
    def _confidence_range(cls, v: float) -> float:
        if not (0.0 <= v <= 1.0):
            raise ValueError("confidence must be between 0 and 1")
        return v


class Pass3NormalizedStep(BaseModel):
    """Output schema for Pass3 step normalization."""

    process_id: AllowedProcessId
    phase_id: str
    step_id: Optional[str] = None
    step_status: AllowedStepStatus
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str

    @field_validator("confidence")
    @classmethod
    def _confidence_range(cls, v: float) -> float:
        if not (0.0 <= v <= 1.0):
            raise ValueError("confidence must be between 0 and 1")
        return v
