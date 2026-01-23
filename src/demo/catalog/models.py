from __future__ import annotations

from typing import Dict, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class HealthThresholds(BaseModel):
    at_risk_after_days: int
    overdue_after_days: int

    @model_validator(mode="after")
    def _validate_thresholds(self) -> "HealthThresholds":
        if self.overdue_after_days < self.at_risk_after_days:
            raise ValueError("overdue_after_days must be >= at_risk_after_days")
        return self


class ProcessSpec(BaseModel):
    display_name: str
    owner: str
    steps: List[str]
    health: HealthThresholds
    step_aliases: Optional[Dict[str, List[str]]] = None
    process_aliases: Optional[List[str]] = None

    @field_validator("steps")
    @classmethod
    def _steps_non_empty_and_unique(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError("steps must be a non-empty list")
        normed = [" ".join(s.strip().split()).lower() for s in v]
        if len(set(normed)) != len(normed):
            raise ValueError("steps must be unique after normalization (lower/strip)")
        return v


class ProcessCatalog(BaseModel):
    processes: Dict[str, ProcessSpec]

    @model_validator(mode="after")
    def _validate_keys(self) -> "ProcessCatalog":
        for key in self.processes.keys():
            if not key or not isinstance(key, str):
                raise ValueError("process keys must be non-empty strings")
        return self


class ClientSpec(BaseModel):
    name: str
    aliases: List[str] = Field(default_factory=list)


class ClientsCatalog(BaseModel):
    clients: List[ClientSpec]


class RolesCatalog(BaseModel):
    canonical: List[str]
    aliases: Dict[str, List[str]] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_roles(self) -> "RolesCatalog":
        if "Other" not in self.canonical or "Unknown" not in self.canonical:
            raise ValueError('roles.canonical must include "Other" and "Unknown"')
        for key in self.aliases.keys():
            if key not in self.canonical:
                raise ValueError(f'alias key "{key}" is not in roles.canonical')
        return self
