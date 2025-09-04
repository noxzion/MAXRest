from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional

from sqlmodel import Field, SQLModel


class SessionEntry(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    phone: str = Field(index=True, unique=True)
    session_dir: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    last_used_at: datetime = Field(default_factory=datetime.utcnow, index=True)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
