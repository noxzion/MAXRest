from __future__ import annotations

from datetime import datetime
from pathlib import Path
from random import randint
from typing import Optional

from sqlmodel import Session, SQLModel, create_engine, select

from .models import SessionEntry, ensure_dir


class RestDB:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        ensure_dir(db_path.parent)
        self.engine = create_engine(f"sqlite:///{db_path}")
        SQLModel.metadata.create_all(self.engine)

    def session(self) -> Session:
        return Session(self.engine)

    def get_or_create_session_dir(self, phone: str, base_cache: Path) -> Path:
        with self.session() as s:
            entry: Optional[SessionEntry] = s.exec(
                select(SessionEntry).where(SessionEntry.phone == phone)
            ).first()
            if entry:
                entry.last_used_at = datetime.utcnow()
                s.add(entry)
                s.commit()
                s.refresh(entry)
                return Path(entry.session_dir)

            suffix = f"{randint(10_000_000, 99_999_999)}"
            session_dir = base_cache / f"{phone}-{suffix}"
            ensure_dir(session_dir)

            entry = SessionEntry(phone=phone, session_dir=str(session_dir))
            s.add(entry)
            s.commit()
            s.refresh(entry)
            return Path(entry.session_dir)

