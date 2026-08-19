"""SQLAlchemy engine and session lifecycle helpers."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from sentinelstream.config.settings import AppSettings
from sentinelstream.database import models as _models  # noqa: F401
from sentinelstream.database.base import Base


def resolved_database_url(settings: AppSettings) -> str:
    """Resolve legacy and Phase 9 database settings without breaking callers."""

    configured = settings.database.url or settings.database_url
    if configured:
        return configured
    default_path = Path(settings.data_dir) / "sentinelstream.db"
    default_path.parent.mkdir(parents=True, exist_ok=True)
    return f"sqlite:///{default_path}"


@dataclass(slots=True)
class Database:
    """Engine, session factory, and explicit schema helpers."""

    engine: Engine
    session_factory: sessionmaker[Session]

    def session(self) -> Session:
        return self.session_factory()

    def create_all(self) -> None:
        Base.metadata.create_all(self.engine)

    def drop_all(self) -> None:
        Base.metadata.drop_all(self.engine)

    def close(self) -> None:
        self.engine.dispose()


def create_database(settings: AppSettings | None = None, *, url: str | None = None) -> Database:
    """Create a database wrapper for PostgreSQL, SQLite, or another SQLAlchemy URL."""

    settings = settings or AppSettings()
    database_url = url or resolved_database_url(settings)
    kwargs: dict[str, object] = {"echo": settings.database.echo, "future": True}
    if database_url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}
    else:
        kwargs.update(
            pool_size=settings.database.pool_size,
            max_overflow=settings.database.max_overflow,
            pool_timeout=settings.database.pool_timeout_seconds,
        )
    engine = create_engine(database_url, **kwargs)
    return Database(engine, sessionmaker(bind=engine, expire_on_commit=False))


def session_scope(database: Database) -> Iterator[Session]:
    """Commit a unit of work or roll it back before closing the session."""

    session = database.session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
