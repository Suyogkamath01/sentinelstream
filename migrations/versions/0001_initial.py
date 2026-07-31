"""Create Phase 9 persistence tables."""

from __future__ import annotations

from alembic import op

from sentinelstream.database import models as _models  # noqa: F401
from sentinelstream.database.base import Base

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create the complete initial relational schema from typed metadata."""

    Base.metadata.create_all(op.get_bind())


def downgrade() -> None:
    """Remove the initial Phase 9 schema."""

    Base.metadata.drop_all(op.get_bind())
