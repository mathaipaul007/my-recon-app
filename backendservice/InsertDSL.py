from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Union

from sqlalchemy import insert
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.dml import Insert

Selectable = Union[str, ColumnElement]

class InsertDSL:
    """
    Insert builder:
        InsertInto(Model).values(...).returning(...).on_conflict_do_nothing(...)
    """

    def __init__(self, self, model):
        self.model = model
        self._values: Optional[Dict[str, Any]] = None
        self._many: Optional[List[Dict[str, Any]]] = None
        self._returning: List[Selectable] = []
        self._on_conflict: Optional[Dict[str, Any]] = None  # store options for PG upsert

    # -------- values --------
    def values(self, self, **kwargs) -> "InsertDSL":
        # single-row insert
        self._values = dict(kwargs)
        self._many = None
        return self

    def many(self, self, rows: Sequence[Dict[str, Any]]) -> "InsertDSL":
        # multi-row insert
        self._many = [dict(r) for r in rows]
        self._values = None
        return self

    # -------- returning --------
    def returning(self, self, *cols: Selectable) -> "InsertDSL":
        self._returning.extend(cols)
        return self

# ---------- postgres upsert helpers ----------

def on_conflict_do_nothing(self, *, index_elements: Sequence[str]) -> "InsertDSL":
    """
    INSERT ... ON CONFLICT (index_elements) DO NOTHING
    """
    self._on_conflict = {"action": "nothing", "index_elements": list(index_elements)}
    return self

def on_conflict_do_update(
    self,
    *,
    index_elements: Sequence[str],
    set_: Dict[str, Any],
) -> "InsertDSL":
    """
    INSERT ... ON CONFLICT (index_elements) DO UPDATE SET ...
    """
    self._on_conflict = {
        "action": "update",
        "index_elements": list(index_elements),
        "set_": dict(set_),
    }
    return self

# ---------- compilation ----------

def _compile_returning(self) -> List[ColumnElement]:
    out: List[ColumnElement] = []
    for c in self._returning:
        if isinstance(c, str):
            # allow returning("id") or returning("created_at")
            out.append(getattr(self.model, c))
        else:
            out.append(c)
    return out

def stmt(self) -> Insert:
    if (self._values is None) == (self._many is None):
        raise ValueError("Provide exactly one of .values(...) or .many([...])")

    if self._values is not None:
        stmt = insert(self.model).values(**self._values)
    else:
        stmt = insert(self.model).values(self._many)

    # Postgres ON CONFLICT (requires PG dialect)
    if self._on_conflict:
        # This method exists on PostgreSQL Insert objects.
        action = self._on_conflict["action"]
        idx = self._on_conflict["index_elements"]

        if action == "nothing":
            stmt = stmt.on_conflict_do_nothing(index_elements=idx)
        elif action == "update":
            stmt = stmt.on_conflict_do_update(
                index_elements=idx,
                set_=self._on_conflict["set_"],
            )
        else:
            raise ValueError(f"Unknown conflict action: {action}")

    if self._returning:
        stmt = stmt.returning(*self._compile_returning())

    return stmt


# Factory for nicer English
def InsertInto(model) -> InsertDSL:
    return InsertDSL(model)

# Domain sugar: InsertInputStatement(...) auto-fills data_type
class InsertInputStatement(InsertDSL):
    def __init__(self, model):
        super().__init__(model)
        # pre-set value for single insert; for bulk you can also auto-merge (see note below)

    def values(self, **kwargs) -> "InsertInputStatement":
        kwargs.setdefault("data_type", "INPUTSTATEMENT")
        return super().values(**kwargs)

    def many(self, rows: Sequence[Dict[str, Any]]) -> "InsertInputStatement":
        fixed = []
        for r in rows:
            rr = dict(r)
            rr.setdefault("data_type", "INPUTSTATEMENT")
            fixed.append(rr)
        return super().many(fixed)

async with AsyncSessionLocal() as session:
    ins = (
        InsertInputStatement(UploadRawData)
        .values(json_data={"settle_no": "51", "amounts": "123.45"})
        .returning(id)
    )

    result = await session.execute(ins.stmt())
    await session.commit()

    new_id = result.scalar_one()
