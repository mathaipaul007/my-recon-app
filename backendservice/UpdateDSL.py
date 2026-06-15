from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Union

from sqlalchemy import update
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.dml import Update


Selectable = Union[str, ColumnElement]
Predicate = ColumnElement # keep it simple: real SQLAlchemy boolean expressions


class UpdateDSL:
    def __init__(self, model):
        self.model = model
        self._sets: Dict[Any, Any] = {} # keys can be model attrs or string col
        self._preds: List[Predicate] = []
        self._returning: List[Selectable] = []

    # -----------------------------------
    # SET
    # -----------------------------------

    def set(self, **kwargs) -> "UpdateDSL":
        """
        .set(status="DONE", data_type="X")
        """
        for k, v in kwargs.items():
            col_expr = getattr(self.model, k) if isinstance(k, str) else k
            self._sets[col_expr] = v
        return self

    def set_expr(self, **expr_map) -> "UpdateDSL":
        """
        Like set(), but values can be SQL expressions:
        .set_expr(updated_at=func.now())
        """
        for k, v in expr_map.items():
            col_expr = getattr(self.model, k) if isinstance(k, str) else k
            self._sets[col_expr] = v
        return self

    # -----------------------------------
    # WHERE
    # -----------------------------------

    def where(self, predicate: Predicate) -> "UpdateDSL":
        self._preds.append(predicate)
        return self

    def and_(self, predicate: Predicate) -> "UpdateDSL":
        return self.where(predicate)

    # -----------------------------------
    # RETURNING
    # -----------------------------------

    def returning(self, *cols: Selectable) -> "UpdateDSL":
        self._returning.extend(cols)
        return self

    def _compile_returning(self) -> List[ColumnElement]:
        out: List[ColumnElement] = []
        for c in self._returning:
            if isinstance(c, str):
                out.append(getattr(self.model, c))
            else:
                out.append(c)
        return out

    # -----------------------------------
    # POSTGRES JSONB convenience
    # -----------------------------------

    def set_json_key(
        self,
        json_column_name: str,
        key: str,
        value: Any,
        *,
        create_missing: bool = True,
    ) -> "UpdateDSL":
        """
        Update a single top-level JSONB key using jsonb_set:
        .set_json_key("json_data", "status", "DONE")

        Requires json_column_name to be a JSONB column on the model.
        """
        from sqlalchemy import func, literal, cast
        from sqlalchemy.dialects.postgresql import JSONB

        json_col = getattr(self.model, json_column_name)

        # jsonb_set(target, '{key}', to_jsonb(value), create_missing)
        # to_jsonb(value) works well for scalars/strings/numbers
        new_json = func.jsonb_set(
            cast(json_col, JSONB),
            literal([key]), # path array
            func.to_jsonb(value),
            create_missing,
        )

        self._sets[json_col] = new_json
        return self

    # -----------------------------------
    # BUILD
    # -----------------------------------

    def stmt(self) -> Update:
        if not self._sets:
            raise ValueError("No SET values provided. Use .set(...) or .set_expr(...).")

        stmt = update(self.model).values(self._sets)

        # WHERE predicates
        for p in self._preds:
            stmt = stmt.where(p)

        # RETURNING
        if self._returning:
            stmt = stmt.returning(*self._compile_returning())

        return stmt


def Update(model) -> UpdateDSL:
    return UpdateDSL(model)