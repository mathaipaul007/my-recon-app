# qdsl_pg_full.py
# Full working Postgres-first DSL:
# - Q with cached expressions (fixes GROUP BY mismatch / asyncpg grouping errors)
# - JsonNumRef (jnum) for numeric JSON fields
# - NumSqlPred (bounded abs compare)
# - FieldRef (col) and JsonFieldRef (jcol)
# - Agg / sum_ for numeric sums
# - FromInputStatement subclass injects data_type = 'INPUTSTATEMENT'

from __future__ import annotations
from dbmodels.UploadRawData import UploadRawData
from dbmodels.FileUploads import FileUploads
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union, Callable
from sqlalchemy.orm import aliased as sa_aliased
from sqlalchemy import Numeric, and_, case, cast, func, select, union, union_all
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.engine import RowMapping
from backendservice.Comporator import FuzzyMatch
from sqlalchemy.sql.selectable import CTE, Select, Alias, CompoundSelect, CTE, Subquery
#from sqlalchemy.sql import Select
from sqlalchemy import literal

# ================================================================
# Helpers (PostgreSQL)
# ================================================================

PostFilter = Callable[[RowMapping], bool]
CTELike = CTE
OuterBuilder = Callable[[CTE], "Q"]
def pg_json_text(json_col, key: str) -> ColumnElement:
    # json_col ->> 'key'
    return json_col.op("->>")(key)

def pg_safe_numeric(json_col, key: str) -> ColumnElement:
    return cast(func.nullif(func.btrim(json_col.op("->>")(key)), ""), Numeric)

def add_(json_col1, key1: str, amt: str) -> ColumnElement:
    return cast(func.nullif(func.btrim(json_col1.op("->>")(key1)), ""), Numeric) + cast(amt, Numeric)

def case_numeric(expr_text: ColumnElement) -> ColumnElement:
    """
    Safely convert TEXT -> NUMERIC (or NULL), avoiding cast errors.
    - trims spaces
    - removes commas
    - validates numeric via regex
    """
    t = func.btrim(expr_text)
    t_clean = func.replace(t, ",", "")
    is_num = t_clean.op("~")(r"^[+-]?\d+(\.\d+)?$")

    return case(
        (is_num, cast(t_clean, Numeric)),
        else_=None
    )

# Field refs
# ===============================

@dataclass(frozen=True)
class FieldRef:
    name: str

    def is_(self, value: Any) -> "Pred":
        return Pred("eq", self, value)

    def contains(self, text: str) -> "Pred":
        return Pred("contains", self, text)

    def in_(self, values: Sequence[Any]) -> "Pred":
        return Pred("in", self, list(values))

    def label(self, name: str) -> ColumnElement:
        return _LabeledJsonRef(self, name)

    def as_(self, name: str) -> ColumnElement:
        return _LabeledJsonRef(self, name)

    def expr(self, table) -> ColumnElement:
        return getattr(table, self.name)


@dataclass(frozen=True)
class _LabeledJsonRef:
    ref: JsonFieldRef
    label: str

    def expr(self, table) -> ColumnElement:
        return self.ref.expr(table).label(self.label)
    
@dataclass(frozen=True)
class JsonFieldRef:
    """Forced JSON ref: always json_data->>key"""
    key: Optional[str] = None
    path: Optional[Sequence[str]] = None

    def is_(self, value: Any) -> "Pred":
        return Pred("eq", self, value)

    def contains(self, text: str) -> "Pred":
        return Pred("contains", self, text)

    def in_(self, values: Sequence[Any]) -> "Pred":
        return Pred("in", self, list(values))

    def label(self, name: str) -> ColumnElement:
        return LabeledJsonRef(self, name)

    def as_(self, name: str) -> ColumnElement:
        return LabeledJsonRef(self, name)

    def numeric(self) -> NumericJsonRef:
        return NumericJsonRef(self)

    def expr(self, table) -> ColumnElement:
        json_col = getattr(table, "json_data")
        return pg_json_text(json_col, self.key)

    def value(self) -> ColumnElement:
        return getattr(UploadRawData, "json_data").op("->>")(self.key)
    
@dataclass(frozen=True)
class ConstRef:
    value: Any
    _label: Optional[str] = None

    def label(self, name: str) -> "ConstRef":
        return ConstRef(self.value, name)

    def as_(self, name: str) -> "ConstRef":
        return ConstRef(self.value, name)

    def expr(self, table=None) -> ColumnElement:
        e = literal(self.value)
        return e.label(self._label) if self._label else e


@dataclass(frozen=True)
class _NumericJsonRef:
    ref: JsonFieldRef
    _label: Optional[str] = None

    def label(self, name: str) -> "_NumericJsonRef":
        return _NumericJsonRef(self.ref, name)

    def as_(self, name: str) -> "_NumericJsonRef":
        return _NumericJsonRef(self.ref, name)

    def expr(self, table) -> ColumnElement:
        base = self.ref.expr(table)
        num = cast(func.nullif(func.btrim(base), ""), Numeric)
        return num.label(self._label) if self._label else num

    def eq(self, v):
        return
    
def const(value : Any) -> ConstRef:
    return ConstRef(value)

def col(name: str) -> FieldRef:
    return FieldRef(name)

def jcol(key: str) -> JsonFieldRef:
    return JsonFieldRef(key)

def jnum(key: str) -> JsonNumRef:
    return JsonNumRef(key)

def jexpr(json_col : ColumnElement , ref: "JsonFieldRef") -> ColumnElement:
    return json_col.op("->>")(ref.key)

# =====================================================================
# Predicates
# =====================================================================

@dataclass(frozen=True)
class Pred:
    op: str
    left: Union[FieldRef, JsonFieldRef]
    right: Any

    def compile(self, q: "Q") -> ColumnElement:
        left_expr = q._resolve_ref(self.left)

        if self.op == "eq":
            return left_expr == self.right

        if self.op == "contains":
            return left_expr.ilike(f"%{self.right}%")

        if self.op == "in":
            return left_expr.in_(self.right)

        raise ValueError(f"Unsupported predicate op: {self.op}")
    
# =====================================================================
# Numeric JSON ref + Numeric predicate (NumSqlPred)
# =====================================================================

@dataclass(frozen=True)
class JsonNumRef:
    """
    Forced JSON numeric field.
    Compiles to: case_numeric(json_data ->> 'key') -- NUMERIC or NULL
    """
    key: str
    _label: Optional[str] = None

    def label(self, name: str) -> "JsonNumRef":
        return JsonNumRef(self.key, name)

    def as_(self, name: str) -> "JsonNumRef":
        return JsonNumRef(self.key, name)

    def expr(self, q: "Q") -> ColumnElement:
        text_expr = q._resolve_name(self.key, forced_json=True) # TEXT
        #return case_numeric(text_expr)
        return case_numeric(text_expr).label(self._label) if self._label else case_numeric(text_expr)

    # numeric predicates

    def eq(self, value: Any) -> "NumPred":
        return NumPred("eq", self, value)

    def gt(self, value: Any) -> "NumPred":
        return NumPred("gt", self, value)

    def gte(self, value: Any) -> "NumPred":
        return NumPred("gte", self, value)

    def lt(self, value: Any) -> "NumPred":
        return NumPred("lt", self, value)
    
    def lte(self, value: Any) -> "NumPred":
         return NumPred("lte", self, value)

    def between(self, lo: Any, hi: Any) -> "NumPred":
        return NumPred("between", self, (lo, hi))

    # rounded abs compare helper (your use-case)
    def rounded_abs_eq(self, value: Any, decimals: int = 2) -> "NumSqlPred":
        return NumSqlPred(self, value, decimals)

    def value(self) -> ColumnElement:
        return getattr(UploadRawData, "json_data").op("->>")(self.key)

    @dataclass(frozen=True)
    class NumPred:
        """Numeric comparison predicate for JsonNumRef."""
        op: str
        left: JsonNumRef
        right: Any

        def compile(self, q: "Q") -> ColumnElement:
            left_expr = self.left.expr(q) # NUMERIC

            if self.op == "eq":
                return left_expr == self.right

            if self.op == "gt":
                return left_expr > self.right
            if self.op == "gte":
                return left_expr >= self.right
            if self.op == "lt":
                return left_expr < self.right
            if self.op == "lte":
                return left_expr <= self.right
            if self.op == "between":
                lo, hi = self.right
                return left_expr.between(lo, hi)

            raise ValueError(f"Unsupported NumPred op: {self.op}")
        
@dataclass(frozen=True)
class NumSqlPred:
    """
    Predicate:
    | round(abs(jnum(key)), decimals) == round(abs(value), decimals)

    Fixes Postgres round signature by ensuring NUMERIC on both sides.
    """
    left: JsonNumRef
    right_value: Any
    decimals: int = 2

    def compile(self, q: "Q") -> ColumnElement:
        left_num = self.left.expr(q)                 # NUMERIC
        right_num = cast(self.right_value, Numeric)   # NUMERIC

        return (
            func.round(func.abs(left_num), self.decimals)
            ==
            func.round(func.abs(right_num), self.decimals)
        )

@dataclass(frozen=True)
class AggPred:
    op: str
    left: Agg
    right: Any

    def compile(self, q: "Q") -> ColumnElement:
        l = self.left.expr(q)
        r = self.right
        if self.op == "eq":
            return l == r

dataclass(frozen=True)
class Agg:
    fn: str
    target: Union[str, FieldRef, JsonFieldRef, JsonNumRef, ColumnElement]
    label: Optional[str] = None
    cast_numeric: bool = True  # for sum/avg, ensure numeric

    def as_(self, label: str) -> "Agg":
        return Agg(self.fn, self.target, label=label, cast_numeric=self.cast_numeric)

    def eq(self, value) -> AggPred:
        return AggPred("eq", self, value)

    def expr(self, q: "Q") -> ColumnElement:
        # Resolve target to expression
        if isinstance(self.target, str):
            base = q._resolve_name(self.target, forced_json=False)
        elif isinstance(self.target, (FieldRef, JsonFieldRef)):
            base = q._resolve_ref(self.target)
        elif isinstance(self.target, JsonNumRef):
            base = self.target.expr(q)  # already NUMERIC
        else:
            base = self.target

        if self.cast_numeric and not isinstance(self.target, JsonNumRef):
            # JSON text or column -> safe numeric
            base = case_numeric(base)

        if self.fn == "sum":
            e = func.sum(base)
        elif self.fn == "avg":
            e = func.avg(base)
        elif self.fn == "count":
            e = func.count(base)
        else:
            raise ValueError(f"Unsupported aggregation fn: {self.fn}")

        return e.label(self.label) if self.label else e
    
    def sum_(field: Union[str, FieldRef, JsonFieldRef, JsonNumRef, ColumnElement]) -> Agg:
        return Agg("sum", field, cast_numeric=True)

# =====================================================================
# Q base class (with caching)
# =====================================================================

SelectableItem = Union[str, FieldRef, JsonFieldRef, JsonNumRef, Agg, ColumnElement, ConstRef, NumericJsonRef]
PredicateItem = Union[Pred, NumPred, NumSqlPred, ColumnElement]

class AsyncExecuter:

    async def execute(self, session: AsyncSession, *, as_mappings: bool = True, apply_post_filters: bool = True) -> List[Any]:
        """
        Execute using AsyncSession.
        - as_mappings=True returns list[RowMapping] (dict-like rows) -> easiest for post filters
        - as_mappings=False returns list[Row] (tuple-like)
        """
        stmt = self.stmt()

        result = await session.execute(stmt, self._params)

        if as_mappings:
            rows: List[RowMapping] = result.mappings().all()
        else:
            rows = result.all() # list[Row]

        if apply_post_filters and self._post_filters:
            if as_mappings:
                for pf in self._post_filters:
                    rows = [r for r in rows if pf(r)]
            else:
                # If you're not using mappings, post-filtering is harder unless you know positions.
                # Prefer as_mappings=True when using post-filters.
                raise ValueError("Post-filters require as_mappings=True for reliable column access.")

        return rows

class Q(AsyncExecuter):
    """
    Postgres-first Q DSL.
    Assumes base has a JSON/JSONB column called `json_data`.
    """

    def __init__(self, base):
        self.base = base
        self._selects: List[SelectableItem] = []
        self._preds: List[PredicateItem] = []
        self._having: List[Union[str, FieldRef, JsonFieldRef, JsonNumRef, ColumnElement]] = []
        self._group_by: List[Union[str, FieldRef, JsonFieldRef, JsonNumRef, ColumnElement]] = []
        self._limit: Optional[int] = None
        self._offset: Optional[int] = None
        self._joins: List[Tuple[Any, ColumnElement], bool] = []

        # Caches to reuse exact expression objects (critical for GROUP BY)
        self._base_expr_cache: Dict[Tuple[str, bool], ColumnElement] = {}
        self._select_expr_cache: Dict[Tuple[str, bool], ColumnElement] = {}

        self._params: Dict[str, Any] = {}
        self._post_filters: List[PostFilter] = []
        self._ctes: List[CTE] = []

    # --- optional helpers ---
    def params(self, **kwargs) -> "Q":
        self._params.update(kwargs)
        return self

    def post_filter(self, fn: PostFilter) -> "Q":
        self._post_filters.append(fn)
        return self
    
    # ---------------------------------------------------------------------
    # Resolution (cached)
    # ---------------------------------------------------------------------

    def _resolve_name(self, name: str, forced_json: bool) -> ColumnElement:
        key = (name, forced_json)
        if key in self._base_expr_cache:
            return self._base_expr_cache[key]

        if not forced_json and hasattr(self.base, name):
            expr = getattr(self.base, name)
        else:
            json_col = getattr(self.base, "json_data")
            expr = pg_json_text(json_col, name)

        self._base_expr_cache[key] = expr
        return expr

    def _resolve_ref(self, ref: Union[FieldRef, JsonFieldRef]) -> ColumnElement:
        if isinstance(ref, JsonFieldRef):
            return self._resolve_name(ref.key, forced_json=True)
        return self._resolve_name(ref.name, forced_json=False)

    def _resolve_select_name(self, name: str, forced_json: bool) -> ColumnElement:
        key = (name, forced_json)
        if key in self._select_expr_cache:
            return self._select_expr_cache[key]

        base_expr = self._resolve_name(name, forced_json=forced_json)
        labeled = base_expr.label(name)
        self._select_expr_cache[key] = labeled
        return labeled
    def find(self, *items: SelectableItem) -> "Q":
        self._selects.extend(items)
        return self

    def where(self, predicate: PredicateItem) -> "Q":
        self._preds.append(predicate)
        return self

    def and_(self, predicate: PredicateItem) -> "Q":
        return self.where(predicate)

    def grouped_by(self, *fields: Union[str, FieldRef, JsonFieldRef, JsonNumRef, ColumnElement]) -> "Q":
        self._group_by.extend(fields)
        return self

    def having(self, *fields: Union[str, FieldRef, JsonFieldRef, JsonNumRef, ColumnElement]) -> "Q":
        self._having.extend(fields)
        return self

    def limit(self, n: int) -> "Q":
        self._limit = int(n)
        return self

    def offset(self, n: int) -> "Q":
        self._offset = int(n)
        return self

    def join(self, right, *, on: ColumnElement) -> "Q":
        self._joins.append((right, on, False))
        return self

    def as_subquery(self, name: str = "subq"):
        """Turn this Query into a subquery (derived table)."""
        return self.stmt().subquery(name)
    def as_scalar_subquery(self) -> ScalarSelect:
        """
        Turn this Query into a scalar subquery usable in IN/NOT IN.
        Requires exactly ONE selected column.
        """
        if len(self._selects) != 1:
            raise ValueError(
                "as_scalar_subquery() requires exactly one selected column in find(...). "
                f"Got {len(self._selects)}."
            )
        return self.stmt().scalar_subquery()

    # ------------------ compilation ------------------

    def _compile_select_item(self, item: SelectableItem) -> ColumnElement:
        if isinstance(item, Agg):
            return item.expr(self)

        if isinstance(item, str):
            return self._resolve_select_name(item, forced_json=False)

        if isinstance(item, JsonFieldRef):
            return self._resolve_select_name(item.key, forced_json=True)

        if isinstance(item, FieldRef):
            return self._resolve_select_name(item.name, forced_json=False)

        if isinstance(item, JsonNumRef):
            # numeric expr; label with key
            return item.expr(self)

        if isinstance(item, _LabeledJsonRef):
            return item.expr(self.base)

        if isinstance(item, ConstRef):
            return item.expr(self.base)

        if isinstance(item, NumericJsonRef):
            return item.expr(self.base)

        return item  # raw SQLAlchemy expr
    def _compile_group_item(self, item) -> ColumnElement:
        if isinstance(item, str):
            return self._resolve_name(item, forced_json=False)

        if isinstance(item, JsonFieldRef):
            return self._resolve_name(item.key, forced_json=True)

        if isinstance(item, FieldRef):
            return self._resolve_name(item.name, forced_json=False)

        if isinstance(item, JsonNumRef):
            return item.expr(self)

        if isinstance(item, _NumericJsonRef):
            return item.expr(self.base)

        return item

    def _compile_having_item(self, item) -> ColumnElement:
        if isinstance(item, str):
            return self._resolve_name(item, forced_json=False)

        if isinstance(item, JsonFieldRef):
            return self._resolve_name(item.key, forced_json=True)

        if isinstance(item, FieldRef):
            return self._resolve_name(item.name, forced_json=False)

        if isinstance(item, JsonNumRef):
            # group by the underlying NUMERIC expression (rare, but valid)
            return item.expr(self)

        if isinstance(item, AggPred):
            return item.compile(self)

        return item
    
    def _compile_pred(self, p:PredicateItem) -> ColumnElement:
        if isinstance(p, NumSqlPred):
                    return p.compile(self)
        if isinstance(p, NumPred):
                    return p.compile(self)
        if isinstance(p, Pred):
                    return p.compile(self)
        return p  # assume it's already a SQLAlchemy boolean expression

    def _apply_joins(self, from_obj):
        for right, on_expr, isouter in self._joins:
            from_obj = sa_join(from_obj, right, on_expr, isouter=isouter)
        return from_obj

    def stmt(self):
        if not self._selects:
            # Prevent accidental select(self.base) which pulls json_data and breaks GROUP BY
            raise ValueError("No projection specified. Use .find(...)")
        
        sel = [self._compile_select_item(i) for i in self._selects]
        from_obj = self._apply_joins(self.base)
        s = select(*sel).select_from(from_obj)
        
        if self._preds:
            s = s.where(and_(*[self._compile_pred(p) for p in self._preds]))
        if self._group_by:
            s = s.group_by(*[self._compile_group_item(g) for g in self._group_by])
        if self._having:
            s = s.having(*[self._compile_having_item(g) for g in self._having])
        if self._limit is not None:
            s = s.limit(self._limit)
        if self._offset is not None:
            s = s.offset(self._offset)
        for cte in self._ctes:
            s = s.add_cte(cte)
            
        return s
    
    def with_(self, *ctes: CTELike) -> "Query":
        """
        Attach CTE(s) to the final statement as WITH clauses.
        """
        for c in ctes:
            if not isinstance(c, CTE):
                raise TypeError(f"with_() expects SQLAlchemy CTE, got {type(c)}")
            self._ctes.append(c)
        return self

    @staticmethod
    def with_cte(name: str, cte_query: "Query", outer_builder: OuterBuilder) -> "Query":
        """
        Build: WITH <name> AS (<cte_query>) <outer_query>
        - name: CTE name
        - cte_query: Query object whose statement becomes the CTE
        - outer_builder: lambda cte: Query(...) that builds outer query using the CTE
        """
        cte_stmt = cte_query.stmt()
        cte = cte_stmt.cte(name)

        outer_q = outer_builder(cte)

        if getattr(cte_query, "_params", None):
            outer_q.params(**cte_query._params)

        return outer_q.with_(cte)

    def union(self, *others: "Q") -> "UnionQ":
        return UnionQ([self, *others], union_all_mode=False)

    def union_all(self, *others: "Q") -> "UnionQ":
        return UnionQ([self, *others], union_all_mode=True)

class UnionQ(AsyncExecuter):
    def __init__(self, queries: List[Q], union_all_mode: bool = False):
        if len(queries) < 2:
            raise ValueError("Union requires at least 2 queries")

        self._queries = queries
        self._union_all_mode = union_all_mode
        self._limit: Optional[int] = None
        self._offset: Optional[int] = None
        self._params: Dict[str, Any] = {}
        self._group_by: List[Union[str, FieldRef, JsonFieldRef, JsonNumRef, ColumnElement]] = []
        self._post_filters: List[PostFilter] = []
        self._preds: List[PredicateItem] = []
        self._validate_union_queries()
        self._having: List[Union[str, FieldRef, JsonFieldRef, JsonNumRef, ColumnElement]] = []
        self._ctes: List[CTE] = []

    def _validate_union_queries(self) -> None:
        first_count = len(self._queries[0]._selects)
        if first_count == 0:
            raise ValueError("Union queries must have projections. Use .find(...)")

        for i, q in enumerate(self._queries[1:], start=2):
            if len(q._selects) != first_count:
                raise ValueError(
                    f"Union query #{i} has {len(q._selects)} select columns; expected {first_count}"
                )

    def union(self, *others: Q) -> "UnionQ":
        return UnionQ(self._queries + list(others), union_all_mode=False)

    def union_all(self, *others: Q) -> "UnionQ":
        return UnionQ(self._queries + list(others), union_all_mode=True)

    def limit(self, n: int) -> "UnionQ":
        self._limit = int(n)
        return self
    def offset(self, n: int) -> "UnionQ":
        self._offset = int(n)
        return self

    def stmt(self):
        stmts = [q.stmt() for q in self._queries]
        s = union_all(*stmts) if self._union_all_mode else union(*stmts)

        # if self._limit is not None:
        #     compound = compound.limit(self._limit)
        # if self._offset is not None:
        #     compound = compound.offset(self._offset)

        if self._preds:
            s = s.where(and_(*[self._compile_pred(p) for p in self._preds]))

        if self._group_by:
            s = s.group_by(*[self._compile_group_item(g) for g in self._group_by])

        if self._having:
            s = s.having(*[self._compile_having_item(g) for g in self._having])

        if self._limit is not None:
            s = s.limit(self._limit)

        if self._offset is not None:
            s = s.offset(self._offset)

        for cte in self._ctes:
            s = s.add_cte(cte)

        return s
    def stmty(self):
        if not self._selects:
            # Prevent accidental select(self.base) which pulls json_data and breaks GROUP BY
            raise ValueError("No projection specified. Use .find(...)")

        sel = [self._compile_select_item(i) for i in self._selects]
        from_obj = self._apply_joins(self.base)
        s = select(*sel).select_from(from_obj)

        if self._preds:
            s = s.where(and_(*[self._compile_pred(p) for p in self._preds]))

        if self._group_by:
            s = s.group_by(*[self._compile_group_item(g) for g in self._group_by])

        if self._having:
            s = s.having(*[self._compile_having_item(g) for g in self._having])

        if self._limit is not None:
            s = s.limit(self._limit)

        if self._offset is not None:
            s = s.offset(self._offset)

        for cte in self._ctes:
            s = s.add_cte(cte)

        return s

def alias(tbl, nm: str):
    return sa_aliased(tbl, nm)

def with_aliases(model, *name_and_builder):
    *names, builder = name_and_builder
    aliases = [sa_aliased(model, name=n) for n in names]
    return builder(*aliases)

class FromBMOFundingFile(Q):
    def __init__(self):
        super().__init__(UploadRawData)
        self._preds.append(col("data_type").is_("BMOFUNDING"))

class FromNBFundingFile(Q):
    def __init__(self):
        super().__init__(UploadRawData)
        self._preds.append(col("data_type").is_("NBFUNDING"))

class FromLockBoxFile(Q):
    def __init__(self):
        super().__init__(UploadRawData)
        self._preds.append(col("data_type").is_("LOCKBOX"))

class FromInputStatement(Q):
    def __init__(self):
        super().__init__(UploadRawData)
        self._preds.append(col("data_type").is_("INPUTRECON"))

class FromNetPayRegistry(Q):
    def __init__(self):
        super().__init__(UploadRawData)
        self._preds.append(col("data_type").is_("NETPAYREGISTRY"))

class FromReversalNetPayRegistry(Q):
    def __init__(self):
        super().__init__(UploadRawData)
        self._preds.append(col("data_type").is_("NETPAYREGISTRY-RS"))

class FromSTPV(Q):
    def __init__(self):
        super().__init__(UploadRawData)
        self._preds.append(col("data_type").is_("STPV"))
class FromLockBoxUploadFiles(Q):
    def __init__(self):
        super().__init__(FileUploads)
        self._preds.append(col("recon_file_type").is_("LOCKBOX"))

class FromAchRejectTracking(Q):
    def __init__(self):
        super().__init__(UploadRawData)
        self._preds.append(col("data_type").is_("ACH_REJECT_TRACKING"))

class FromHanaBreakUp(Q):
    def __init__(self):
        super().__init__(UploadRawData)
        self._preds.append(col("data_type").is_("HANA_MASTER_BREAK_UP"))

class FromDeductionGL(Q):
    def __init__(self):
        super().__init__(UploadRawData)
        self._preds.append(col("data_type").is_("DEDUCTION_GL"))

def fuzzy_match(field: str, value: str, threshold: float = 0.70):
    matcher = FuzzyMatch(threshold)
    def _pf(row):
        # row is RowMapping: dict-like access by selected labels/column keys
        return matcher.compare(row.get(field), value)
    return _pf