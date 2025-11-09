from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List

QueryResult = List[Dict[str, Any]]


class QueryReturnType(Enum):
    SCALAR = "scalar"
    ONE = "one"
    ALL = "all"
    NONE = None


@dataclass(frozen=True)
class QueryType:
    name: str
    sql: str
    return_type: QueryReturnType
