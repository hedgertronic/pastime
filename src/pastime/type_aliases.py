from datetime import date
from typing import Sequence


ParamComponent = str | int | float | date | None
Param = ParamComponent | Sequence[ParamComponent]
