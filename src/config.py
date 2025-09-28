from pydantic import BaseModel, Field
from typing import Optional


class StandardizeConfig(BaseModel):
    winsor_lower: float = Field(0.01, ge=0.0, le=0.5)
    winsor_upper: float = Field(0.99, ge=0.5, le=1.0)
    rolling_window: int = Field(12, ge=1)
    rolling_min_periods: int = Field(3, ge=1)
    rolling_mad: bool = Field(
        False, description="If true, use rolling MAD as scale for robust_zscore"
    )


DEFAULT_STD_CONFIG = StandardizeConfig(
    winsor_lower=0.01,
    winsor_upper=0.99,
    rolling_window=12,
    rolling_min_periods=3,
    rolling_mad=False,
)
import yaml  # type: ignore[import]
from datetime import date
from typing import Any, Dict, List
from pydantic import BaseModel, Field, ValidationError, validator, root_validator


# Compatibility helper: safely convert Pydantic models to plain dicts.
# Only runs model_dump()/dict() when the object is a BaseModel instance;
# otherwise returns the object unchanged. This avoids accidental dict()
# conversions of primitives (strings) and keeps behavior stable across
# pydantic v1 and v2.
def model_as_dict(obj):
    if isinstance(obj, BaseModel):
        # pydantic v2
        if hasattr(obj, "model_dump") and callable(getattr(obj, "model_dump")):
            try:
                return obj.model_dump()
            except Exception:
                pass
        # pydantic v1
        if hasattr(obj, "dict") and callable(getattr(obj, "dict")):
            try:
                return obj.dict()
            except Exception:
                pass
    return obj


class SourceEntry(BaseModel):
    source: str
    code: str


class IndicatorConfig(BaseModel):
    id: str
    sources: List[SourceEntry]
    good_direction: str = "up"
    transform: str = "none"


class ScoringConfig(BaseModel):
    standardization: str = "zscore"
    smoothing: int = 0
    weights: Dict[str, float]
    min_coverage_ratio: float = 0.6


class ExcelConfig(BaseModel):
    path: str = "./output/macro_ranking.xlsx"
    sheet_prefix: str = "Makro"
    number_format: str = "#.##0,00"
    date_format: str = "DD.MM.YYYY"


class CachingConfig(BaseModel):
    enabled: bool = True
    ttl_hours: int = 24


class RuntimeConfig(BaseModel):
    max_workers: int = 4
    request_timeout_sec: int = 20
    retry_max: int = 3
    backoff_initial_sec: float = 1.0


class BacktestConfig(BaseModel):
    # If true, do not backfill when running backtests (point-in-time enforcement)
    no_backfill: bool = False


class AllocationConfig(BaseModel):
    min_alloc: float = 0.0
    max_alloc: float = 1.0
    top_n: Optional[int] = None

    @validator("min_alloc", "max_alloc")
    def check_alloc_range(cls, v):
        if not (0.0 <= v <= 1.0):
            raise ValueError("allocation values must be between 0 and 1")
        return v

    @validator("top_n")
    def check_top_n(cls, v):
        if v is None:
            return v
        if isinstance(v, int) and v >= 1:
            return v
        raise ValueError("top_n must be an integer >= 1")

    @root_validator(skip_on_failure=True)
    def check_min_le_max(cls, values):
        min_a = values.get("min_alloc")
        max_a = values.get("max_alloc")
        if min_a is not None and max_a is not None:
            if min_a > max_a:
                raise ValueError("min_alloc must be <= max_alloc")
        return values


class ConfigModel(BaseModel):
    countries: List[str]
    period: Dict[str, Any]
    indicators: List[IndicatorConfig]
    scoring: ScoringConfig
    excel: ExcelConfig
    caching: CachingConfig
    runtime: RuntimeConfig
    allocation: AllocationConfig = Field(default_factory=AllocationConfig)
    backtest: BacktestConfig = Field(default_factory=BacktestConfig)

    @validator("period")
    def validate_period(cls, v):
        if "start" not in v or "end" not in v or "frequency" not in v:
            raise ValueError("period must contain start, end and frequency")
        return v


DEFAULT_CONFIG = {
    "countries": ["DEU", "FRA", "ITA", "ESP", "NLD", "SWE", "USA", "GBR", "JPN", "CHN"],
    "period": {
        "start": "2015-01-01",
        "end": date.today().isoformat(),
        "frequency": "Q",
    },
    "indicators": [
        {
            "id": "gdp_real_yoy",
            "sources": [{"source": "WB", "code": "NY.GDP.MKTP.KD.ZG"}],
            "good_direction": "up",
            "transform": "none",
        },
        {
            "id": "inflation",
            "sources": [{"source": "WB", "code": "FP.CPI.TOTL.ZG"}],
            "good_direction": "down",
            "transform": "none",
        },
        {
            "id": "unemployment",
            "sources": [{"source": "WB", "code": "SL.UEM.TOTL.ZS"}],
            "good_direction": "down",
            "transform": "none",
        },
        {
            "id": "debt_gdp",
            "sources": [{"source": "WB", "code": "GC.DOD.TOTL.GD.ZS"}],
            "good_direction": "down",
            "transform": "none",
        },
        {
            "id": "current_account_gdp",
            "sources": [{"source": "WB", "code": "BN.CAB.XOKA.GD.ZS"}],
            "good_direction": "up",
            "transform": "none",
        },
    ],
    "scoring": {
        "standardization": "zscore",
        "smoothing": 0,
        "weights": {
            "gdp_real_yoy": 0.35,
            "inflation": 0.25,
            "unemployment": 0.2,
            "debt_gdp": 0.1,
            "current_account_gdp": 0.1,
        },
        "min_coverage_ratio": 0.6,
    },
    "excel": {
        "path": "./output/macro_ranking.xlsx",
        "sheet_prefix": "Makro",
        "number_format": "#.##0,00",
        "date_format": "DD.MM.YYYY",
    },
    "caching": {"enabled": True, "ttl_hours": 24},
    "runtime": {
        "max_workers": 4,
        "request_timeout_sec": 20,
        "retry_max": 3,
        "backoff_initial_sec": 1.0,
    },
    "allocation": {"min_alloc": 0.0, "max_alloc": 1.0, "top_n": None},
    "backtest": {"no_backfill": False},
}


def load_config(path: Optional[str] = None) -> ConfigModel:
    if path:
        with open(path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    else:
        cfg = DEFAULT_CONFIG
    try:
        model = ConfigModel(**cfg)
    except ValidationError as e:
        print("Config validation error:")
        print(e.json())
        raise
    return model
