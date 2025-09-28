import pytest
from src.config import ConfigModel, DEFAULT_CONFIG
from pydantic import ValidationError


def make_cfg(overrides: dict):
    cfg = DEFAULT_CONFIG.copy()
    cfg.update(overrides)
    return cfg


def test_allocation_valid_defaults():
    cfg = ConfigModel(**DEFAULT_CONFIG)
    assert cfg.allocation.min_alloc == 0.0
    assert cfg.allocation.max_alloc == 1.0
    assert cfg.allocation.top_n is None


def test_allocation_invalid_min_gt_max():
    bad = DEFAULT_CONFIG.copy()
    bad["allocation"] = {"min_alloc": 0.6, "max_alloc": 0.5, "top_n": None}
    with pytest.raises(ValidationError):
        ConfigModel(**bad)


def test_allocation_invalid_out_of_bounds():
    bad = DEFAULT_CONFIG.copy()
    bad["allocation"] = {"min_alloc": -0.1, "max_alloc": 1.2, "top_n": None}
    with pytest.raises(ValidationError):
        ConfigModel(**bad)


def test_allocation_invalid_top_n():
    bad = DEFAULT_CONFIG.copy()
    bad["allocation"] = {"min_alloc": 0.0, "max_alloc": 1.0, "top_n": 0}
    with pytest.raises(ValidationError):
        ConfigModel(**bad)


def test_allocation_valid_custom():
    good = DEFAULT_CONFIG.copy()
    good["allocation"] = {"min_alloc": 0.01, "max_alloc": 0.2, "top_n": 3}
    cfg = ConfigModel(**good)
    assert cfg.allocation.min_alloc == 0.01
    assert cfg.allocation.max_alloc == 0.2
    assert cfg.allocation.top_n == 3
