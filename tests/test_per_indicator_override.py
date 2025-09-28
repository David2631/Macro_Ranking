import pandas as pd
from src.transforms.pipeline import apply_standardization


def make_df(values):
    return pd.DataFrame({
        'indicator': ['test_ind'] * len(values),
        'country': ['AAA'] * len(values),
    'date': pd.date_range('2020-01-01', periods=len(values), freq='ME'),
        'value': values,
    })


def test_per_indicator_override_changes_output():
    # create a series with an outlier
    df = make_df([1, 2, 3, 1000, 5, 6, 7])

    global_cfg = {
        'winsor_lower': 0.01,
        'winsor_upper': 0.99,
        'rolling_window': 3,
        'rolling_min_periods': 1,
    }

    # per-indicator override with tight winsor bounds that will cap the outlier
    ind_override = {
        'winsor_lower': 0.10,
        'winsor_upper': 0.90,
        'rolling_window': 3,
        'rolling_min_periods': 1,
    }

    out_global = apply_standardization(df.copy(), config=global_cfg, method='robust_zscore')
    out_override = apply_standardization(df.copy(), config=ind_override, method='robust_zscore')

    # both should have produced a standardized column (std_value or value_std)
    if 'std_value' in out_global.columns:
        a = out_global['std_value']
    elif 'value_std' in out_global.columns:
        a = out_global['value_std']
    else:
        raise AssertionError('no standardized column in global output')

    if 'std_value' in out_override.columns:
        b = out_override['std_value']
    elif 'value_std' in out_override.columns:
        b = out_override['value_std']
    else:
        raise AssertionError('no standardized column in override output')

    # The presence of the large outlier should make the global result different
    # from the override where the outlier is winsorized more aggressively.
    assert not a.equals(b)
