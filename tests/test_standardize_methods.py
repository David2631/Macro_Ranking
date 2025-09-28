import pandas as pd
from src.transforms.pipeline import apply_standardization


def make_df(values):
    return pd.DataFrame({
        'indicator': ['t1'] * len(values),
        'country': ['AAA'] * len(values),
        'date': pd.date_range('2020-01-01', periods=len(values), freq='ME'),
        'value': values,
    })


def test_winsorized_and_robust_zscore_differ():
    df = make_df([1, 2, 3, 1000, 5, 6, 7])

    out_robust = apply_standardization(df.copy(), config={'winsor_lower': 0.01, 'winsor_upper': 0.99, 'rolling_window': 3, 'rolling_min_periods': 1}, method='robust_zscore')
    out_wins = apply_standardization(df.copy(), config={'winsor_lower': 0.10, 'winsor_upper': 0.90, 'rolling_window': 3, 'rolling_min_periods': 1}, method='winsorized_zscore')

    a = out_robust['std_value']
    b = out_wins['std_value']

    # With stronger winsorization, at least one element should differ
    assert not a.equals(b)


def test_auto_sign_check_flips_when_needed():
    # construct a dev series where higher raw value is bad but good_direction='up'
    df = make_df([10, 9, 8, 7, 6, 5, 4])
    # here, higher raw value indicates worse outcome, so if good_direction='up' we expect flip
    out = apply_standardization(df.copy(), config={'winsor_lower': 0.01, 'winsor_upper': 0.99, 'rolling_window': 3, 'rolling_min_periods': 1}, method='robust_zscore', good_direction='up', auto_sign_check=True)

    # compute spearman between dev and std — should be positive when good_direction matches, else flipped
    s = out.set_index('date')
    dev = s['value'] - s['value'].rolling(window=3, min_periods=1).median()
    corr = dev.corr(s['std_value'], method='spearman')

    # since we forced good_direction='up' but actual dev is negatively sloped, we expect corr>0 after auto-flip
    assert corr is not None
    assert corr > 0
