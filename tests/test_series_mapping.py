from src.fetchers.mapping import load_series_mapping, lookup_indicator
import os


def test_load_and_lookup(tmp_path, monkeypatch):
    # copy example mapping into tmpdir
    src = os.path.join(os.getcwd(), "data", "series_mapping.csv")
    dst = tmp_path / "series_mapping.csv"
    with open(src, "r", encoding="utf-8") as fsrc:
        with open(dst, "w", encoding="utf-8") as fdst:
            fdst.write(fsrc.read())

    mapping = load_series_mapping(str(dst))
    assert mapping
    ind = lookup_indicator(mapping, 'IMF', 'IFS', 'ABC.GDP.1')
    assert ind == 'GDP'
    ind2 = lookup_indicator(mapping, 'OECD', 'MEI', 'XYZ.IND1')
    assert ind2 == 'IND1'
