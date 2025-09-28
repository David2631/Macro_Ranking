import argparse
import logging
from .config import load_config

# WorldBankFetcher imported later when needed to avoid top-level network deps
from .processing.harmonize import harmonize_countries, parse_dates
from .processing.features import apply_transform, smooth, standardize
from .transforms.pipeline import apply_standardization
from .processing.scoring import (
    compute_coverage,
    compute_composite,
    rank_scores,
)
from .io.cache import cache_get, cache_set
from .io.excel import export_to_excel
from .io.artifacts import write_manifest, _enrich_fetch_entry
from .portfolio.allocations import score_to_weights, write_allocations
import pandas as pd


def setup_logging(debug: bool = False):
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")


def main(cli_args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", "-c", default=None)
    parser.add_argument(
        "--countries",
        "-C",
        default=None,
        help="Optional comma-separated list of ISO3 country codes to override config",
    )
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args(cli_args)
    setup_logging(args.debug)
    cfg = load_config(args.config)
    # allow CLI override of countries (comma-separated ISO3 codes)
    if args.countries:
        # normalize and set on the Pydantic model
        clist = [c.strip() for c in args.countries.split(",") if c.strip()]
        if clist:
            try:
                cfg.countries = clist
                logging.getLogger(__name__).info(
                    f"Overriding countries from CLI: {cfg.countries}"
                )
            except Exception:
                # if cfg is a dict-like, set directly
                try:
                    cfg["countries"] = clist
                    logging.getLogger(__name__).info(
                        f'Overriding countries from CLI: {cfg["countries"]}'
                    )
                except Exception:
                    pass
    logging.getLogger(__name__).info("Starting fetch...")
    # Build fetcher registry and dispatch per source
    from .fetchers.worldbank import WorldBankFetcher
    from .fetchers.imf import IMFFetcher
    from .fetchers.oecd import OECDFetcher
    from .fetchers.ecb import ECBFetcher

    fetcher_map = {
        "WB": WorldBankFetcher,
        "IMF": IMFFetcher,
        "OECD": OECDFetcher,
        "ECB": ECBFetcher,
    }

    all_rows = []
    fetch_summary = {}
    fetch_entries = []
    # prepare concurrent fetch tasks
    from concurrent.futures import ThreadPoolExecutor, as_completed

    fetch_tasks = []
    executor = ThreadPoolExecutor(
        max_workers=cfg.runtime.max_workers if getattr(cfg, "runtime", None) else 4
    )
    # Iterate per indicator and per declared source for that indicator using Indicator plugins
    from .indicators.wb_indicator import WBIndicator
    from .indicators.imf_indicator import IMFIndicator

    for ind in cfg.indicators:
        ind_id = ind.id
        sources = ind.sources or []
        for src_entry in sources:
            if isinstance(src_entry, dict):
                src = src_entry.get("source")
                code = src_entry.get("code")
            else:
                src = getattr(src_entry, "source", None)
                code = getattr(src_entry, "code", None)

            # Plugin support for WB and IMF; otherwise fall back to a TempPlugin
            if src == "WB":
                plugin = WBIndicator(
                    cfg.dict() if hasattr(cfg, "dict") else dict(cfg), ind_id, code
                )
            elif src == "IMF":
                plugin = IMFIndicator(
                    cfg.dict() if hasattr(cfg, "dict") else dict(cfg), ind_id, code
                )
            else:
                logging.warning(
                    f"No plugin for source {src} (indicator {ind_id}), falling back to fetcher map"
                )
                FetcherCls = fetcher_map.get(src)
                if not FetcherCls:
                    logging.warning(
                        f"No fetcher for source {src} (indicator {ind_id}), skipping"
                    )
                    continue
                fetcher = FetcherCls(
                    cfg.runtime.dict() if hasattr(cfg.runtime, "dict") else cfg.runtime
                )

                # Wrap fetcher in a minimal plugin-like object
                class _TempPlugin:
                    def __init__(self, fetcher, ind_id, code):
                        self.fetcher = fetcher
                        self.id = ind_id
                        self.code = code

                    def fetch(self, countries, start, end, freq):
                        return self.fetcher.fetch(
                            countries,
                            [{"id": self.id, "code": self.code}],
                            start,
                            end,
                            freq,
                        )

                    def transform(self, df):
                        return df

                    def standardize(self, df):
                        df2 = df.copy()
                        mu = df2["value"].mean()
                        sigma = df2["value"].std(ddof=0)
                        df2["value_std"] = (df2["value"] - mu) / (
                            sigma if sigma != 0 else 1.0
                        )
                        return df2

                plugin = _TempPlugin(fetcher, ind_id, code)

            cache_key = f"{src.lower()}_{ind_id}_{'_'.join(cfg.countries)}"
            data_src = None
            # Try to load from cache
            if getattr(cfg, "caching", None) and cfg.caching.enabled:
                cached = cache_get(cache_key, cfg.caching.ttl_hours)
                if cached:
                    # cached may be {'records': [...], 'fetch_logs': [...]}
                    records = (
                        cached.get("records") if isinstance(cached, dict) else cached
                    )
                    data_src = pd.DataFrame(records)
                    # attach cached fetch logs into manifest fetch_entries
                    if isinstance(cached, dict) and cached.get("fetch_logs"):
                        # ensure cached logs are enriched to canonical schema
                        enriched = []
                        for f in cached.get("fetch_logs"):
                            try:
                                enriched.append(_enrich_fetch_entry(f))
                            except Exception:
                                enriched.append(f)
                        fetch_entries.extend(enriched)

            # If not cached, schedule fetch concurrently
            if data_src is None:

                def _fetch_task(
                    plugin=plugin, cache_key=cache_key, ind_id=ind_id, src=src
                ):
                    try:
                        res = plugin.fetch(
                            cfg.countries,
                            cfg.period["start"][:10],
                            cfg.period["end"][:10],
                            cfg.period["frequency"],
                        )
                        # normalize plugin return values; plugin.fetch may return:
                        # - (df, logs)
                        # - df
                        # - None
                        if res is None:
                            df_src = pd.DataFrame(columns=["source","indicator","country","date","value"])
                            logs = []
                        elif isinstance(res, tuple) and len(res) >= 2:
                            df_src, logs = res[0], res[1] or []
                        else:
                            df_src, logs = res, []
                        # defensive coercion to DataFrame
                        if not isinstance(df_src, pd.DataFrame):
                            try:
                                df_src = pd.DataFrame(df_src)
                            except Exception:
                                df_src = pd.DataFrame(columns=["source","indicator","country","date","value"])
                        # Enrich any fetch logs immediately so cached logs are canonical
                        enriched_logs = []
                        for f in (logs or []):
                            try:
                                enriched_logs.append(_enrich_fetch_entry(f))
                            except Exception:
                                enriched_logs.append(f)
                        logs = enriched_logs
                        # write cache (cache_set is thread-safe)
                        try:
                            if logs:
                                cache_set(
                                    cache_key,
                                    {
                                        "records": df_src.to_dict(orient="records"),
                                        "fetch_logs": logs,
                                    },
                                    cfg.caching.ttl_hours,
                                )
                            else:
                                cache_set(
                                    cache_key,
                                    df_src.to_dict(orient="records"),
                                    cfg.caching.ttl_hours,
                                )
                        except Exception:
                            pass
                        return (src, ind_id, df_src, logs)
                    except Exception as e:
                        logging.getLogger(__name__).warning(
                            f"Fetch task failed for {cache_key}: {e}"
                        )
                        return (
                            src,
                            ind_id,
                            pd.DataFrame(
                                columns=[
                                    "source",
                                    "indicator",
                                    "country",
                                    "date",
                                    "value",
                                ]
                            ),
                            [],
                        )

                future = executor.submit(_fetch_task)
                fetch_tasks.append(future)
            else:
                if data_src is not None and not data_src.empty:
                    # tag with canonical indicator id
                    data_src["indicator"] = ind_id
                    # record summary per source
                    fetch_summary.setdefault(src, 0)
                    fetch_summary[src] += len(data_src)
                    all_rows.append(data_src)

    # collect concurrent fetch results
    for fut in as_completed(fetch_tasks):
        try:
            src, ind_id, df_src, logs = fut.result()
            if logs:
                # logs should already be enriched, but ensure canonical shape
                enriched = []
                for f in logs:
                    try:
                        enriched.append(_enrich_fetch_entry(f))
                    except Exception:
                        enriched.append(f)
                fetch_entries.extend(enriched)
            if df_src is not None and not df_src.empty:
                df_src["indicator"] = ind_id
                fetch_summary.setdefault(src, 0)
                fetch_summary[src] += len(df_src)
                all_rows.append(df_src)
        except Exception:
            continue

    if not all_rows:
        logging.warning("No data fetched from any source for any indicator.")
        data = pd.DataFrame(columns=["source", "indicator", "country", "date", "value"])
    else:
        data = pd.concat(all_rows, ignore_index=True)
    # Write artifacts manifest
    try:
        manifest = {
            "timestamp": pd.Timestamp.utcnow().isoformat(),
            "fetch_summary": fetch_summary,
            "fetches": fetch_entries,
            # compute per-series as_of mapping from fetch_entries
            "series_as_of": {},
            "n_rows": len(data),
            "config_snapshot": cfg.dict() if hasattr(cfg, "dict") else dict(cfg),
        }
        try:
            series_map = {}
            for f in fetch_entries:
                ind = f.get("indicator")
                c = f.get("country")
                ts = f.get("fetch_timestamp")
                if not ind or not c or not ts:
                    continue
                key = (ind, c)
                # keep most recent
                if key not in series_map or ts > series_map[key]:
                    series_map[key] = ts
            # convert tuple keys to string keys for JSON
            manifest["series_as_of"] = {
                f"{k[0]}::{k[1]}": v for k, v in series_map.items()
            }
        except Exception:
            pass
        # include output hashes (if files exist)
        outputs = {"excel": cfg.excel.path, "csv": "./output/macro_ranking.csv"}
        mpath = write_manifest(manifest, outputs=outputs)
        logging.info(f"Wrote manifest to {mpath}")
    except Exception as e:
        logging.warning(f"Failed to write manifest: {e}")
    if data.empty:
        logging.warning("No data fetched.")
    # Harmonize
    data = harmonize_countries(data)
    data = parse_dates(data)
    # Apply frequency harmonization to all series (to target frequency from config)
    try:
        from .processing.harmonize import harmonize_df

        target_freq = cfg.period.get('frequency', 'Q') if isinstance(cfg.period, dict) else getattr(cfg.period, 'frequency', 'Q')
        data, harmonize_report = harmonize_df(data, target_freq=target_freq, aggregation='mean')
        # attach report (could be written to manifest later)
        manifest['harmonize_report'] = harmonize_report.to_dict(orient='records')
    except Exception:
        harmonize_report = None
    # Compute per-series as_of mapping from fetch entries (for point-in-time/backtest enforcement)
    as_of_map = {}
    try:
        # Only respect fetch entries that indicate point-in-time semantics.
        # A series will be enforced for no-backfill if either the global backtest
        # configuration has no_backfill=True or the individual fetch entry has
        # no_backfill=True. This allows per-source metadata to opt into point-in-time behavior.
        enforce_global = getattr(cfg, "backtest", None) and getattr(cfg.backtest, "no_backfill", False)
        for f in fetch_entries:
            try:
                # only consider entries that opt into no_backfill or if global enforcement is on
                if not (enforce_global or f.get("no_backfill", False)):
                    continue
                key = (f.get("indicator"), f.get("country"))
                ts = f.get("fetch_timestamp")
                if key[0] is None or key[1] is None or ts is None:
                    continue
                # keep the most recent fetch_timestamp
                if key not in as_of_map or ts > as_of_map[key]:
                    as_of_map[key] = ts
            except Exception:
                continue
    except Exception:
        as_of_map = {}

    # Apply transforms per indicator
    transformed_rows = []
    for ind in cfg.indicators:
        ind_df = data[data["indicator"] == ind.id]
        if ind_df.empty:
            continue
        ind_df = apply_transform(ind_df, ind.transform or "none", ind.id)
        ind_df = smooth(ind_df, cfg.scoring.smoothing)
        # Use new pipeline standardization (rolling baseline -> winsorize -> robust_zscore)
        try:
            # extract standardization config if present
            try:
                global_std = cfg.scoring.standardization.dict()
            except Exception:
                global_std = cfg.scoring.standardization if getattr(cfg.scoring, 'standardization', None) else None
            try:
                ind_std = ind.standardization.dict()
            except Exception:
                ind_std = getattr(ind, 'standardization', None)
            # merge dicts: per-indicator overrides global
            if isinstance(global_std, dict) and isinstance(ind_std, dict):
                merged_std = dict(global_std)
                merged_std.update(ind_std)
            else:
                merged_std = ind_std or global_std
            method = getattr(cfg.scoring, 'standardization_method', 'robust_zscore')
            invert_flag = True if getattr(ind, 'good_direction', None) == 'down' else False
            ind_df = apply_standardization(ind_df, config=merged_std, method=method, invert=invert_flag)
            # ensure compatibility with downstream code expecting 'value_std'
            if 'std_value' in ind_df.columns:
                ind_df = ind_df.rename(columns={'std_value': 'value_std'})
        except Exception:
            # fallback to legacy standardize function
            ind_df = standardize(ind_df, cfg.scoring.standardization)
            # apply inversion if indicator semantics require lower==better
            if getattr(ind, 'good_direction', None) == 'down':
                try:
                    ind_df['value_std'] = -ind_df['value_std']
                except Exception:
                    pass
        # If backtest.no_backfill is set, enforce point-in-time by dropping rows whose date > fetch as_of
        if getattr(cfg, "backtest", None) and getattr(
            cfg.backtest, "no_backfill", False
        ):
            try:
                # for each country, get as_of timestamp for this indicator
                def _filter_row(r):
                    key = (r["indicator"], r["country"])
                    as_of_ts = as_of_map.get(key)
                    if not as_of_ts:
                        return True
                    # compare date strings YYYY-MM-DD or YYYY to timestamp date
                    try:
                        row_date = pd.to_datetime(r["date"]).date()
                        as_of_date = pd.to_datetime(as_of_ts).date()
                        return row_date <= as_of_date
                    except Exception:
                        return True

                ind_df = ind_df[ind_df.apply(_filter_row, axis=1)]
            except Exception:
                pass
        # keep latest per country
        latest = ind_df.sort_values("date").groupby("country").last().reset_index()
        transformed_rows.append(
            latest[["country", "indicator", "date", "value", "value_std"]]
        )
    if not transformed_rows:
        logging.error("No transformed data available. Exiting.")
        return
    indicators_df = pd.concat(transformed_rows, ignore_index=True)
    # Create pivot for scoring
    pivot = indicators_df.pivot(
        index="country", columns="indicator", values="value_std"
    )
    coverage = compute_coverage(pivot)
    # apply min coverage
    min_cov = cfg.scoring.min_coverage_ratio
    eligible = coverage[coverage >= min_cov].index
    pivot_eligible = pivot.loc[eligible]
    # determine coverage penalty settings from config
    apply_cov_pen = getattr(cfg.scoring, "apply_coverage_penalty", False)
    cov_k = float(getattr(cfg.scoring, "coverage_k", 1.0))
    scores = compute_composite(
        pivot_eligible, cfg.scoring.weights, apply_coverage_penalty=apply_cov_pen, coverage_k=cov_k
    )

    # Optional: bootstrap uncertainty & rank stability
    try:
        if getattr(cfg.scoring, "bootstrap", None) and getattr(cfg.scoring.bootstrap, "enabled", False):
            from src.processing.scoring import bootstrap_scores, rank_stability

            n_boot = int(getattr(cfg.scoring.bootstrap, "n", 1000))
            seed = int(getattr(cfg.scoring.bootstrap, "seed", 0))
            summary, samples = bootstrap_scores(pivot_eligible, cfg.scoring.weights, n_boot=n_boot, seed=seed)
            # summary contains score_mean, score_ci_low, score_ci_high
            # override scores with mean
            scores = summary["score_mean"].reindex(scores.index)
    except Exception:
        pass

    ranked = rank_scores(scores)
    # attach CI and stability columns if available
    try:
        if 'score_ci_low' in locals() or (getattr(cfg.scoring, "bootstrap", None) and getattr(cfg.scoring.bootstrap, "enabled", False)):
            # attach CI and stability if summary/samples exist
            if 'summary' in locals():
                ranked['score_ci_low'] = summary['score_ci_low'].reindex(ranked.index)
                ranked['score_ci_high'] = summary['score_ci_high'].reindex(ranked.index)
            if 'samples' in locals():
                baseline_scores = compute_composite(
                    pivot_eligible, cfg.scoring.weights, apply_coverage_penalty=apply_cov_pen, coverage_k=cov_k
                )
                stab = rank_stability(samples, baseline_scores)
                ranked['rank_stability'] = stab.reindex(ranked.index)
    except Exception:
        pass
    ranked["coverage_ratio"] = coverage.loc[ranked.index]
    # Einzelindikatoren: keep raw and std
    # Raw data: merge
    raw_df = data
    # Optional: run backtest if configured
    portfolio_df = None
    backtest_df = None
    try:
        if getattr(cfg, "backtest", None) and getattr(cfg.backtest, "enabled", False):
            logging.info("Backtest enabled — constructing time-series signals and running backtest")
            # Build time-series signals (index: dates, columns: countries) from raw_df
            try:
                # we expect raw_df to contain historical standardized values per date and country
                hist = raw_df.copy()
                # standardized column name is value_std; if not present, try 'value'
                if "value_std" in hist.columns:
                    valcol = "value_std"
                else:
                    valcol = "value"
                # pivot to dates x country
                signals_ts = (
                    hist[["date", "country", valcol]]
                    .dropna()
                    .assign(date=lambda df: pd.to_datetime(df["date"]).dt.floor("D"))
                    .pivot(index="date", columns="country", values=valcol)
                )
                # compute rebalanced weights per date
                from src.backtest.simple import compute_rebalanced_weights, run_backtest

                w_by_date = compute_rebalanced_weights(
                    signals_ts,
                    top_n=(cfg.backtest.top_n if getattr(cfg.backtest, "top_n", None) is not None else cfg.allocation.top_n),
                    min_alloc=(cfg.backtest.min_alloc if getattr(cfg.backtest, "min_alloc", None) is not None else cfg.allocation.min_alloc),
                    max_alloc=(cfg.backtest.max_alloc if getattr(cfg.backtest, "max_alloc", None) is not None else cfg.allocation.max_alloc),
                )
                # Attempt to build price series from raw_df if available: expect column 'price' else fall back to synthetic returns
                prices = None
                if "price" in raw_df.columns:
                    try:
                        prices = (
                            raw_df[["date", "country", "price"]]
                            .assign(date=lambda df: pd.to_datetime(df["date"]).dt.floor("D"))
                            .pivot(index="date", columns="country", values="price")
                            .sort_index()
                        )
                    except Exception:
                        prices = None
                # If no price series, try to synthesize a price series from the signals (use exp(cumsum of signal pct changes))
                if prices is None:
                    # construct synthetic prices using small returns derived from z-scores changes
                    signals_ts_sorted = signals_ts.sort_index()
                    pct = signals_ts_sorted.fillna(0).diff().fillna(0) * 0.001
                    prices = (1 + pct).cumprod() * 100

                backtest_res = run_backtest(prices, w_by_date, rebalance_on=sorted(w_by_date.keys()))
                # produce portfolio table (last weights) and export
                # take last available weights as current allocation
                if w_by_date:
                    last_dt = sorted(w_by_date.keys())[-1]
                    last_w = w_by_date[last_dt].reindex(sorted(w_by_date[last_dt].index)).fillna(0.0)
                    portfolio_df = last_w.reset_index()
                    portfolio_df.columns = ["country", "weight"]
                backtest_df = backtest_res.reset_index()
            except Exception as e:
                logging.warning(f"Backtest construction failed: {e}")
    except Exception:
        logging.warning("Unexpected error while preparing backtest — skipping")

    # export
    # package config snapshot with optional portfolio/backtest frames for Excel writer
    cfg_for_excel = cfg.dict()
    if portfolio_df is not None:
        cfg_for_excel.setdefault("portfolio", {})
        cfg_for_excel["portfolio"]["allocations"] = portfolio_df
    if backtest_df is not None:
        cfg_for_excel.setdefault("backtest", {})
        cfg_for_excel["backtest"]["results"] = backtest_df

    # Try to write Excel; if the target is locked (PermissionError), retry with a timestamped filename
    try:
        export_to_excel(cfg.excel.path, ranked, indicators_df, raw_df, cfg_for_excel)
    except PermissionError:
        import datetime
        import os

        base, ext = os.path.splitext(cfg.excel.path)
        alt = f"{base}_{datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}{ext}"
        logging.warning(
            f"Could not write to {cfg.excel.path} (permission error). Retrying with {alt}"
        )
        export_to_excel(alt, ranked, indicators_df, raw_df, cfg_for_excel)
    logging.info("Done.")
    # Portfolio allocations: convert scores to weights, support alternative portfolio methods
    try:
        # attempt to read previous allocations if present
        prev_alloc = None
        try:
            if os.path.exists("./output/allocations.csv"):
                prev_alloc = pd.read_csv("./output/allocations.csv").set_index("country")["weight"]
        except Exception:
            prev_alloc = None

        # choose allocation method
        try:
            pconf = cfg.portfolio if getattr(cfg, 'portfolio', None) is not None else None
            method = None
            if pconf is not None:
                try:
                    method = getattr(pconf, 'method', None) if not isinstance(pconf, dict) else pconf.get('method')
                except Exception:
                    method = None
        except Exception:
            method = None

        if method == 'threshold_power':
            from src.portfolio.rebalance import compute_target_weights

            thr = 0.0
            pw = 1.0
            top_n = None
            min_alloc = None
            max_alloc = None
            try:
                if pconf is not None:
                    thr = float(getattr(pconf, 'threshold', pconf.get('threshold')) if not isinstance(pconf, dict) else pconf.get('threshold', 0.0))
                    pw = float(getattr(pconf, 'power', pconf.get('power')) if not isinstance(pconf, dict) else pconf.get('power', 1.0))
                    top_n = int(getattr(pconf, 'top_n', pconf.get('top_n'))) if (getattr(pconf, 'top_n', None) or (isinstance(pconf, dict) and pconf.get('top_n') is not None)) else None
                    min_alloc = float(getattr(pconf, 'min_alloc', pconf.get('min_alloc'))) if (getattr(pconf, 'min_alloc', None) or (isinstance(pconf, dict) and pconf.get('min_alloc') is not None)) else 0.0
                    max_alloc = float(getattr(pconf, 'max_alloc', pconf.get('max_alloc'))) if (getattr(pconf, 'max_alloc', None) or (isinstance(pconf, dict) and pconf.get('max_alloc') is not None)) else 1.0
            except Exception:
                pass
            try:
                weights = compute_target_weights(ranked['score'], mapping=None, threshold=thr, power=pw, min_alloc=min_alloc, max_alloc=max_alloc, top_n=top_n)
            except Exception:
                weights = score_to_weights(ranked['score'], min_alloc=(cfg.allocation.min_alloc if getattr(cfg, 'allocation', None) else 0.0), max_alloc=(cfg.allocation.max_alloc if getattr(cfg, 'allocation', None) else 1.0), top_n=(cfg.allocation.top_n if getattr(cfg, 'allocation', None) else None))
        else:
            alloc_cfg = cfg.allocation
            weights = score_to_weights(
                ranked["score"],
                min_alloc=alloc_cfg.min_alloc,
                max_alloc=alloc_cfg.max_alloc,
                top_n=alloc_cfg.top_n,
            )

        write_allocations("./output/allocations.csv", weights)

        # ensure allocations CSV also appears in excel portfolio sheet if not already
        if portfolio_df is None:
            try:
                portfolio_df = (
                    pd.Series(weights).reset_index().rename(columns={"index": "country", 0: "weight"})
                )
            except Exception:
                portfolio_df = None

        # attach prev_weight column to cfg_for_excel portfolio allocations if prev_alloc exists
        try:
            if prev_alloc is not None and portfolio_df is not None:
                # merge prev weights
                pdf = portfolio_df.copy()
                pdf = pdf.merge(prev_alloc.reset_index().rename(columns={'country':'country','weight':'prev_weight'}), on='country', how='left')
                cfg_for_excel.setdefault('portfolio', {})
                cfg_for_excel['portfolio']['allocations'] = pdf
        except Exception:
            pass
    except Exception as e:
        logging.warning(f"Failed to write allocations: {e}")


if __name__ == "__main__":
    main()
