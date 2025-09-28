import json
import os
import sys
import hashlib
import hmac
import uuid
import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, Optional

ARTIFACT_DIR = "data/_artifacts"


def ensure_artifact_dir():
    os.makedirs(ARTIFACT_DIR, exist_ok=True)


def sha256_of_bytes(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()


def sha256_of_file(path: str) -> Optional[str]:
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None


def sha256_of_records(records) -> Optional[str]:
    """Compute a deterministic sha256 over a list of record dicts.

    The function normalizes the records by:
    - selecting a stable sort order (indicator, country, date)
    - ensuring keys are sorted in the JSON encoding
    - using compact separators and UTF-8 encoding
    Returns hex digest or None on error.
    """
    try:
        # defensively convert to list
        recs = list(records or [])

        # define stable key for sorting
        def sort_key(r):
            return (
                r.get("indicator") or "",
                r.get("country") or "",
                r.get("date") or "",
            )

        recs_sorted = sorted(recs, key=sort_key)
        # ensure consistent types for JSON by converting numbers to native Python types
        canonical = json.dumps(
            recs_sorted, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        return sha256_of_bytes(canonical.encode("utf-8"))
    except Exception:
        return None


def pip_freeze() -> str:
    try:
        out = subprocess.check_output(
            ["pip", "freeze"], stderr=subprocess.DEVNULL, text=True
        )
        return out
    except Exception:
        return ""


def pip_list() -> Dict[str, str]:
    """Return pip list as a mapping name->version when available."""
    try:
        out = subprocess.check_output(
            ["pip", "list", "--format", "json"], stderr=subprocess.DEVNULL, text=True
        )
        import json as _json

        items = _json.loads(out)
        res: Dict[str, str] = {}
        for it in items:
            if not isinstance(it, dict):
                continue
            name = it.get("name")
            version = it.get("version")
            if isinstance(name, str) and isinstance(version, str):
                res[name] = version
        return res
    except Exception:
        return {}


def git_commit_hash() -> Optional[str]:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL, text=True
        )
        return out.strip()
    except Exception:
        return None


def git_branch() -> Optional[str]:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        )
        return out.strip()
    except Exception:
        return None


def git_remote_url() -> Optional[str]:
    try:
        out = subprocess.check_output(
            ["git", "config", "--get", "remote.origin.url"],
            stderr=subprocess.DEVNULL,
            text=True,
        )
        return out.strip()
    except Exception:
        return None


def git_is_dirty() -> bool:
    try:
        out = subprocess.check_output(
            ["git", "status", "--porcelain"], stderr=subprocess.DEVNULL, text=True
        )
        return bool(out.strip())
    except Exception:
        return False


def environment_manifest() -> Dict[str, Any]:
    import platform

    env = {
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "pip_freeze": pip_freeze(),
        "pip_list": pip_list(),
        "git_commit": git_commit_hash(),
        "git_branch": git_branch(),
        "git_remote": git_remote_url(),
        "git_dirty": git_is_dirty(),
        "run_id": os.environ.get("RUN_ID") or None,
        "python_executable": sys.executable if hasattr(sys, "executable") else None,
        "cwd": os.getcwd(),
    }
    # compute a short deterministic environment hash combining python_version, git_commit and pip_freeze
    try:
        # include pip_list (sorted items) to strengthen determinism of the env hash
        env_payload = json.dumps(
            {
                "python_version": env["python_version"],
                "git_commit": env["git_commit"],
                "pip_freeze": env["pip_freeze"],
                "pip_list": env.get("pip_list") or {},
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        env["env_hash"] = sha256_of_bytes(env_payload)
    except Exception:
        env["env_hash"] = None
    return env


def _enrich_fetch_entry(fe: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize/enrich a single fetch entry to the canonical fetch schema.

    - ensure keys: request_url, params, http_status, response_time_ms, rows,
      etag, last_modified, sha256_raw, sha256_normalized, fetch_timestamp,
      indicator, country, api_meta
    - if 'records' present, compute sha256_normalized deterministically over records
    """
    out = dict(fe or {})
    # map common variants
    if "request_url" not in out:
        if "url" in out:
            out["request_url"] = out.get("url")
        elif "request" in out:
            out["request_url"] = out.get("request")
        else:
            out.setdefault("request_url", None)
    out.setdefault("params", out.get("params") or None)
    out.setdefault(
        "http_status", out.get("http_status") or out.get("status_code") or None
    )
    # default response time to 0 when not provided (indicates no timing was captured)
    if out.get("response_time_ms") is None:
        out["response_time_ms"] = 0
    # rows: prefer explicit 'rows' else number of records
    if "rows" not in out:
        recs = out.get("records")
        try:
            out["rows"] = len(recs) if recs is not None else None
        except Exception:
            out["rows"] = None
    out.setdefault("etag", out.get("etag") or out.get("e_tag") or None)
    out.setdefault("last_modified", out.get("last_modified") or None)
    # if raw bytes are provided (e.g., response.content) allow computing sha256_raw
    if not out.get("sha256_raw"):
        raw = out.get("raw_bytes") or out.get("response_bytes")
        if isinstance(raw, (bytes, bytearray)):
            try:
                out["sha256_raw"] = sha256_of_bytes(bytes(raw))
            except Exception:
                out["sha256_raw"] = None
        else:
            out.setdefault("sha256_raw", out.get("sha256_raw") or None)
    # compute sha256_normalized from attached 'records' if not present
    if not out.get("sha256_normalized"):
        recs = out.get("records")
        if recs:
            try:
                out["sha256_normalized"] = sha256_of_records(recs)
            except Exception:
                out["sha256_normalized"] = None
        else:
            out.setdefault("sha256_normalized", None)
    # fetch_timestamp: when the fetch occurred (isoformat) and optional as_of date for series
    # set fetch timestamp to now if missing to ensure provenance completeness
    if not out.get("fetch_timestamp"):
        try:
            out["fetch_timestamp"] = datetime.now(timezone.utc).isoformat()
        except Exception:
            out.setdefault("fetch_timestamp", None)
    out.setdefault("as_of", out.get("as_of") or None)
    # optional point-in-time/backfill flags
    out.setdefault("no_backfill", out.get("no_backfill") or False)
    out.setdefault("indicator", out.get("indicator") or None)
    out.setdefault("country", out.get("country") or None)
    out.setdefault("api_meta", out.get("api_meta") or None)
    return out


def _validate_fetch_entry(fe: Dict[str, Any]) -> list:
    """Return a list of validation issue strings for a fetch entry.

    Checks presence of key provenance fields and basic types.
    """
    issues = []
    required = [
        "request_url",
        "http_status",
        "response_time_ms",
        "rows",
        "sha256_normalized",
        "fetch_timestamp",
    ]
    for k in required:
        if k not in fe or fe.get(k) is None:
            issues.append(f"missing:{k}")
    # rows should be non-negative integer if present
    rows = fe.get("rows")
    if rows is not None:
        try:
            if int(rows) < 0:
                issues.append("rows_negative")
        except Exception:
            issues.append("rows_not_int")
    return issues


def write_manifest(
    manifest: Dict[str, Any],
    prefix: str = "manifest",
    outputs: Optional[Dict[str, str]] = None,
) -> str:
    """Write a manifest JSON to disk and augment it with environment info and output hashes.

    Arguments:
      manifest: base manifest dict (will not be mutated)
      prefix: filename prefix
      outputs: optional mapping of output logical name -> file path to compute hashes

    Returns path to manifest file.
    """
    ensure_artifact_dir()
    m = dict(manifest)  # shallow copy
    # stable timestamp for human readability (not used in deterministic signature)
    # use timezone-aware UTC datetime
    m.setdefault("run_timestamp_utc", datetime.now(timezone.utc).isoformat())
    m["environment"] = environment_manifest()

    # Enrich fetch entries to canonical schema where possible
    try:
        fetches = m.get("fetches") or []
        m["fetches"] = [_enrich_fetch_entry(f) for f in fetches]
    except Exception:
        # keep original if enrichment fails
        pass
    # Validate fetch entries and include validation report in manifest
    try:
        fetch_issues = {}
        for i, f in enumerate(m.get("fetches", [])):
            issues = _validate_fetch_entry(f)
            if issues:
                fetch_issues[str(i)] = issues
        m.setdefault("validation", {})
        m["validation"]["fetch_issues"] = fetch_issues
        m["validation"]["n_issues"] = sum(len(v) for v in fetch_issues.values())
    except Exception:
        pass

    # compute output hashes where provided
    out_hashes = {}
    if outputs:
        for k, p in outputs.items():
            h = sha256_of_file(p)
            out_hashes[k] = {"path": p, "sha256": h}
    m["outputs"] = out_hashes

    # Build a stable payload for deterministic run_id/signature. We intentionally
    # only include the stable, input-like fields so that ephemeral fields such as
    # run_timestamp_utc or environment do not change the HMAC.
    stable_keys = ["config_snapshot", "fetch_summary", "fetches", "n_rows", "outputs"]
    stable_payload = {k: m.get(k) for k in stable_keys if k in m}

    # Optional signing: if MANIFEST_SIGNING_KEY is set, compute HMAC-SHA256 over
    # the canonical JSON of the stable payload and use that as a deterministic
    # run_id (and also expose as manifest_signature). Otherwise generate a UUID4.
    signing_key = os.environ.get("MANIFEST_SIGNING_KEY")
    if signing_key:
        canonical = json.dumps(
            stable_payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
        sig = hmac.new(
            signing_key.encode("utf-8"), canonical, hashlib.sha256
        ).hexdigest()
        run_id = sig
        m["manifest_signature"] = sig
    else:
        run_id = str(uuid.uuid4())

    m["run_id"] = run_id

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = os.path.join(ARTIFACT_DIR, f"{prefix}_{ts}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(m, f, ensure_ascii=False, indent=2, default=str)
    return path
