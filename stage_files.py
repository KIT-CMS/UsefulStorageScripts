#! /usr/bin/env python3
"""
stage_files.py — Large-scale dCache tape staging via WLCG Tape REST API v1.

Manages a complete staging lifecycle: pre-check file locality, submit bulk
stage requests in batches, poll for progress, and release (unpin) completed
files to free pool space. Full state is persisted to a JSON file for crash
recovery and restart.

Usage:
    python3 stage_files.py /path/to/workdir

The workdir must contain a .conf configuration file and a file list.
See README.md for configuration details.
"""

import argparse
import configparser
import datetime
import json
import logging
import math
import os
import signal
import ssl
import sys
import tempfile
import time

import requests as http_requests
from cryptography import x509
from cryptography.hazmat.primitives.serialization import Encoding

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STATE_VERSION = 1

OVERALL_NEW = "new"
OVERALL_SUBMITTING = "submitting"
OVERALL_STAGING = "staging"
OVERALL_COMPLETED = "completed"
OVERALL_COMPLETED_WITH_ERRORS = "completed_with_errors"

FILE_SUBMITTED = "SUBMITTED"
FILE_STARTED = "STARTED"
FILE_COMPLETED = "COMPLETED"
FILE_FAILED = "FAILED"
FILE_CANCELLED = "CANCELLED"

TERMINAL_FILE_STATES = {FILE_COMPLETED, FILE_FAILED, FILE_CANCELLED}
ONLINE_LOCALITIES = {"DISK", "DISK_AND_TAPE"}

# Grace period (in seconds) to wait after a file completes before releasing
# This prevents race conditions where the pin is removed before the bulk
# STAGE operation can properly complete and report success.
# See: https://github.com/dcache/dcache/issues/...
RELEASE_GRACE_PERIOD = int(os.environ.get("STAGING_RELEASE_GRACE_PERIOD", 120))

TRANSIENT_HTTP_CODES = {429, 500, 502, 503, 504}
AUTH_ERROR_CODES = {401, 403}
RETRY_DELAYS = [10, 30, 90]


class AuthError(Exception):
    """Raised on unrecoverable authentication/authorization failures."""


# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------

_shutdown_requested = False


def _handle_signal(signum, frame):
    global _shutdown_requested
    _shutdown_requested = True
    logging.getLogger().info("Shutdown requested (signal %d), finishing current cycle…", signum)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

class Config:
    """Parses and holds all configuration from the .conf file and environment."""

    def __init__(self, workdir: str, conf_path: str):
        self.workdir = workdir
        cp = configparser.ConfigParser()
        if not cp.read(conf_path):
            raise SystemExit(f"Cannot read config file: {conf_path}")

        # [dcache]
        self.base_url = cp.get("dcache", "base_url").rstrip("/")
        self.api_path = cp.get("dcache", "api_path", fallback="/api/v1/tape").strip("/")
        self.batch_size = cp.getint("dcache", "batch_size", fallback=2000)
        self.disk_lifetime = cp.get("dcache", "disk_lifetime", fallback="P7D")
        self.poll_interval = cp.getint("dcache", "poll_interval", fallback=300)
        self.archiveinfo_batch_size = cp.getint("dcache", "archiveinfo_batch_size", fallback=5000)
        self.auto_release = cp.getboolean("dcache", "auto_release", fallback=True)

        # [auth]
        self.proxy_cert = self._resolve_proxy(cp)
        self.ca_dir = self._resolve_ca_dir(cp)
        self.proxy_lifetime_factor = cp.getfloat("auth", "proxy_lifetime_factor", fallback=2.0)

        # [files]
        self.filelist = os.path.join(workdir, cp.get("files", "filelist", fallback="filelist.txt"))
        self.state_file = os.path.join(workdir, cp.get("files", "state_file", fallback="stage_state.json"))

        # [logging]
        self.log_file = os.path.join(workdir, cp.get("logging", "log_file", fallback="stage_files.log"))

    # -- helpers --

    @staticmethod
    def _resolve_proxy(cp: configparser.ConfigParser) -> str:
        val = cp.get("auth", "proxy_cert", fallback="").strip()
        if val:
            return val
        val = os.environ.get("X509_USER_PROXY", "").strip()
        if val:
            return val
        return f"/tmp/x509up_u{os.getuid()}"

    @staticmethod
    def _resolve_ca_dir(cp: configparser.ConfigParser) -> str:
        val = cp.get("auth", "ca_dir", fallback="").strip()
        if val:
            return val
        val = os.environ.get("X509_CERT_DIR", "").strip()
        if val:
            return val
        return "/etc/grid-security/certificates"

    @property
    def stage_url(self) -> str:
        return f"{self.base_url}/{self.api_path}/stage"

    @property
    def archiveinfo_url(self) -> str:
        return f"{self.base_url}/{self.api_path}/archiveinfo"

    def release_url(self, request_id: str) -> str:
        return f"{self.base_url}/{self.api_path}/release/{request_id}"

    def stage_status_url(self, request_id: str) -> str:
        return f"{self.base_url}/{self.api_path}/stage/{request_id}"


# ---------------------------------------------------------------------------
# Proxy certificate lifetime helpers
# ---------------------------------------------------------------------------

def _get_proxy_expiry(proxy_path: str, logger: logging.Logger) -> datetime.datetime | None:
    """Read the expiry time (notAfter) from a PEM proxy certificate.

    Returns a timezone-aware UTC datetime, or None if parsing fails.
    """
    try:
        with open(proxy_path, "rb") as f:
            pem_data = f.read()
        cert = x509.load_pem_x509_certificate(pem_data)
        return cert.not_valid_after_utc
    except Exception as exc:
        logger.warning("Could not read proxy certificate expiry from %s: %s", proxy_path, exc)
        return None


def _check_proxy_lifetime(cfg: Config, logger: logging.Logger) -> None:
    """Raise AuthError if the proxy is expired or about to expire.

    The safety margin is ``proxy_lifetime_factor * poll_interval`` seconds.
    """
    expiry = _get_proxy_expiry(cfg.proxy_cert, logger)
    if expiry is None:
        return  # cannot determine — let the server decide

    remaining = (expiry - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
    margin = cfg.proxy_lifetime_factor * cfg.poll_interval

    if remaining <= 0:
        raise AuthError(
            f"Proxy certificate has expired ({expiry.isoformat()})"
        )
    if remaining < margin:
        raise AuthError(
            f"Proxy expires in {int(remaining)}s, below safety margin of {int(margin)}s "
            f"(factor {cfg.proxy_lifetime_factor} × poll_interval {cfg.poll_interval}s). "
            f"Expiry: {expiry.isoformat()}"
        )
    logger.info("Proxy certificate valid for %d more seconds (margin: %ds)", int(remaining), int(margin))


# ---------------------------------------------------------------------------
# HTTP client helpers
# ---------------------------------------------------------------------------

def _create_session(cfg: Config) -> http_requests.Session:
    """Create a requests.Session configured with X.509 proxy auth."""
    session = http_requests.Session()
    session.cert = (cfg.proxy_cert, cfg.proxy_cert)
    session.verify = cfg.ca_dir
    session.headers.update({
        "Content-Type": "application/json",
        "Accept": "application/json",
    })
    return session


def _request_with_retry(
    session: http_requests.Session,
    method: str,
    url: str,
    logger: logging.Logger,
    **kwargs,
) -> http_requests.Response:
    """Execute an HTTP request with exponential-backoff retry on transient errors."""
    last_exc = None
    for attempt, delay in enumerate(RETRY_DELAYS + [0], start=1):
        try:
            resp = session.request(method, url, **kwargs)
            if resp.status_code in AUTH_ERROR_CODES:
                raise AuthError(
                    f"HTTP {resp.status_code} from {method.upper()} {url}: "
                    f"{resp.text[:300]}"
                )
            if resp.status_code not in TRANSIENT_HTTP_CODES:
                return resp
            logger.warning(
                "HTTP %d from %s %s (attempt %d/%d)",
                resp.status_code, method.upper(), url, attempt, len(RETRY_DELAYS) + 1,
            )
        except http_requests.RequestException as exc:
            logger.warning(
                "Request error %s %s: %s (attempt %d/%d)",
                method.upper(), url, exc, attempt, len(RETRY_DELAYS) + 1,
            )
            last_exc = exc
        if delay:
            time.sleep(delay)

    # Return last response if we got one, otherwise raise
    if last_exc:
        raise last_exc
    return resp  # type: ignore[possibly-undefined]


# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------

def _new_state(total_files: int, pending: list[str]) -> dict:
    """Create a fresh state dict."""
    now = datetime.datetime.now().isoformat(timespec="seconds")
    return {
        "version": STATE_VERSION,
        "overall_state": OVERALL_NEW,
        "created_at": now,
        "updated_at": now,
        "total_files": total_files,
        "summary": {
            "skipped_online": 0,
            "submitted": 0,
            "completed": 0,
            "released": 0,
            "failed": 0,
            "in_progress": 0,
        },
        "requests": {},
        "pending_files": pending,
        "skipped_files": {},
    }


def _save_state(state: dict, path: str) -> None:
    """Atomically write state to JSON."""
    state["updated_at"] = datetime.datetime.now().isoformat(timespec="seconds")
    dir_ = os.path.dirname(path)
    fd, tmp = tempfile.mkstemp(dir=dir_, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(state, f, indent=2)
            f.write("\n")
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _load_state(path: str) -> dict:
    """Load state from JSON."""
    with open(path) as f:
        return json.load(f)


def _recompute_summary(state: dict) -> None:
    """Recompute the summary counters from the actual per-file data."""
    completed = 0
    released = 0
    failed = 0
    in_progress = 0
    submitted = 0
    for req in state["requests"].values():
        for finfo in req["files"].values():
            s = finfo["state"]
            if s == FILE_COMPLETED:
                completed += 1
                if finfo.get("released", False):
                    released += 1
            elif s == FILE_FAILED or s == FILE_CANCELLED:
                failed += 1
            elif s == FILE_STARTED:
                in_progress += 1
            else:
                submitted += 1
    state["summary"]["skipped_online"] = len(state.get("skipped_files", {}))
    state["summary"]["submitted"] = submitted
    state["summary"]["completed"] = completed
    state["summary"]["released"] = released
    state["summary"]["failed"] = failed
    state["summary"]["in_progress"] = in_progress


# ---------------------------------------------------------------------------
# Phase implementations
# ---------------------------------------------------------------------------

def phase_archiveinfo(cfg: Config, state: dict, session: http_requests.Session, logger: logging.Logger) -> None:
    """Phase 2: pre-check file locality and skip already-online files."""
    pending = state["pending_files"]
    total = len(pending)
    batch_sz = cfg.archiveinfo_batch_size
    n_batches = math.ceil(total / batch_sz)
    skipped = state.setdefault("skipped_files", {})
    still_pending = []

    logger.info("Archiveinfo pre-check: %d files in %d batch(es)", total, n_batches)

    for i in range(n_batches):
        if _shutdown_requested:
            break
        batch = pending[i * batch_sz : (i + 1) * batch_sz]
        resp = _request_with_retry(
            session, "POST", cfg.archiveinfo_url, logger,
            json={"paths": batch},
        )
        if resp.status_code != 200:
            logger.warning("Archiveinfo returned HTTP %d, skipping pre-check for this batch", resp.status_code)
            still_pending.extend(batch)
            continue

        results = resp.json()

        # Guard against silent server-side truncation: dCache drops paths
        # beyond frontend.service.wlcg.file-locality-max-files (default 10000)
        # without any error.  Detect this and ensure no files are lost.
        returned_paths = {entry.get("path", "") for entry in results}
        for path in batch:
            if path not in returned_paths:
                logger.warning(
                    "Archiveinfo response missing path (server-side truncation?): %s",
                    path,
                )
                still_pending.append(path)

        for entry in results:
            path = entry.get("path", "")
            locality = entry.get("locality", "")
            if locality in ONLINE_LOCALITIES:
                skipped[path] = locality
            elif "error" in entry:
                logger.warning("Archiveinfo error for %s: %s", path, entry["error"])
                still_pending.append(path)
            else:
                still_pending.append(path)

    state["pending_files"] = still_pending
    state["overall_state"] = OVERALL_SUBMITTING
    _recompute_summary(state)
    _save_state(state, cfg.state_file)
    logger.info(
        "Pre-check done: %d files already online (skipped), %d files need staging",
        len(skipped), len(still_pending),
    )


def phase_submit(cfg: Config, state: dict, session: http_requests.Session, logger: logging.Logger) -> None:
    """Phase 3: submit stage requests in batches."""
    pending = state["pending_files"]
    batch_sz = cfg.batch_size
    n_batches = math.ceil(len(pending) / batch_sz) if pending else 0

    logger.info("Submitting %d files in %d batch(es) of up to %d", len(pending), n_batches, batch_sz)

    submitted_count = 0
    while state["pending_files"] and not _shutdown_requested:
        _check_proxy_lifetime(cfg, logger)
        batch = state["pending_files"][:batch_sz]
        files_payload = [{"path": p, "diskLifetime": cfg.disk_lifetime} for p in batch]

        resp = _request_with_retry(
            session, "POST", cfg.stage_url, logger,
            json={"files": files_payload},
        )

        if resp.status_code != 201:
            logger.error("Stage submit failed with HTTP %d: %s", resp.status_code, resp.text[:500])
            break

        data = resp.json()
        request_id = data.get("requestId")
        if not request_id:
            logger.error("No requestId in stage response: %s", resp.text[:500])
            break

        now = datetime.datetime.now().isoformat(timespec="seconds")
        state["requests"][request_id] = {
            "state": "QUEUED",
            "submitted_at": now,
            "completed_at": None,
            "files": {
                p: {"state": FILE_SUBMITTED, "error": None, "released": False}
                for p in batch
            },
        }
        state["pending_files"] = state["pending_files"][batch_sz:]
        submitted_count += 1
        _recompute_summary(state)
        _save_state(state, cfg.state_file)
        logger.info(
            "Submitted batch %d/%d: request %s with %d files",
            submitted_count, n_batches, request_id, len(batch),
        )

    if not state["pending_files"]:
        state["overall_state"] = OVERALL_STAGING
        _recompute_summary(state)
        _save_state(state, cfg.state_file)
        logger.info("All batches submitted, entering staging/polling phase")


def phase_poll_and_release(cfg: Config, state: dict, session: http_requests.Session, logger: logging.Logger) -> None:
    """Phase 4: poll request statuses and release completed files."""
    while not _shutdown_requested:
        _check_proxy_lifetime(cfg, logger)

        active_requests = [
            rid for rid, rinfo in state["requests"].items()
            if rinfo["state"] not in {"COMPLETED", "CANCELLED"}
        ]

        if not active_requests:
            _finalize(state, cfg, logger)
            return

        for rid in active_requests:
            if _shutdown_requested:
                break
            _poll_single_request(cfg, state, session, logger, rid)

        _recompute_summary(state)
        _save_state(state, cfg.state_file)

        s = state["summary"]
        total_to_stage = state["total_files"] - s["skipped_online"]
        logger.info(
            "Progress: %d/%d completed (%d released), %d failed, %d in progress across %d active request(s)",
            s["completed"], total_to_stage, s["released"],
            s["failed"], s["in_progress"] + s["submitted"], len(active_requests),
        )

        # Check if all done
        remaining_active = [
            rid for rid, rinfo in state["requests"].items()
            if rinfo["state"] not in {"COMPLETED", "CANCELLED"}
        ]
        if not remaining_active:
            _finalize(state, cfg, logger)
            return

        logger.info("Sleeping %d seconds before next poll cycle…", cfg.poll_interval)
        _interruptible_sleep(cfg.poll_interval)


def _poll_single_request(
    cfg: Config, state: dict, session: http_requests.Session,
    logger: logging.Logger, request_id: str,
) -> None:
    """Poll a single stage request and optionally release completed files."""
    rinfo = state["requests"][request_id]

    resp = _request_with_retry(
        session, "GET", cfg.stage_status_url(request_id), logger,
    )

    if resp.status_code == 404:
        logger.warning("Request %s returned 404, marking as COMPLETED", request_id)
        rinfo["state"] = "COMPLETED"
        rinfo["completed_at"] = datetime.datetime.now().isoformat(timespec="seconds")
        return

    if resp.status_code != 200:
        logger.warning("Unexpected HTTP %d polling request %s, skipping this cycle", resp.status_code, request_id)
        return

    data = resp.json()
    newly_completed_paths = []
    files_ready_for_release = []

    for file_entry in data.get("files", []):
        path = file_entry.get("path", "")
        remote_state = file_entry.get("state", "")
        error = file_entry.get("error")

        if path not in rinfo["files"]:
            continue

        local = rinfo["files"][path]
        old_state = local["state"]

        # Map WLCG states
        if remote_state in {FILE_COMPLETED, FILE_FAILED, FILE_CANCELLED, FILE_STARTED, FILE_SUBMITTED}:
            local["state"] = remote_state
        if error:
            local["error"] = error

        # Detect newly completed - record the completion time
        if local["state"] == FILE_COMPLETED and old_state != FILE_COMPLETED:
            local["completed_at"] = datetime.datetime.now().isoformat(timespec="seconds")
            newly_completed_paths.append(path)

    # Check ALL completed files (not just newly completed) for release eligibility
    # This handles the case where files completed in previous cycles but haven't
    # been released yet due to the grace period
    now = datetime.datetime.now()
    for path, local in rinfo["files"].items():
        # Skip if not completed or already released
        if local["state"] != FILE_COMPLETED or local.get("released", False):
            continue
        
        completed_at_str = local.get("completed_at")
        if completed_at_str:
            completed_at = datetime.datetime.fromisoformat(completed_at_str)
            elapsed = (now - completed_at).total_seconds()
            if elapsed >= RELEASE_GRACE_PERIOD:
                files_ready_for_release.append(path)
        else:
            # Fallback: release immediately if no completion time recorded
            # (shouldn't happen, but handle gracefully)
            files_ready_for_release.append(path)

    # Release files that have passed the grace period
    if cfg.auto_release and files_ready_for_release:
        _release_files(cfg, session, logger, request_id, files_ready_for_release, rinfo)

    # Check if this request is fully terminal
    all_terminal = all(
        fi["state"] in TERMINAL_FILE_STATES
        for fi in rinfo["files"].values()
    )
    if all_terminal:
        rinfo["state"] = "COMPLETED"
        rinfo["completed_at"] = datetime.datetime.now().isoformat(timespec="seconds")


def _release_files(
    cfg: Config, session: http_requests.Session, logger: logging.Logger,
    request_id: str, paths: list[str], rinfo: dict,
) -> None:
    """Release (unpin) completed files for a given request."""
    resp = _request_with_retry(
        session, "POST", cfg.release_url(request_id), logger,
        json={"paths": paths},
    )
    if resp.status_code == 200:
        for p in paths:
            if p in rinfo["files"]:
                rinfo["files"][p]["released"] = True
        logger.info("Released %d completed file(s) from request %s", len(paths), request_id)
    else:
        logger.warning(
            "Release returned HTTP %d for request %s (%d files), will retry next cycle",
            resp.status_code, request_id, len(paths),
        )


def _finalize(state: dict, cfg: Config, logger: logging.Logger) -> None:
    """Set final overall state and save."""
    has_failures = state["summary"]["failed"] > 0
    state["overall_state"] = OVERALL_COMPLETED_WITH_ERRORS if has_failures else OVERALL_COMPLETED
    _recompute_summary(state)
    _save_state(state, cfg.state_file)

    s = state["summary"]
    total = state["total_files"]
    logger.info(
        "Staging complete — total: %d, skipped (online): %d, completed: %d (%d released), failed: %d",
        total, s["skipped_online"], s["completed"], s["released"], s["failed"],
    )
    if has_failures:
        logger.warning("Finished with errors — see state file for per-file details: %s", cfg.state_file)


def _interruptible_sleep(seconds: int) -> None:
    """Sleep in 1-second increments so we can respond to shutdown signals."""
    for _ in range(seconds):
        if _shutdown_requested:
            break
        time.sleep(1)


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def setup_logging(cfg: Config) -> logging.Logger:
    """Configure dual-output logging (stdout + file)."""
    logger = logging.getLogger("stage_files")
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # Append-mode file handler with timestamped name
    log_stem, log_ext = os.path.splitext(cfg.log_file)
    timestamped_log = f"{log_stem}_{datetime.datetime.now():%Y-%m-%d_%H-%M-%S}{log_ext}"
    file_handler = logging.FileHandler(timestamped_log)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Manage large-scale tape staging via dCache WLCG Tape REST API v1.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "workdir",
        help="Working directory containing .conf file, file list, and state file.",
    )
    args = parser.parse_args()

    workdir = os.path.abspath(args.workdir)
    if not os.path.isdir(workdir):
        raise SystemExit(f"Workdir does not exist: {workdir}")

    # Find .conf file in workdir
    conf_files = [f for f in os.listdir(workdir) if f.endswith(".conf")]
    if len(conf_files) != 1:
        raise SystemExit(
            f"Expected exactly one .conf file in {workdir}, found {len(conf_files)}: {conf_files}"
        )
    conf_path = os.path.join(workdir, conf_files[0])

    cfg = Config(workdir, conf_path)
    logger = setup_logging(cfg)

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    logger.info("Starting stage_files with workdir: %s", workdir)

    # Validate proxy cert exists and has sufficient lifetime
    if not os.path.isfile(cfg.proxy_cert):
        raise SystemExit(f"Proxy certificate not found: {cfg.proxy_cert}")
    _check_proxy_lifetime(cfg, logger)

    # Create HTTP session
    session = _create_session(cfg)

    # Load or initialize state
    if os.path.isfile(cfg.state_file):
        state = _load_state(cfg.state_file)
        logger.info("Resumed from state file (overall_state=%s)", state["overall_state"])
    else:
        if not os.path.isfile(cfg.filelist):
            raise SystemExit(f"File list not found: {cfg.filelist}")
        with open(cfg.filelist) as f:
            files = [line.strip().split(",")[0].strip() for line in f if line.strip()]
        if not files:
            raise SystemExit(f"File list is empty: {cfg.filelist}")
        state = _new_state(len(files), files)
        _save_state(state, cfg.state_file)
        logger.info("Initialized new staging task with %d files", len(files))

    # Already finished?
    if state["overall_state"] in {OVERALL_COMPLETED, OVERALL_COMPLETED_WITH_ERRORS}:
        logger.info("Task already in terminal state: %s", state["overall_state"])
        return

    try:
        # Phase 2: archiveinfo pre-check
        if state["overall_state"] == OVERALL_NEW:
            phase_archiveinfo(cfg, state, session, logger)
            if _shutdown_requested:
                logger.info("Shutdown after archiveinfo phase, state saved")
                return

        # Phase 3: submit batches
        if state["overall_state"] == OVERALL_SUBMITTING:
            phase_submit(cfg, state, session, logger)
            if _shutdown_requested:
                logger.info("Shutdown during submission phase, state saved")
                return

        # Phase 4: poll and release
        if state["overall_state"] == OVERALL_STAGING:
            phase_poll_and_release(cfg, state, session, logger)
            if _shutdown_requested:
                _recompute_summary(state)
                _save_state(state, cfg.state_file)
                logger.info("Shutdown during polling phase, state saved")

    except AuthError as exc:
        logger.error("Authentication failure: %s", exc)
        logger.error("Saving state and shutting down — renew your proxy certificate and restart.")
        state["stopped_reason"] = "auth_error"
        state["stopped_at"] = datetime.datetime.now().isoformat(timespec="seconds")
        _recompute_summary(state)
        _save_state(state, cfg.state_file)
        sys.exit(2)

    except Exception as exc:
        logger.error("Unexpected error: %s", exc, exc_info=True)
        logger.error("Saving state and shutting down.")
        state["stopped_reason"] = "unexpected_error"
        state["stopped_at"] = datetime.datetime.now().isoformat(timespec="seconds")
        _recompute_summary(state)
        _save_state(state, cfg.state_file)
        sys.exit(1)


if __name__ == "__main__":
    main()
