import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from typing import Any

import structlog

from sqs_job_worker.job_middleware import JobMiddleware

logger = structlog.get_logger(__name__)


class EcsDrainMiddleware(JobMiddleware):
    """Pause polling while a newer running task-definition revision exists.

    The current task identity comes from ECS Task Metadata Endpoint V4 unless it is
    supplied explicitly. The ECS client is created by the application and injected.
    Detection uses running task revisions rather than the latest registered revision,
    so polling resumes if the newer generation fails or stops.
    """

    MIN_QUERY_INTERVAL_SECONDS = 10
    METADATA_TIMEOUT_SECONDS = 1.0
    MAX_LIST_TASKS_PAGES = 20

    def __init__(
        self, ecs_client, cluster: str | None = None, family: str | None = None, revision: int | None = None, *, recheck_interval_seconds: float = 15
    ) -> None:
        if recheck_interval_seconds <= 0:
            raise ValueError(f"recheck_interval_seconds must be positive, got {recheck_interval_seconds!r}")
        self._cluster = cluster
        self._family = family
        self._revision = revision
        self._metadata_resolved = cluster is not None and family is not None and revision is not None
        self._ecs = ecs_client
        self._cached_result: bool | None = None
        self._cached_at = 0.0
        self._recheck_interval_seconds = recheck_interval_seconds
        self._draining = False

    def poll(self, call_next: Callable[[], None]) -> None:
        if self._is_draining():
            if not self._draining:
                logger.info("draining detected; stopped receiving and idling until SIGTERM or drain release")
            self._draining = True
            time.sleep(self._recheck_interval_seconds)
            return
        if self._draining:
            logger.info("drain released (no newer running generation); resuming receive")
        self._draining = False
        call_next()

    def _is_draining(self) -> bool:
        now = time.monotonic()
        if self._cached_result is not None and (now - self._cached_at) < self.MIN_QUERY_INTERVAL_SECONDS:
            return self._cached_result
        self._resolve_metadata()
        # Fail open outside ECS or while task identity is still unresolved.
        result = False if not self._family or self._revision is None else self._query_is_draining(self._revision)
        self._cached_result = result
        self._cached_at = now
        return result

    def _resolve_metadata(self) -> None:
        if self._metadata_resolved:
            return
        meta = self._fetch_metadata()
        if meta is None:
            return
        self._cluster = self._cluster if self._cluster is not None else meta.get("cluster")
        self._family = self._family if self._family is not None else meta.get("family")
        self._revision = self._revision if self._revision is not None else meta.get("revision")
        self._metadata_resolved = True

    def _fetch_metadata(self) -> dict | None:
        """Return task metadata, or None after a transient failure so a later poll retries."""
        uri = os.environ.get("ECS_CONTAINER_METADATA_URI_V4")
        if not uri:
            logger.info("ECS_CONTAINER_METADATA_URI_V4 is not set; drain detection disabled (continuing)")
            return {}
        if urllib.parse.urlsplit(uri).scheme not in ("http", "https"):
            logger.warning(f"unexpected metadata URI scheme; drain detection disabled: {uri!r}")
            return {}
        try:
            request = urllib.request.Request(uri + "/task")
            with urllib.request.urlopen(request, timeout=self.METADATA_TIMEOUT_SECONDS) as response:  # nosec B310: scheme validated above
                data = json.loads(response.read())
            family = data.get("Family")
            raw_revision = data.get("Revision")
            cluster = data.get("Cluster")
            if not family or raw_revision is None:
                logger.warning(f"task metadata has no Family/Revision; drain detection disabled: {data!r}")
                return {}
            return {"cluster": cluster, "family": family, "revision": int(raw_revision)}
        except (urllib.error.URLError, OSError, ValueError) as e:
            logger.warning(f"failed to fetch ECS task metadata; drain detection stays off until a retry succeeds: {e}")
            return None

    def _query_is_draining(self, self_revision: int) -> bool:
        try:
            running = self._running_revisions()
        except Exception as e:
            # Fail open on throttling, permissions, network failures, or malformed responses.
            logger.warning(f"drain check (ECS) failed; continuing: {e}")
            return False
        if any(revision > self_revision for revision in running):
            logger.info(f"draining: a newer generation is running family={self._family} self_revision={self_revision} running={sorted(set(running))}")
            return True
        return False

    def _running_revisions(self) -> list[int]:
        task_arns = self._list_running_task_arns()
        revisions: list[int] = []
        # DescribeTasks accepts at most 100 tasks per request.
        for start in range(0, len(task_arns), 100):
            chunk = task_arns[start : start + 100]
            describe_params: dict[str, Any] = {"tasks": chunk}
            if self._cluster:
                describe_params["cluster"] = self._cluster
            described = self._ecs.describe_tasks(**describe_params)
            for task in described.get("tasks", []):
                # Only active tasks count so polling resumes if the new generation stops.
                if task.get("lastStatus") != "RUNNING":
                    continue
                arn = task.get("taskDefinitionArn")
                if not arn:
                    continue
                try:
                    tail = arn.rsplit("/", 1)[-1]
                    family, _, revision = tail.rpartition(":")
                    revision = int(revision)
                except ValueError:
                    continue
                if family == self._family:
                    revisions.append(revision)
        return revisions

    def _list_running_task_arns(self) -> list[str]:
        task_arns: list[str] = []
        next_token = None
        for _ in range(self.MAX_LIST_TASKS_PAGES):
            params: dict[str, Any] = {"family": self._family, "desiredStatus": "RUNNING"}
            if self._cluster:
                params["cluster"] = self._cluster
            if next_token:
                params["nextToken"] = next_token
            response = self._ecs.list_tasks(**params)
            task_arns.extend(response.get("taskArns", []))
            next_token = response.get("nextToken")
            if not next_token:
                break
        return task_arns
