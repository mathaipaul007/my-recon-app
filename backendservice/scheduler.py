import logging
import threading
from dataclasses import dataclass, field
from typing import Callable, Any, Dict, Optional

from contextlib import asynccontextmanager
from starlette.applications import Starlette
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from backendservice.ProcessFile import ProcessFile
logger = logging.getLogger("job_framework")
logging.basicConfig(level=logging.INFO)

# -------------------------------------
# Job definition
# -------------------------------------
@dataclass
class JobConfig:
    id: str
    func: Callable[..., Any]
    trigger: str = "cron"
    trigger_args: Dict[str, Any] = field(default_factory=dict)

    # APScheduler controls
    max_instances: int = 1
    coalesce: bool = True
    misfire_grace_time: int = 300
    replace_existing: bool = True

    # Framework controls
    use_lock: bool = True
    enabled: bool = True
    kwargs: Dict[str, Any] = field(default_factory=dict)

# -------------------------------------
# Wrapper for safe execution
# -------------------------------------
class JobWrapper:
    """
    Wraps each job with:
    - per-job non-blocking lock
    - structured logging
    - exception safety
    """

    def __init__(self, job_id: str, func: Callable[..., Any], use_lock: bool = True):
        self.job_id = job_id
        self.func = func
        self.use_lock = use_lock
        self._lock = threading.Lock()

    def __call__(self, *args, **kwargs):
        acquired = True

        if self.use_lock:
            acquired = self._lock.acquire(blocking=False)
            if not acquired:
                logger.warning("Job '%s' is already running; skipping", self.job_id)
                return

        try:
            logger.info("Starting job '%s'", self.job_id)
            self.func(*args, **kwargs)
            logger.info("Finished job '%s'", self.job_id)
        except Exception:
            logger.exception("Job '%s' failed", self.job_id)
            raise
        finally:
            if self.use_lock and acquired:
                self._lock.release()
# -------------------------------------
# Scheduler manager
# -------------------------------------
class SchedulerManager:
    def __init__(self, max_worker_threads: int = 10):
        self.scheduler = BackgroundScheduler(
            executors={
                "default": ThreadPoolExecutor(max_worker_threads)
            },
            job_defaults={
                "coalesce": True,
                "max_instances": 1,
                "misfire_grace_time": 300,
            },
            timezone="Asia/Kolkata", # optional
        )
        self._wrappers: Dict[str, JobWrapper] = {}
        self._started = False

        self.scheduler.add_listener(
            self._job_listener,
            EVENT_JOB_EXECUTED | EVENT_JOB_ERROR,
        )

    def _job_listener(self, event):
        if event.exception:
            logger.error("APScheduler reports job '%s' crashed", event.job_id)
        else:
            logger.info("APScheduler reports job '%s' executed successfully", event.job_id)

    def register_job(self, config: JobConfig):
        if not config.enabled:
            logger.info("Job '%s' is disabled; skipping registration", config.id)
            return

        wrapper = JobWrapper(
            job_id=config.id,
            func=config.func,
            use_lock=config.use_lock,
        )
        self._wrappers[config.id] = wrapper

        self.scheduler.add_job(
            func=wrapper,
            trigger=config.trigger,
            id=config.id,
            replace_existing=config.replace_existing,
            max_instances=config.max_instances,
            coalesce=config.coalesce,
            misfire_grace_time=config.misfire_grace_time,
            kwargs=config.kwargs,
            **config.trigger_args,
        )
        logger.info("Registered job '%s'", config.id)

    def start(self):
        if not self.scheduler.running:
            self.scheduler.start()
            self._started = True
            logger.info("Scheduler started")

    def shutdown(self, wait: bool = False):
        if self.scheduler.running:
            self.scheduler.shutdown(wait=wait)
            self._started = False
            logger.info("Scheduler stopped")

    def pause_job(self, job_id: str):
        self.scheduler.pause_job(job_id)
        logger.info("Paused job '%s'", job_id)

    def resume_job(self, job_id: str):
        self.scheduler.resume_job(job_id)
        logger.info("Resumed job '%s'", job_id)

    def remove_job(self, job_id: str):
        self.scheduler.remove_job(job_id)
        logger.info("Removed job '%s'", job_id)
    

    def list_jobs(self):
        return self.scheduler.get_jobs()

# -------------------------------------
# Example jobs
# -------------------------------------
def daily_cleanup():
    logger.info("Running daily cleanup...")
    # long-running cleanup logic here

def sync_reports():
    logger.info("Running reports sync...")
    # long-running sync logic here

def heartbeat(clientId):
    logger.info("Heartbeat job running... %s", clientId)
    # quick status logic here

def FileUploadChecker(clientId):
    ProcessFile(clientId)

schedule_function = {
    "FileUploadChecker" : FileUploadChecker
}   