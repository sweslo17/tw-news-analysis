"""APScheduler wrapper for crawler job management."""

import threading
from datetime import datetime
from typing import Callable

from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from app.config import settings


class SchedulerManager:
    """
    Thread-safe singleton wrapper for APScheduler.

    Provides a clean interface for job management with the following features:
    - Thread-safe operations with locking
    - Singleton pattern ensures single scheduler instance
    - Support for add, remove, reschedule, and immediate run operations
    """

    _instance: "SchedulerManager | None" = None
    _lock = threading.Lock()
    _initialized: bool = False

    def __new__(cls) -> "SchedulerManager":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return

        with self._lock:
            if self._initialized:
                return

            self._scheduler = BackgroundScheduler(
                jobstores={"default": MemoryJobStore()},
                executors={"default": ThreadPoolExecutor(max_workers=10)},
                job_defaults={
                    "coalesce": True,  # Combine missed runs
                    "max_instances": 1,  # Only one instance per job
                    "misfire_grace_time": 60,
                },
                timezone=settings.scheduler_timezone,
            )
            self._initialized = True

    def start(self) -> None:
        """Start the scheduler if not running."""
        if not self._scheduler.running:
            self._scheduler.start()
            print("Scheduler started")

    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the scheduler."""
        if self._scheduler.running:
            self._scheduler.shutdown(wait=wait)
            print("Scheduler shutdown")

    @property
    def is_running(self) -> bool:
        """Check if scheduler is running."""
        return self._scheduler.running

    def add_job(
        self,
        job_id: str,
        func: Callable,
        interval_minutes: int,
        **kwargs,
    ) -> None:
        """
        Add a new interval job.

        Args:
            job_id: Unique identifier for the job.
            func: The function to execute.
            interval_minutes: Interval between executions in minutes.
            **kwargs: Additional arguments passed to add_job.
        """
        with self._lock:
            # Remove existing job if present
            if self._scheduler.get_job(job_id):
                self._scheduler.remove_job(job_id)

            self._scheduler.add_job(
                func,
                trigger=IntervalTrigger(minutes=interval_minutes),
                id=job_id,
                replace_existing=True,
                **kwargs,
            )
            print(f"Job added: {job_id} (interval: {interval_minutes}min)")

    def remove_job(self, job_id: str) -> bool:
        """
        Remove a job by ID.

        Args:
            job_id: The job ID to remove.

        Returns:
            True if job was removed, False if not found.
        """
        with self._lock:
            job = self._scheduler.get_job(job_id)
            if job:
                self._scheduler.remove_job(job_id)
                print(f"Job removed: {job_id}")
                return True
            return False

    def reschedule_job(self, job_id: str, interval_minutes: int) -> bool:
        """
        Reschedule an existing job with new interval.

        Args:
            job_id: The job ID to reschedule.
            interval_minutes: New interval in minutes.

        Returns:
            True if job was rescheduled, False if not found.
        """
        with self._lock:
            job = self._scheduler.get_job(job_id)
            if job:
                self._scheduler.reschedule_job(
                    job_id,
                    trigger=IntervalTrigger(minutes=interval_minutes),
                )
                print(f"Job rescheduled: {job_id} (new interval: {interval_minutes}min)")
                return True
            return False

    def pause_job(self, job_id: str) -> bool:
        """
        Pause a job.

        Args:
            job_id: The job ID to pause.

        Returns:
            True if job was paused, False if not found.
        """
        with self._lock:
            job = self._scheduler.get_job(job_id)
            if job:
                self._scheduler.pause_job(job_id)
                print(f"Job paused: {job_id}")
                return True
            return False

    def resume_job(self, job_id: str) -> bool:
        """
        Resume a paused job.

        Args:
            job_id: The job ID to resume.

        Returns:
            True if job was resumed, False if not found.
        """
        with self._lock:
            job = self._scheduler.get_job(job_id)
            if job:
                self._scheduler.resume_job(job_id)
                print(f"Job resumed: {job_id}")
                return True
            return False

    def get_next_run_time(self, job_id: str) -> datetime | None:
        """
        Get next scheduled run time for a job.

        Args:
            job_id: The job ID to query.

        Returns:
            Next run time or None if job not found.
        """
        job = self._scheduler.get_job(job_id)
        return job.next_run_time if job else None

    def run_job_now(self, job_id: str) -> bool:
        """
        Trigger immediate execution of a job.

        Creates a one-time job that runs immediately with the same
        function as the scheduled job.

        Args:
            job_id: The job ID to run immediately.

        Returns:
            True if job was triggered, False if not found.
        """
        with self._lock:
            job = self._scheduler.get_job(job_id)
            if job:
                # Schedule a one-time run immediately
                self._scheduler.add_job(
                    job.func,
                    id=f"{job_id}_immediate_{datetime.utcnow().timestamp()}",
                    args=job.args,
                    kwargs=job.kwargs,
                )
                print(f"Job triggered immediately: {job_id}")
                return True
            return False

    def get_all_jobs(self) -> list:
        """Get all scheduled jobs."""
        return self._scheduler.get_jobs()

    def job_exists(self, job_id: str) -> bool:
        """Check if a job exists."""
        return self._scheduler.get_job(job_id) is not None


# Global singleton instance
scheduler_manager = SchedulerManager()
