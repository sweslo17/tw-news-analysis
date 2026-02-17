"""Result store service for pipeline."""

from sqlalchemy.orm import Session

from app.models import (
    ArticleFilterResult,
    ArticleAnalysisResult,
    PipelineRun,
    PipelineRunStatus,
    PipelineStage,
    FilterDecision,
)


class ResultStoreService:
    """Service for storing pipeline results to database."""

    def __init__(self, db: Session):
        self.db = db

    def save_filter_results(
        self, results: list[ArticleFilterResult], commit: bool = True
    ) -> None:
        """
        Save filter results to database.

        Args:
            results: List of filter results to save
            commit: Whether to commit the transaction
        """
        self.db.add_all(results)
        if commit:
            self.db.commit()

    def save_analysis_results(
        self, results: list[ArticleAnalysisResult], commit: bool = True
    ) -> None:
        """
        Save analysis results to database.

        Args:
            results: List of analysis results to save
            commit: Whether to commit the transaction
        """
        self.db.add_all(results)
        if commit:
            self.db.commit()

    def update_pipeline_run_stats(
        self, pipeline_run: PipelineRun, commit: bool = True
    ) -> None:
        """
        Update pipeline run statistics from filter results.

        Args:
            pipeline_run: The pipeline run to update
            commit: Whether to commit the transaction
        """
        from sqlalchemy import func

        # Count rule filter results
        rule_stats = (
            self.db.query(
                ArticleFilterResult.decision, func.count(ArticleFilterResult.id)
            )
            .filter(
                ArticleFilterResult.pipeline_run_id == pipeline_run.id,
                ArticleFilterResult.stage == PipelineStage.RULE_FILTER,
            )
            .group_by(ArticleFilterResult.decision)
            .all()
        )

        # Reset counts before recalculating
        pipeline_run.rule_filtered_count = 0
        pipeline_run.rule_passed_count = 0

        for decision, count in rule_stats:
            if decision == FilterDecision.FILTER:
                pipeline_run.rule_filtered_count = count
            elif decision in (FilterDecision.KEEP, FilterDecision.FORCE_INCLUDE):
                pipeline_run.rule_passed_count = count

        # Count force-included articles
        force_include_count = (
            self.db.query(func.count(ArticleFilterResult.id))
            .filter(
                ArticleFilterResult.pipeline_run_id == pipeline_run.id,
                ArticleFilterResult.decision == FilterDecision.FORCE_INCLUDE,
            )
            .scalar()
        )
        pipeline_run.force_included_count = force_include_count or 0

        # Count analyzed articles
        analyzed_count = (
            self.db.query(func.count(ArticleAnalysisResult.id))
            .filter(ArticleAnalysisResult.pipeline_run_id == pipeline_run.id)
            .scalar()
        )
        pipeline_run.analyzed_count = analyzed_count or 0

        if commit:
            self.db.commit()

    def update_pipeline_run_status(
        self,
        pipeline_run: PipelineRun,
        status: PipelineRunStatus,
        current_stage: PipelineStage | None = None,
        error_log: str | None = None,
        commit: bool = True,
    ) -> None:
        """
        Update pipeline run status.

        Args:
            pipeline_run: The pipeline run to update
            status: New status
            current_stage: Current stage (optional)
            error_log: Error message (optional)
            commit: Whether to commit the transaction
        """
        from datetime import datetime

        pipeline_run.status = status
        if current_stage is not None:
            pipeline_run.current_stage = current_stage

        if error_log is not None:
            pipeline_run.error_log = error_log

        if status == PipelineRunStatus.RUNNING and pipeline_run.started_at is None:
            pipeline_run.started_at = datetime.utcnow()

        if status in (PipelineRunStatus.COMPLETED, PipelineRunStatus.FAILED):
            pipeline_run.completed_at = datetime.utcnow()

        if commit:
            self.db.commit()

    def delete_filter_results_from_stage(
        self,
        pipeline_run_id: int,
        stage: PipelineStage,
        commit: bool = True,
    ) -> int:
        """
        Delete filter results from a specific stage (for reset functionality).

        Args:
            pipeline_run_id: Pipeline run ID
            stage: Stage to delete from
            commit: Whether to commit the transaction

        Returns:
            Number of deleted records
        """
        deleted = (
            self.db.query(ArticleFilterResult)
            .filter(
                ArticleFilterResult.pipeline_run_id == pipeline_run_id,
                ArticleFilterResult.stage == stage,
            )
            .delete()
        )

        if commit:
            self.db.commit()

        return deleted

    def delete_analysis_results(
        self, pipeline_run_id: int, commit: bool = True
    ) -> int:
        """
        Delete analysis results for a pipeline run.

        Args:
            pipeline_run_id: Pipeline run ID
            commit: Whether to commit the transaction

        Returns:
            Number of deleted records
        """
        deleted = (
            self.db.query(ArticleAnalysisResult)
            .filter(ArticleAnalysisResult.pipeline_run_id == pipeline_run_id)
            .delete()
        )

        if commit:
            self.db.commit()

        return deleted
