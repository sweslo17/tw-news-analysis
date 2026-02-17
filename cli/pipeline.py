"""Pipeline CLI commands."""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from rich.panel import Panel
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.config import settings
from app.models import PipelineStage, Base
from app.services.pipeline import PipelineOrchestrator, StatisticsService

app = typer.Typer(help="News article filtering pipeline commands")
console = Console()


def get_db():
    """Get database session."""
    engine = create_engine(settings.database_url, echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()


def parse_stage(stage: str) -> PipelineStage:
    """Parse stage string to PipelineStage enum."""
    stage_map = {
        "fetch": PipelineStage.FETCH,
        "rule_filter": PipelineStage.RULE_FILTER,
        "llm_analysis": PipelineStage.LLM_ANALYSIS,
        "store": PipelineStage.STORE,
    }
    if stage.lower() not in stage_map:
        raise typer.BadParameter(
            f"Invalid stage: {stage}. Valid: {list(stage_map.keys())}"
        )
    return stage_map[stage.lower()]


def _get_date_range(
    days: int | None,
    hours: int | None,
    minutes: int | None,
    yesterday: bool,
    date: str | None,
) -> tuple[datetime | None, datetime | None, str]:
    """
    Calculate date range based on options.

    Returns:
        (date_from, date_to, name_suffix)
    """
    if yesterday:
        # Yesterday: 00:00:00 ~ 23:59:59
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        date_from = today - timedelta(days=1)
        date_to = today - timedelta(seconds=1)  # Yesterday 23:59:59
        name_suffix = f"yesterday ({date_from.strftime('%Y-%m-%d')})"
        return date_from, date_to, name_suffix

    if date:
        # Specific date: 00:00:00 ~ 23:59:59
        target_date = datetime.strptime(date, "%Y-%m-%d")
        date_from = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        date_to = target_date.replace(hour=23, minute=59, second=59, microsecond=999999)
        name_suffix = date
        return date_from, date_to, name_suffix

    if hours is not None:
        # Last N hours from now
        date_from = datetime.utcnow() - timedelta(hours=hours)
        name_suffix = f"last {hours} hour(s)"
        return date_from, None, name_suffix

    if minutes is not None:
        # Last N minutes from now
        date_from = datetime.utcnow() - timedelta(minutes=minutes)
        name_suffix = f"last {minutes} minute(s)"
        return date_from, None, name_suffix

    # Default: last N days from now
    date_from = datetime.utcnow() - timedelta(days=days or 1)
    name_suffix = f"last {days or 1} day(s)"
    return date_from, None, name_suffix


@app.command()
def quick(
    days: Optional[int] = typer.Option(
        None, "--days", "-d", help="Number of days to look back from now"
    ),
    hours: Optional[int] = typer.Option(
        None, "--hours", "-H", help="Number of hours to look back from now"
    ),
    minutes: Optional[int] = typer.Option(
        None, "--minutes", "-m", help="Number of minutes to look back from now"
    ),
    yesterday: bool = typer.Option(
        False, "--yesterday", "-y", help="Process yesterday's articles only"
    ),
    date: Optional[str] = typer.Option(
        None, "--date", help="Process specific date (YYYY-MM-DD)"
    ),
    until: str = typer.Option(
        "rule_filter", "--until", "-u", help="Run until this stage"
    ),
):
    """Quick run: Fetch articles and apply rule-based filter.

    Examples:
        quick --days 1          # Last 24 hours from now
        quick --hours 2         # Last 2 hours
        quick --minutes 60      # Last 60 minutes
        quick --yesterday       # Yesterday (00:00 ~ 23:59)
        quick --date 2025-01-20 # Specific date
    """
    # Validate options - only one time range option allowed
    options_set = sum([
        days is not None,
        hours is not None,
        minutes is not None,
        yesterday,
        date is not None,
    ])
    if options_set > 1:
        console.print("[red]Error: Use only one of --days, --hours, --minutes, --yesterday, or --date[/red]")
        raise typer.Exit(1)

    # Default to --days 1 if no option specified
    if options_set == 0:
        days = 1

    db = get_db()
    orchestrator = PipelineOrchestrator(db)
    until_stage = parse_stage(until)

    # Calculate date range
    date_from, date_to, name_suffix = _get_date_range(days, hours, minutes, yesterday, date)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        console=console,
    ) as progress:
        task = progress.add_task("Starting pipeline...", total=100)

        def update_progress(stage: str, current: int, total: int):
            if total > 0:
                pct = (current / total) * 100
                progress.update(task, description=f"[{stage}] {current}/{total}", completed=pct)
            else:
                progress.update(task, description=f"[{stage}] Counting articles...")

        run = asyncio.run(
            orchestrator.run_quick_pipeline_with_range(
                date_from=date_from,
                date_to=date_to,
                name_suffix=name_suffix,
                until_stage=until_stage,
                progress_callback=update_progress,
            )
        )

    # Show results
    stats = orchestrator.stats.get_pipeline_run_stats(run.id)
    if stats:
        _display_run_stats(stats)

    console.print(f"\n[green]Pipeline run ID: {run.id}[/green]")


@app.command()
def create(
    name: str = typer.Option(..., "--name", "-n", help="Name for this pipeline run"),
    date_from: Optional[str] = typer.Option(
        None, "--date-from", help="Start date (YYYY-MM-DD)"
    ),
    date_to: Optional[str] = typer.Option(
        None, "--date-to", help="End date (YYYY-MM-DD)"
    ),
):
    """Create a new pipeline run."""
    db = get_db()
    orchestrator = PipelineOrchestrator(db)

    # Parse dates
    from_dt = datetime.strptime(date_from, "%Y-%m-%d") if date_from else None
    to_dt = datetime.strptime(date_to, "%Y-%m-%d") if date_to else None

    run = orchestrator.create_pipeline_run(name=name, date_from=from_dt, date_to=to_dt)

    console.print(f"[green]Created pipeline run:[/green]")
    console.print(f"  ID: {run.id}")
    console.print(f"  Name: {run.name}")
    console.print(f"  Date range: {from_dt or 'All'} - {to_dt or 'Now'}")


@app.command()
def run(
    run_id: int = typer.Argument(..., help="Pipeline run ID"),
    until: str = typer.Option("store", "--until", "-u", help="Run until this stage"),
):
    """Run pipeline to specified stage."""
    db = get_db()
    orchestrator = PipelineOrchestrator(db)
    until_stage = parse_stage(until)

    pipeline_run = orchestrator.get_pipeline_run(run_id)
    if not pipeline_run:
        console.print(f"[red]Pipeline run {run_id} not found[/red]")
        raise typer.Exit(1)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        console=console,
    ) as progress:
        task = progress.add_task("Starting pipeline...", total=100)

        def update_progress(stage: str, current: int, total: int):
            if total > 0:
                pct = (current / total) * 100
                progress.update(task, description=f"[{stage}] {current}/{total}", completed=pct)
            else:
                progress.update(task, description=f"[{stage}] Processing...")

        run_result = asyncio.run(
            orchestrator.run_pipeline(
                run_id,
                until_stage=until_stage,
                progress_callback=update_progress,
            )
        )

    # Show results
    stats = orchestrator.stats.get_pipeline_run_stats(run_result.id)
    if stats:
        _display_run_stats(stats)


@app.command()
def review(
    run_id: int = typer.Argument(..., help="Pipeline run ID"),
    show_filtered: bool = typer.Option(
        False, "--show-filtered", "-f", help="Show filtered articles"
    ),
    show_passed: bool = typer.Option(
        False, "--show-passed", "-p", help="Show passed articles"
    ),
    limit: int = typer.Option(20, "--limit", "-l", help="Maximum articles to show"),
    export: Optional[str] = typer.Option(
        None, "--export", "-e", help="Export results to JSON file"
    ),
):
    """Review pipeline run results."""
    db = get_db()
    stats_service = StatisticsService(db)

    run_stats = stats_service.get_pipeline_run_stats(run_id)
    if not run_stats:
        console.print(f"[red]Pipeline run {run_id} not found[/red]")
        raise typer.Exit(1)

    _display_run_stats(run_stats)

    export_data = {"stats": vars(run_stats)}

    if show_filtered:
        console.print("\n[bold]Filtered Articles:[/bold]")
        filtered = stats_service.get_filtered_articles(run_id, limit=limit)
        export_data["filtered"] = filtered

        if filtered:
            table = Table(show_header=True, header_style="bold")
            table.add_column("ID", style="dim")
            table.add_column("Title", max_width=50)
            table.add_column("Source")
            table.add_column("Stage")
            table.add_column("Rule/Reason")

            for article in filtered:
                table.add_row(
                    str(article["article_id"]),
                    article["title"][:50],
                    article["source"],
                    article["stage"],
                    article["rule_name"] or article["reason"][:30],
                )
            console.print(table)
        else:
            console.print("[dim]No filtered articles[/dim]")

    if show_passed:
        console.print("\n[bold]Passed Articles:[/bold]")
        passed = stats_service.get_passed_articles(run_id, limit=limit)
        export_data["passed"] = passed

        if passed:
            table = Table(show_header=True, header_style="bold")
            table.add_column("ID", style="dim")
            table.add_column("Title", max_width=50)
            table.add_column("Source")
            table.add_column("Category")
            table.add_column("Decision")

            for article in passed:
                table.add_row(
                    str(article["article_id"]),
                    article["title"][:50],
                    article["source"],
                    article["category"] or "-",
                    article["decision"],
                )
            console.print(table)
        else:
            console.print("[dim]No passed articles[/dim]")

    if export:
        with open(export, "w", encoding="utf-8") as f:
            json.dump(export_data, f, ensure_ascii=False, indent=2, default=str)
        console.print(f"\n[green]Results exported to {export}[/green]")


@app.command()
def stats(
    run_id: Optional[int] = typer.Argument(None, help="Pipeline run ID (optional)"),
):
    """Show pipeline statistics."""
    db = get_db()
    stats_service = StatisticsService(db)

    if run_id:
        run_stats = stats_service.get_pipeline_run_stats(run_id)
        if not run_stats:
            console.print(f"[red]Pipeline run {run_id} not found[/red]")
            raise typer.Exit(1)
        _display_run_stats(run_stats)
    else:
        # Show overall stats
        overall = stats_service.get_overall_stats()

        console.print(Panel("[bold]Overall Pipeline Statistics[/bold]"))

        table = Table(show_header=False)
        table.add_column("Metric", style="bold")
        table.add_column("Value")

        table.add_row("Total Runs", str(overall.total_runs))
        table.add_row("Completed Runs", str(overall.completed_runs))
        table.add_row("Total Articles Processed", f"{overall.total_articles_processed:,}")
        table.add_row("Total Rule Filtered", f"{overall.total_rule_filtered:,}")
        table.add_row("Total Analyzed", f"{overall.total_analyzed:,}")
        table.add_row("Avg Rule Filter Rate", f"{overall.avg_rule_filter_rate}%")

        console.print(table)

        # Show recent runs
        console.print("\n[bold]Recent Runs:[/bold]")
        recent = stats_service.get_recent_runs(limit=5)
        if recent:
            runs_table = Table(show_header=True, header_style="bold")
            runs_table.add_column("ID", style="dim")
            runs_table.add_column("Name")
            runs_table.add_column("Status")
            runs_table.add_column("Articles")
            runs_table.add_column("Filtered")
            runs_table.add_column("Created")

            for r in recent:
                status_color = {
                    "completed": "green",
                    "running": "yellow",
                    "failed": "red",
                    "pending": "dim",
                    "paused": "blue",
                }.get(r["status"], "white")

                runs_table.add_row(
                    str(r["id"]),
                    r["name"][:30],
                    f"[{status_color}]{r['status']}[/{status_color}]",
                    str(r["total_articles"]),
                    str(r["rule_filtered"]),
                    r["created_at"][:16],
                )
            console.print(runs_table)

        # Show rule stats
        console.print("\n[bold]Rule Statistics:[/bold]")
        rules = stats_service.get_rule_stats()
        if rules:
            rules_table = Table(show_header=True, header_style="bold")
            rules_table.add_column("Rule Name")
            rules_table.add_column("Type")
            rules_table.add_column("Active")
            rules_table.add_column("Total Filtered")

            for rule in rules:
                active = "[green]Yes[/green]" if rule.is_active else "[red]No[/red]"
                rules_table.add_row(
                    rule.rule_name,
                    rule.rule_type,
                    active,
                    f"{rule.total_filtered_count:,}",
                )
            console.print(rules_table)


@app.command(name="force-include")
def force_include(
    article_id: int = typer.Option(..., "--article-id", "-a", help="Article ID to force include"),
    reason: str = typer.Option(..., "--reason", "-r", help="Reason for force including"),
    user: Optional[str] = typer.Option(None, "--user", "-u", help="User adding this entry"),
):
    """Force include an article in future pipeline runs."""
    db = get_db()
    orchestrator = PipelineOrchestrator(db)

    try:
        entry = orchestrator.add_force_include(article_id, reason, user)
        console.print(f"[green]Article {article_id} added to force-include list[/green]")
        console.print(f"  Reason: {reason}")
    except ValueError as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)


@app.command(name="list-force-includes")
def list_force_includes():
    """List all force-included articles."""
    db = get_db()
    orchestrator = PipelineOrchestrator(db)

    entries = orchestrator.list_force_includes()

    if not entries:
        console.print("[dim]No force-included articles[/dim]")
        return

    table = Table(show_header=True, header_style="bold")
    table.add_column("Article ID", style="dim")
    table.add_column("Title", max_width=40)
    table.add_column("Source")
    table.add_column("Reason")
    table.add_column("Added By")
    table.add_column("Created")

    for entry in entries:
        table.add_row(
            str(entry["article_id"]),
            entry["title"][:40],
            entry["source"],
            entry["reason"][:30],
            entry["added_by"] or "-",
            entry["created_at"][:10],
        )

    console.print(table)


@app.command(name="remove-force-include")
def remove_force_include(
    article_id: int = typer.Option(..., "--article-id", "-a", help="Article ID to remove"),
):
    """Remove an article from force-include list."""
    db = get_db()
    orchestrator = PipelineOrchestrator(db)

    if orchestrator.remove_force_include(article_id):
        console.print(f"[green]Article {article_id} removed from force-include list[/green]")
    else:
        console.print(f"[yellow]Article {article_id} was not in force-include list[/yellow]")


@app.command()
def reset(
    run_id: int = typer.Argument(..., help="Pipeline run ID"),
    from_stage: str = typer.Option(
        "rule_filter", "--from-stage", "-s", help="Stage to reset from"
    ),
):
    """Reset pipeline run to re-execute from a specific stage."""
    db = get_db()
    orchestrator = PipelineOrchestrator(db)
    stage = parse_stage(from_stage)

    try:
        run = orchestrator.reset_pipeline_run(run_id, stage)
        console.print(f"[green]Pipeline run {run_id} reset from stage '{from_stage}'[/green]")
        console.print(f"  Status: {run.status.value}")
    except ValueError as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def providers():
    """List available LLM providers."""
    from app.services.pipeline.llm_filter_service import PROVIDERS

    console.print("[bold]Available LLM Providers:[/bold]")
    for p in PROVIDERS:
        default = " (default)" if p == settings.default_llm_provider else ""
        console.print(f"  • {p}{default}")


def _display_run_stats(stats):
    """Display pipeline run statistics."""
    console.print(Panel(f"[bold]Pipeline Run: {stats.name}[/bold]"))

    table = Table(show_header=False)
    table.add_column("Metric", style="bold")
    table.add_column("Value")

    table.add_row("Run ID", str(stats.run_id))
    table.add_row("Status", stats.status)
    table.add_row("Total Articles", f"{stats.total_articles:,}")
    table.add_row("Rule Filtered", f"{stats.rule_filtered_count:,} ({stats.rule_filter_rate}%)")
    table.add_row("Rule Passed", f"{stats.rule_passed_count:,}")
    table.add_row("Force Included", f"{stats.force_included_count:,}")

    if stats.duration_seconds:
        table.add_row("Duration", f"{stats.duration_seconds}s")

    console.print(table)


if __name__ == "__main__":
    app()
