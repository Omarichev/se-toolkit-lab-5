"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.models.interaction import InteractionLog

router = APIRouter()


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab."""
    # Parse lab identifier: "lab-04" → "Lab 04"
    lab_title = lab.replace("-", " ").title()

    # Find the lab item
    result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.ilike(f"%{lab_title}%")
        )
    )
    lab_item = result.scalars().one_or_none()

    if not lab_item:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]

    # Store lab_id immediately to avoid any reference issues
    lab_id = lab_item.id

    # Find all task items that belong to this lab
    tasks_result = await session.exec(
        select(ItemRecord).where(ItemRecord.parent_id == lab_id)
    )
    task_ids = [t.id for t in tasks_result.scalars().all()]

    if not task_ids:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]

    # Build the bucket CASE expression
    bucket_expr = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        else_="76-100",
    )

    # Query interactions grouped by bucket
    stmt = (
        select(bucket_expr.label("bucket"), func.count().label("count"))
        .select_from(InteractionLog)
        .where(
            InteractionLog.item_id.in_(task_ids),
            InteractionLog.score.isnot(None)
        )
        .group_by(bucket_expr)
    )

    result = await session.exec(stmt)
    bucket_counts = {row.bucket: row.count for row in result.all()}

    # Return all four buckets, even if count is 0
    return [
        {"bucket": "0-25", "count": bucket_counts.get("0-25", 0)},
        {"bucket": "26-50", "count": bucket_counts.get("26-50", 0)},
        {"bucket": "51-75", "count": bucket_counts.get("51-75", 0)},
        {"bucket": "76-100", "count": bucket_counts.get("76-100", 0)},
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab."""
    # Parse lab identifier: "lab-04" → "Lab 04"
    lab_title = lab.replace("-", " ").title()

    # Find the lab item
    result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.ilike(f"%{lab_title}%")
        )
    )
    lab_item = result.scalars().one_or_none()

    if not lab_item:
        return []

    # Store lab_id immediately
    lab_id = lab_item.id

    # Find all task items that belong to this lab
    tasks_result = await session.exec(
        select(ItemRecord).where(ItemRecord.parent_id == lab_id).order_by(ItemRecord.title)
    )
    tasks = tasks_result.scalars().all()

    result_list = []
    for task in tasks:
        # Get interactions for this task
        stmt = (
            select(func.avg(InteractionLog.score).label("avg_score"), func.count().label("attempts"))
            .select_from(InteractionLog)
            .where(InteractionLog.item_id == task.id)
        )
        row_result = await session.exec(stmt)
        row = row_result.one_or_none()

        if row and row.attempts > 0:
            avg_score = round(row.avg_score, 1) if row.avg_score is not None else 0.0
            result_list.append({
                "task": task.title,
                "avg_score": avg_score,
                "attempts": row.attempts,
            })

    return result_list


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab."""
    # Parse lab identifier: "lab-04" → "Lab 04"
    lab_title = lab.replace("-", " ").title()

    # Find the lab item
    result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.ilike(f"%{lab_title}%")
        )
    )
    lab_item = result.scalars().one_or_none()

    if not lab_item:
        return []

    # Store lab_id immediately
    lab_id = lab_item.id

    # Find all task items that belong to this lab
    tasks_result = await session.exec(
        select(ItemRecord).where(ItemRecord.parent_id == lab_id)
    )
    task_ids = [t.id for t in tasks_result.scalars().all()]

    if not task_ids:
        return []

    # Group interactions by date
    stmt = (
        select(
            func.date(InteractionLog.created_at).label("date"),
            func.count().label("submissions")
        )
        .select_from(InteractionLog)
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(func.date(InteractionLog.created_at))
        .order_by(func.date(InteractionLog.created_at))
    )

    result = await session.exec(stmt)

    return [
        {"date": str(row.date), "submissions": row.submissions}
        for row in result.all()
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab."""
    # Parse lab identifier: "lab-04" → "Lab 04"
    lab_title = lab.replace("-", " ").title()

    # Find the lab item
    result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.ilike(f"%{lab_title}%")
        )
    )
    lab_item = result.scalars().one_or_none()

    if not lab_item:
        return []

    # Store lab_id immediately
    lab_id = lab_item.id

    # Find all task items that belong to this lab
    tasks_result = await session.exec(
        select(ItemRecord).where(ItemRecord.parent_id == lab_id)
    )
    task_ids = [t.id for t in tasks_result.scalars().all()]

    if not task_ids:
        return []

    # Join interactions with learners and group by student_group
    stmt = (
        select(
            Learner.student_group.label("group"),
            func.avg(InteractionLog.score).label("avg_score"),
            func.count(func.distinct(Learner.id)).label("students")
        )
        .select_from(InteractionLog)
        .join(Learner, InteractionLog.learner_id == Learner.id)
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )

    result = await session.exec(stmt)

    return [
        {
            "group": row.group,
            "avg_score": round(row.avg_score, 1) if row.avg_score is not None else 0.0,
            "students": row.students,
        }
        for row in result.all()
    ]
