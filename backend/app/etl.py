"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime
from typing import Any

import httpx
from sqlmodel.ext.asyncio.session import AsyncSession

from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict[str, Any]]:
    """Fetch the lab/task catalog from the autochecker API.

    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/items
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - The response is a JSON array of objects with keys:
      lab (str), task (str | null), title (str), type ("lab" | "task")
    - Return the parsed list of dicts (filtered to lab-05 only)
    - Raise an exception if the response status is not 200
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{settings.autochecker_api_url}/api/items",
            auth=(settings.autochecker_email, settings.autochecker_password),
        )
        response.raise_for_status()
        items = response.json()  # type: ignore[no-any-return]
        # Filter to only lab-05 items
        return [item for item in items if item.get("lab") == "lab-05"]


async def fetch_logs(since: datetime | None = None) -> list[dict[str, Any]]:
    """Fetch check results from the autochecker API.

    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/logs
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - Query parameters:
      - limit=500 (fetch in batches)
      - since={iso timestamp} if provided (for incremental sync)
    - The response JSON has shape:
      {"logs": [...], "count": int, "has_more": bool}
    - Handle pagination: keep fetching while has_more is True
      - Use the submitted_at of the last log as the new "since" value
    - Return the combined list of all log dicts from all pages (filtered to lab-05 only)
    """
    all_logs: list[dict[str, Any]] = []
    current_since = since

    while True:
        params: dict[str, Any] = {"limit": 500}
        if current_since is not None:
            params["since"] = current_since.isoformat()

        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{settings.autochecker_api_url}/api/logs",
                auth=(settings.autochecker_email, settings.autochecker_password),
                params=params,
            )
            response.raise_for_status()
            data = response.json()

        logs: list[dict[str, Any]] = data["logs"]
        # Filter to only lab-05 logs
        all_logs.extend([log for log in logs if log.get("lab") == "lab-05"])

        if not data.get("has_more", False) or not logs:
            break

        # Use the last log's submitted_at as the new since value
        last_log = logs[-1]
        current_since = datetime.fromisoformat(last_log["submitted_at"])

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict[str, Any]], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database.

    - Import ItemRecord from app.models.item
    - Process labs first (items where type="lab"):
      - For each lab, check if an item with type="lab" and matching title
        already exists (SELECT)
      - If not, INSERT a new ItemRecord(type="lab", title=lab_title)
      - Build a dict mapping the lab's short ID (the "lab" field, e.g.
        "lab-01") to the lab's database record, so you can look up
        parent IDs when processing tasks
    - Then process tasks (items where type="task"):
      - Find the parent lab item using the task's "lab" field (e.g.
        "lab-01") as the key into the dict you built above
      - Check if a task with this title and parent_id already exists
      - If not, INSERT a new ItemRecord(type="task", title=task_title,
        parent_id=lab_item.id)
    - Commit after all inserts
    - Return the number of newly created items
    """
    from app.models.item import ItemRecord
    from sqlmodel import select, col

    new_count = 0
    lab_id_map: dict[str, ItemRecord] = {}

    # Process labs first
    for item_data in items:
        if item_data["type"] != "lab":
            continue

        lab_title: str = item_data["title"]
        lab_short_id: str = item_data["lab"]

        # Check if lab already exists
        statement = select(ItemRecord).where(
            col(ItemRecord.type) == "lab",
            col(ItemRecord.title) == lab_title,
        )
        result = await session.exec(statement)
        existing = result.one_or_none()

        if existing is None:
            # Create new lab item
            lab_item = ItemRecord(type="lab", title=lab_title)
            session.add(lab_item)
            await session.flush()  # Get the ID without committing
            lab_id_map[lab_short_id] = lab_item
            new_count += 1
        else:
            lab_id_map[lab_short_id] = existing

    # Process tasks
    for item_data in items:
        if item_data["type"] != "task":
            continue

        task_title: str = item_data["title"]
        lab_short_id = item_data["lab"]

        # Find the parent lab
        parent_lab = lab_id_map.get(lab_short_id)
        if parent_lab is None:
            # Parent lab not found, skip this task
            continue

        # Check if task already exists
        statement = select(ItemRecord).where(
            col(ItemRecord.type) == "task",
            col(ItemRecord.title) == task_title,
            col(ItemRecord.parent_id) == parent_lab.id,
        )
        result = await session.exec(statement)
        existing = result.one_or_none()

        if existing is None:
            # Create new task item
            task_item = ItemRecord(
                type="task", title=task_title, parent_id=parent_lab.id
            )
            session.add(task_item)
            await session.flush()
            new_count += 1

    await session.commit()
    return new_count


async def load_logs(
    logs: list[dict[str, Any]],
    items_catalog: list[dict[str, Any]],
    session: AsyncSession,
) -> int:
    """Load interaction logs into the database.

    Args:
        logs: Raw log dicts from the API (each has lab, task, student_id, etc.)
        items_catalog: Raw item dicts from fetch_items() — needed to map
            short IDs (e.g. "lab-01", "setup") to item titles stored in the DB.
        session: Database session.

    - Import Learner from app.models.learner
    - Import InteractionLog from app.models.interaction
    - Import ItemRecord from app.models.item
    - Build a lookup from (lab_short_id, task_short_id) to item title
      using items_catalog. For labs, the key is (lab, None). For tasks,
      the key is (lab, task). The value is the item's title.
    - For each log dict:
      1. Find or create a Learner by external_id (log["student_id"])
         - If creating, set student_group from log["group"]
      2. Find the matching item in the database:
         - Use the lookup to get the title for (log["lab"], log["task"])
         - Query the DB for an ItemRecord with that title
         - Skip this log if no matching item is found
      3. Check if an InteractionLog with this external_id already exists
         (for idempotent upsert — skip if it does)
      4. Create InteractionLog with:
         - external_id = log["id"]
         - learner_id = learner.id
         - item_id = item.id
         - kind = "attempt"
         - score = log["score"]
         - checks_passed = log["passed"]
         - checks_total = log["total"]
         - created_at = parsed log["submitted_at"]
    - Commit after all inserts
    - Return the number of newly created interactions
    """
    from app.models.interaction import InteractionLog
    from app.models.learner import Learner
    from app.models.item import ItemRecord
    from sqlmodel import select, col

    new_count = 0

    # Build lookup: (lab_short_id, task_short_id) -> item title
    item_title_lookup: dict[tuple[str, str | None], str] = {}
    for item_data in items_catalog:
        lab_short_id: str = item_data["lab"]
        task_short_id: str | None = item_data.get("task")
        title: str = item_data["title"]
        key = (lab_short_id, task_short_id)
        item_title_lookup[key] = title

    for log in logs:
        # 1. Find or create learner
        student_id: str = log["student_id"]
        student_group: str = log.get("group", "")

        # Query by external_id (string), not by id (integer)
        statement = select(Learner).where(col(Learner.external_id) == student_id)
        result = await session.exec(statement)
        learner = result.one_or_none()

        if learner is None:
            learner = Learner(external_id=student_id, student_group=student_group)
            session.add(learner)
            await session.flush()

        # 2. Find the matching item
        lab_short_id = log["lab"]
        task_short_id: str | None = log.get("task")
        item_key = (lab_short_id, task_short_id)
        item_title = item_title_lookup.get(item_key)

        if item_title is None:
            # No matching item found, skip this log
            continue

        # Determine item type: "lab" if task_short_id is None, else "task"
        item_type = "lab" if task_short_id is None else "task"

        # Query DB for ItemRecord with this title and type
        statement = select(ItemRecord).where(
            col(ItemRecord.title) == item_title,
            col(ItemRecord.type) == item_type,
        )
        result = await session.exec(statement)
        item = result.first()

        if item is None:
            # No matching item in DB, skip this log
            continue

        # 3. Check if InteractionLog with this external_id already exists
        log_external_id: int = log["id"]
        existing = await session.get(InteractionLog, log_external_id)
        if existing is not None:
            # Already exists, skip for idempotency
            continue

        # 4. Create InteractionLog
        # At this point, learner and item have been flushed, so their IDs are set
        assert learner.id is not None, "Learner ID should be set after flush"
        assert item.id is not None, "Item ID should be set after query or flush"

        interaction = InteractionLog(
            external_id=log_external_id,
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log.get("score"),
            checks_passed=log.get("passed"),
            checks_total=log.get("total"),
            created_at=datetime.fromisoformat(log["submitted_at"]),
        )
        session.add(interaction)
        new_count += 1

    await session.commit()
    return new_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict[str, int]:
    """Run the full ETL pipeline.

    - Step 1: Fetch items from the API (keep the raw list) and load them
      into the database
    - Step 2: Determine the last synced timestamp
      - Query the most recent created_at from InteractionLog
      - If no records exist, since=None (fetch everything)
    - Step 3: Fetch logs since that timestamp and load them
      - Pass the raw items list to load_logs so it can map short IDs
        to titles
    - Return a dict: {"new_records": <number of new interactions>,
                      "total_records": <total interactions in DB>}
    """
    from app.models.interaction import InteractionLog
    from sqlmodel import select, col

    # Step 1: Fetch and load items
    items = await fetch_items()
    await load_items(items, session)

    # Step 2: Determine the last synced timestamp
    statement = select(InteractionLog).order_by(col(InteractionLog.created_at).desc())
    result = await session.exec(statement)
    last_interaction = result.first()

    since = last_interaction.created_at if last_interaction else None

    # Step 3: Fetch and load logs
    logs = await fetch_logs(since=since)
    new_records = await load_logs(logs, items, session)

    # Get total records count
    total_statement = select(InteractionLog)
    total_result = await session.exec(total_statement)
    total_records = len(list(total_result.all()))

    return {"new_records": new_records, "total_records": total_records}
