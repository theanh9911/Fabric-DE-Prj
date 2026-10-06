"""Ngữ cảnh của một lần chạy: run_id, ngày logic (p_load_date), đường dẫn Files.

Bảng được gọi bằng tên `schema.table` trên default lakehouse (notebook gắn theo tên bằng %%configure),
nên ở đây chỉ còn đường dẫn Files (landing) — tương đối với default lakehouse.
"""

from __future__ import annotations

import datetime as dt
import uuid
from dataclasses import dataclass
from zoneinfo import ZoneInfo

from companya_de.config import PlatformSettings, load_settings


@dataclass(frozen=True)
class RunContext:
    run_id: str
    load_date: dt.date
    business_timezone: str
    files_root: str = "Files"   # tương đối với default lakehouse; test truyền thư mục tạm

    def files_path(self, *parts: str) -> str:
        return "/".join([self.files_root.rstrip("/"), *(p.strip("/") for p in parts)])

    def landing_path(self, source_system: str, entity: str, batch_id: str) -> str:
        """Files/landing/<source>/<entity>/load_date=YYYY-MM-DD/batch=<id>/"""
        return self.files_path(
            "landing", source_system, entity, f"load_date={self.load_date.isoformat()}", f"batch={batch_id}"
        )

    def batch_id(self, source_system: str, entity: str) -> str:
        return f"{source_system}_{entity}_{self.load_date:%Y%m%d}_{self.run_id}"


def new_run_context(
    p_load_date: str | None = None,
    p_run_id: str | None = None,
    *,
    settings: PlatformSettings | None = None,
    files_root: str = "Files",
    now: dt.datetime | None = None,
) -> RunContext:
    """Tham số notebook → RunContext. Không truyền p_load_date → hôm qua theo múi giờ nghiệp vụ."""
    settings = settings or load_settings()
    if p_load_date:
        load_date = dt.date.fromisoformat(p_load_date)
    else:
        now = now or dt.datetime.now(dt.UTC)
        load_date = now.astimezone(ZoneInfo(settings.business_timezone)).date() - dt.timedelta(days=1)
    run_id = p_run_id or uuid.uuid4().hex[:12]
    return RunContext(run_id=run_id, load_date=load_date, business_timezone=settings.business_timezone,
                      files_root=files_root)
