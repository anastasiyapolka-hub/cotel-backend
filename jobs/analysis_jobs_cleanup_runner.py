# jobs/analysis_jobs_cleanup_runner.py
"""
Ночная очистка таблицы `analysis_jobs` (async-задачи анализа чатов).

Зачем: завершённые задачи (`done`/`error`) копятся бесконечно — таблицу нужно
периодически чистить. Заодно подстраховываем C1: помечаем `error` задачи,
застрявшие в `running` дольше порога (обычно прерванные деплоем), на случай если
приложение давно не перезапускалось и стартовый сборщик не сработал.

Запуск (Render cron):
  python -m jobs.analysis_jobs_cleanup_runner

Cron expression: `30 3 * * *` — в 03:30 UTC каждую ночь.

Идемпотентно: повторный запуск ничего не ломает (DELETE/UPDATE по условию).
Параметры порогов берутся из env, дефолты безопасные.
"""
import asyncio
import os
from datetime import datetime, timezone, timedelta

from sqlalchemy import delete, update
from sqlalchemy.exc import SQLAlchemyError

from db.session import AsyncSessionLocal
from db.models import AnalysisJob

# Завершённые задачи старше этого срока — удаляем.
CLEANUP_DAYS = int(os.getenv("ANALYSIS_JOBS_CLEANUP_DAYS", "1"))
# Задачи в running дольше этого — считаем зависшими (как в C1).
STUCK_MINUTES = int(os.getenv("ANALYSIS_JOBS_STUCK_MINUTES", "30"))


async def run_tick() -> int:
    """Главный entry-point. 0 — ОК, 2 — фатальный сбой БД."""
    now_utc = datetime.now(timezone.utc)
    done_cutoff = now_utc - timedelta(days=CLEANUP_DAYS)
    stuck_cutoff = now_utc - timedelta(minutes=STUCK_MINUTES)

    print(
        f"[analysis_jobs_cleanup] START now_utc={now_utc.isoformat()} "
        f"cleanup_days={CLEANUP_DAYS} stuck_minutes={STUCK_MINUTES}"
    )

    async with AsyncSessionLocal() as db:
        try:
            # 1) Зависшие running → error (подстраховка C1).
            stuck_res = await db.execute(
                update(AnalysisJob)
                .where(
                    AnalysisJob.status == "running",
                    AnalysisJob.updated_at < stuck_cutoff,
                )
                .values(
                    status="error",
                    error_code="INTERRUPTED",
                    error_message="Задача прервана (зависла дольше порога).",
                )
            )
            # 2) Завершённые (done/error) старше срока → удаляем.
            del_res = await db.execute(
                delete(AnalysisJob).where(
                    AnalysisJob.status.in_(("done", "error")),
                    AnalysisJob.updated_at < done_cutoff,
                )
            )
            await db.commit()
        except SQLAlchemyError as e:
            print(f"[analysis_jobs_cleanup] FATAL db_error err={e}")
            return 2

    marked = stuck_res.rowcount or 0
    deleted = del_res.rowcount or 0
    print(
        f"[analysis_jobs_cleanup] DONE marked_stuck={marked} deleted={deleted}"
    )
    return 0


def main():
    code = asyncio.run(run_tick())
    raise SystemExit(code)


if __name__ == "__main__":
    main()
