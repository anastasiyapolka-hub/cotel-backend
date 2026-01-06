# jobs/notifications_runner.py
import asyncio
from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.exc import SQLAlchemyError

from db.session import AsyncSessionLocal  # :contentReference[oaicite:9]{index=9}
from models import BotUserLink, MatchEvent, Subscription
from main import build_tg_message_link, bot_send_message  # предполагаю, что bot_send_message у тебя в main.py


BATCH_LIMIT = 200  # как в эндпоинте :contentReference[oaicite:10]{index=10}


async def run_tick() -> int:
    async with AsyncSessionLocal() as db:
        try:
            # 1) куда слать (MVP: первый живой линк) :contentReference[oaicite:11]{index=11}
            r = await db.execute(
                select(BotUserLink)
                .where(BotUserLink.is_blocked == False)  # noqa: E712
                .order_by(BotUserLink.id.desc())
            )
            link = r.scalars().first()
            if not link:
                print("[notifications_runner] NO_BOT_USER_LINK")
                return 0

            dest_chat_id = link.telegram_chat_id

            # 2) queued события + subscription :contentReference[oaicite:12]{index=12}
            r2 = await db.execute(
                select(MatchEvent, Subscription)
                .join(Subscription, Subscription.id == MatchEvent.subscription_id)
                .where(MatchEvent.notify_status == "queued")
                .order_by(MatchEvent.subscription_id.asc(), MatchEvent.id.asc())
                .limit(BATCH_LIMIT)
            )
            rows = list(r2.all())

            if not rows:
                print("[notifications_runner] No queued events")
                return 0

            # 3) группировка по subscription_id :contentReference[oaicite:13]{index=13}
            grouped = {}
            for ev, sub in rows:
                sid = int(ev.subscription_id)
                grouped.setdefault(sid, {"sub": sub, "events": []})
                grouped[sid]["events"].append(ev)

            # 4) одна отправка на подписку :contentReference[oaicite:14]{index=14}
            for sid, pack in grouped.items():
                sub = pack["sub"]
                events = pack["events"]

                try:
                    max_items = 10
                    shown = events[:max_items]
                    rest = len(events) - len(shown)

                    header = (
                        f"Найдены события по подписке: {sub.name or f'#{sid}'}\n"
                        f"Совпадений: {len(events)}\n"
                    )

                    lines = []
                    for i, ev in enumerate(shown, start=1):
                        author = ev.author_display or (str(ev.author_id) if ev.author_id else "—")
                        ts = ev.message_ts.isoformat() if ev.message_ts else "—"

                        excerpt = (ev.excerpt or "").strip()
                        if len(excerpt) > 300:
                            excerpt = excerpt[:300].rstrip() + "…"

                        url = build_tg_message_link(
                            chat_ref=getattr(sub, "chat_ref", None),
                            chat_id=getattr(sub, "chat_id", None),
                            message_id=int(ev.message_id),
                        )
                        link_text = f"\n{url}" if url else ""

                        lines.append(f"\n{i}) {author} • {ts}\n{excerpt or '—'}{link_text}")

                    footer = f"\n\nЕщё {rest} (свернуто)." if rest > 0 else ""
                    text = header + "".join(lines) + footer

                    ok = await bot_send_message(dest_chat_id=dest_chat_id, text=text)
                    if ok:
                        # помечаем все событи я группы sent
                        ids = [int(e.id) for e in events]
                        await db.execute(
                            update(MatchEvent).where(MatchEvent.id.in_(ids)).values(notify_status="sent")
                        )
                        await db.commit()
                        print(f"[notifications_runner] SENT subscription_id={sid} events={len(events)}")
                    else:
                        ids = [int(e.id) for e in events]
                        await db.execute(
                            update(MatchEvent).where(MatchEvent.id.in_(ids)).values(notify_status="failed")
                        )
                        await db.commit()
                        print(f"[notifications_runner] FAILED_SEND subscription_id={sid} events={len(events)}")

                except Exception as e:
                    # не валим весь тик
                    print(f"[notifications_runner] ERROR subscription_id={sid} err={e}")

            return 0

        except SQLAlchemyError as e:
            print(f"[notifications_runner] DB_ERROR: {e}")
            return 2


def main():
    code = asyncio.run(run_tick())
    raise SystemExit(code)


if __name__ == "__main__":
    main()
