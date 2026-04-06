from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from telethon import functions, types
from telethon.tl.types import Message

from db.session import get_db
from service_account_service import (
    ServiceAccountError,
    load_service_session_string,
    get_service_tg_client,
    normalize_public_chat_ref,
    resolve_service_entity,
    ensure_join_and_access,
)

router = APIRouter(prefix="/admin/service-accounts", tags=["admin-service-accounts"])


class ServiceAccountChatDiagnosticIn(BaseModel):
    service_account_id: int = Field(ge=1)
    chat_ref: str = Field(min_length=1)
    sample_limit: int = Field(default=5, ge=1, le=20)

    # --- TESTING BLOCK: reaction test ---
    reaction_emoji: str = Field(default="🔥", min_length=1, max_length=16)
    react_to_last_read: bool = Field(default=True)
    # --- /TESTING BLOCK: reaction test ---

async def _diag_read_sample(client, entity, limit: int = 5) -> list[dict]:
    rows: list[dict] = []

    async for msg in client.iter_messages(entity, limit=limit):
        if not isinstance(msg, Message):
            continue

        text = (msg.message or "").strip()

        rows.append({
            "message_id": getattr(msg, "id", None),
            "date": msg.date.isoformat() if getattr(msg, "date", None) else None,
            "text": text[:300] if text else "",
        })

    return rows


async def _diag_has_dialog(client, entity_id: int | None, limit: int = 300) -> tuple[bool, dict | None]:
    if not entity_id:
        return False, None

    dialogs = await client.get_dialogs(limit=limit)

    for d in dialogs:
        ent = d.entity
        if getattr(ent, "id", None) == entity_id:
            return True, {
                "id": getattr(ent, "id", None),
                "title": getattr(ent, "title", None),
                "username": getattr(ent, "username", None),
                "entity_type": ent.__class__.__name__,
            }

    return False, None


def _format_diag_report(payload: dict) -> str:
    lines: list[str] = []

    lines.append("=== Диагностика service account ↔ chat ===")
    lines.append(f"service_account_id: {payload.get('service_account_id')}")
    lines.append(f"chat_ref_input: {payload.get('chat_ref_input')}")
    lines.append(f"chat_ref_normalized: {payload.get('chat_ref_normalized')}")
    lines.append("")

    auth = payload.get("auth", {})
    lines.append("[1] Авторизация аккаунта")
    lines.append(f"is_user_authorized: {auth.get('is_user_authorized')}")
    lines.append(f"me.id: {auth.get('me', {}).get('id')}")
    lines.append(f"me.username: {auth.get('me', {}).get('username')}")
    lines.append(f"me.phone: {auth.get('me', {}).get('phone')}")
    lines.append("")

    sess = payload.get("session", {})
    lines.append("[2] Сессия")
    lines.append(f"loaded: {sess.get('loaded')}")
    lines.append(f"length: {sess.get('length')}")
    lines.append(f"preview: {sess.get('preview')}")
    lines.append("")

    resolve = payload.get("resolve", {})
    lines.append("[3] Resolve entity")
    lines.append(f"success: {resolve.get('success')}")
    if resolve.get("error"):
        lines.append(f"error: {resolve.get('error')}")
    if resolve.get("entity"):
        entity = resolve["entity"]
        lines.append(f"entity_type: {entity.get('entity_type')}")
        lines.append(f"id: {entity.get('id')}")
        lines.append(f"title: {entity.get('title')}")
        lines.append(f"username: {entity.get('username')}")
        lines.append(f"broadcast: {entity.get('broadcast')}")
        lines.append(f"megagroup: {entity.get('megagroup')}")
    lines.append("")

    before_dialog = payload.get("dialog_before", {})
    lines.append("[4] Чат в dialogs до join")
    lines.append(f"present: {before_dialog.get('present')}")
    if before_dialog.get("dialog"):
        lines.append(f"dialog: {before_dialog.get('dialog')}")
    lines.append("")

    before_read = payload.get("read_before_join", {})
    lines.append("[5] Чтение до join")
    lines.append(f"success: {before_read.get('success')}")
    if before_read.get("error"):
        lines.append(f"error: {before_read.get('error')}")
    lines.append(f"messages_count: {before_read.get('messages_count')}")
    for row in before_read.get("messages", []) or []:
        lines.append(f"- {row.get('message_id')} | {row.get('date')} | {row.get('text')}")
    lines.append("")

    join_step = payload.get("join", {})
    lines.append("[6] Join / ensure access")
    lines.append(f"success: {join_step.get('success')}")
    if join_step.get("error"):
        lines.append(f"error: {join_step.get('error')}")
    if join_step.get("entity"):
        lines.append(f"entity_after_join: {join_step.get('entity')}")
    lines.append("")

    after_dialog = payload.get("dialog_after", {})
    lines.append("[7] Чат в dialogs после join")
    lines.append(f"present: {after_dialog.get('present')}")
    if after_dialog.get("dialog"):
        lines.append(f"dialog: {after_dialog.get('dialog')}")
    lines.append("")

    after_read = payload.get("read_after_join", {})
    lines.append("[8] Чтение после join")
    lines.append(f"success: {after_read.get('success')}")
    if after_read.get("error"):
        lines.append(f"error: {after_read.get('error')}")
    lines.append(f"messages_count: {after_read.get('messages_count')}")
    for row in after_read.get("messages", []) or []:
        lines.append(f"- {row.get('message_id')} | {row.get('date')} | {row.get('text')}")

    reaction_test = payload.get("reaction_test", {})
    lines.append("")
    lines.append("[9] Тест реакции")
    lines.append(f"attempted: {reaction_test.get('attempted')}")
    lines.append(f"success: {reaction_test.get('success')}")
    lines.append(f"emoji: {reaction_test.get('emoji')}")
    lines.append(f"target_message_id: {reaction_test.get('target_message_id')}")
    if reaction_test.get("error"):
        lines.append(f"error: {reaction_test.get('error')}")

    return "\n".join(lines)


@router.post("/diagnostics/chat")
async def diagnose_service_account_chat(
    payload: ServiceAccountChatDiagnosticIn,
    db: AsyncSession = Depends(get_db),
):
    result: dict = {
        "service_account_id": payload.service_account_id,
        "chat_ref_input": payload.chat_ref,
        "chat_ref_normalized": None,
        "session": {},
        "auth": {},
        "resolve": {},
        "dialog_before": {},
        "read_before_join": {},
        "join": {},
        "dialog_after": {},
        "read_after_join": {},
        "reaction_test": {},  # --- TESTING BLOCK: reaction test ---
    }

    try:
        session_string = await load_service_session_string(db, payload.service_account_id)
        result["session"] = {
            "loaded": True,
            "length": len(session_string),
            "preview": f"{session_string[:20]}...{session_string[-12:]}" if len(session_string) > 40 else session_string,
        }

        client = await get_service_tg_client(db, payload.service_account_id)

        is_auth = await client.is_user_authorized()
        me = await client.get_me() if is_auth else None

        result["auth"] = {
            "is_user_authorized": is_auth,
            "me": {
                "id": getattr(me, "id", None),
                "username": getattr(me, "username", None),
                "phone": getattr(me, "phone", None),
                "first_name": getattr(me, "first_name", None),
                "last_name": getattr(me, "last_name", None),
            } if me else None,
        }

        if not is_auth:
            result["report_text"] = _format_diag_report(result)
            return result

        normalized_ref = normalize_public_chat_ref(payload.chat_ref)
        result["chat_ref_normalized"] = normalized_ref

        entity = None

        try:
            entity = await resolve_service_entity(client, normalized_ref)
            result["resolve"] = {
                "success": True,
                "entity": {
                    "entity_type": entity.__class__.__name__,
                    "id": getattr(entity, "id", None),
                    "title": getattr(entity, "title", None),
                    "username": getattr(entity, "username", None),
                    "broadcast": getattr(entity, "broadcast", None),
                    "megagroup": getattr(entity, "megagroup", None),
                },
            }
        except Exception as e:
            result["resolve"] = {
                "success": False,
                "error": f"{e.__class__.__name__}: {str(e)}",
            }
            result["report_text"] = _format_diag_report(result)
            return result

        present_before, dialog_before = await _diag_has_dialog(client, getattr(entity, "id", None))
        result["dialog_before"] = {
            "present": present_before,
            "dialog": dialog_before,
        }

        try:
            msgs_before = await _diag_read_sample(client, entity, payload.sample_limit)
            result["read_before_join"] = {
                "success": True,
                "messages_count": len(msgs_before),
                "messages": msgs_before,
            }
        except Exception as e:
            result["read_before_join"] = {
                "success": False,
                "error": f"{e.__class__.__name__}: {str(e)}",
                "messages_count": 0,
                "messages": [],
            }

        joined_entity = entity
        try:
            joined_entity = await ensure_join_and_access(client, normalized_ref, entity)
            result["join"] = {
                "success": True,
                "entity": {
                    "entity_type": joined_entity.__class__.__name__,
                    "id": getattr(joined_entity, "id", None),
                    "title": getattr(joined_entity, "title", None),
                    "username": getattr(joined_entity, "username", None),
                },
            }
        except Exception as e:
            result["join"] = {
                "success": False,
                "error": f"{e.__class__.__name__}: {str(e)}",
            }

        present_after, dialog_after = await _diag_has_dialog(client, getattr(joined_entity, "id", None))
        result["dialog_after"] = {
            "present": present_after,
            "dialog": dialog_after,
        }

        try:
            msgs_after = await _diag_read_sample(client, joined_entity, payload.sample_limit)
            result["read_after_join"] = {
                "success": True,
                "messages_count": len(msgs_after),
                "messages": msgs_after,
            }
        except Exception as e:
            result["read_after_join"] = {
                "success": False,
                "error": f"{e.__class__.__name__}: {str(e)}",
                "messages_count": 0,
                "messages": [],
            }


        # =========================================================
        # --- TESTING BLOCK: send reaction to one of read messages ---
        # ВАЖНО:
        # _diag_read_sample читает через iter_messages(limit=N),
        # а Telethon отдаёт сообщения от новых к старым.
        # Поэтому:
        #   msgs_after[0]  -> самое новое из выборки
        #   msgs_after[-1] -> самое старое из выборки
        #
        # Для теста "реакция на последнее актуальное сообщение" берём msgs_after[0].
        # Если захочешь реакцию именно на последний элемент в выведенном списке,
        # поменяй target_row = msgs_after[-1]
        # =========================================================
        result["reaction_test"] = {
            "attempted": False,
            "success": False,
            "emoji": payload.reaction_emoji,
            "target_message_id": None,
            "error": None,
        }

        try:
            msgs_after = result.get("read_after_join", {}).get("messages") or []

            if payload.react_to_last_read and msgs_after:
                target_row = msgs_after[0]   # самое новое сообщение из прочитанной выборки
                target_message_id = target_row.get("message_id")

                result["reaction_test"]["attempted"] = True
                result["reaction_test"]["target_message_id"] = target_message_id

                if target_message_id:
                    await client(
                        functions.messages.SendReactionRequest(
                            peer=joined_entity,
                            msg_id=int(target_message_id),
                            big=True,
                            add_to_recent=True,
                            reaction=[
                                types.ReactionEmoji(
                                    emoticon=payload.reaction_emoji
                                )
                            ],
                        )
                    )

                    result["reaction_test"]["success"] = True
                else:
                    result["reaction_test"]["error"] = "TARGET_MESSAGE_ID_MISSING"
            else:
                result["reaction_test"]["error"] = "NO_MESSAGES_FOR_REACTION"
        except Exception as e:
            result["reaction_test"]["success"] = False
            result["reaction_test"]["error"] = f"{e.__class__.__name__}: {str(e)}"
        # --- /TESTING BLOCK: send reaction to one of read messages ---
        # =========================================================



        result["report_text"] = _format_diag_report(result)
        return result

    except ServiceAccountError as e:
        raise HTTPException(
            status_code=e.http_status,
            detail={
                "code": e.code,
                "message": e.user_message,
            },
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "code": "SERVICE_ACCOUNT_DIAGNOSTIC_FAILED",
                "message": str(e),
            },
        )