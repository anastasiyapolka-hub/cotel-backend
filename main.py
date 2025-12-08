from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import json

app = FastAPI()

# CORS (пока открытый, потом ограничим доменом фронта)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/analyze")
async def analyze_chat(
    file: UploadFile = File(...),
    params: str | None = Form(None),
):
    # 1. Проверяем расширение файла
    if not file.filename.lower().endswith(".json"):
        raise HTTPException(
            status_code=400,
            detail="Ожидается JSON-файл экспорта Telegram (.json)",
        )

    # 2. Читаем файл в память
    raw_bytes = await file.read()

    # 3. Пробуем распарсить JSON
    try:
        data = json.loads(raw_bytes)
    except json.JSONDecodeError:
        raise HTTPException(
            status_code=400,
            detail="Ошибка: Файл не является корректным JSON."
        )

    # 4. Проверка структуры Telegram экспорта (опционально)
    messages = data.get("messages")
    if messages is None:
        # необязательная проверка, но полезно
        raise HTTPException(
            status_code=400,
            detail="JSON не содержит поле 'messages'. Возможно, экспорт выполнен в HTML-формате."
        )

    # 📌 Извлекаем имя чата (если есть)
    chat_name = data.get("name") or data.get("title") or "Без названия"

    if not isinstance(messages, list):
        raise HTTPException(
            status_code=400,
            detail="Поле 'messages' должно быть списком сообщений"
        )

    # 5. Количество сообщений
    messages_count = len(messages)

    # 6. Ответ фронту
    return {
        "status": "ok",
        "message": "Файл успешно загружен",
        "filename": file.filename,
        "messages_count": messages_count,
        "chat_name": chat_name,  # <--- ВОТ ЭТО
        "note": "Файл принят. Анализ LLM добавим позже."
    }
