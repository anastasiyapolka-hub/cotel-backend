from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
import json

app = FastAPI()

# Для MVP можно открыть CORS для всех, потом сузим
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],       # позже сюда подставим URL фронта
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
    params: str | None = Form(None),  # на будущее, пока можно не использовать
):
    # 1. читаем файл в память
    raw_bytes = await file.read()

    # 2. пробуем распарсить JSON
    try:
        chat_json = json.loads(raw_bytes)
    except Exception as e:
        return {"error": f"Не удалось прочитать JSON: {e}"}

    # 3. простая заглушка: посчитаем количество сообщений
    messages_count = 0
    if isinstance(chat_json, dict) and "messages" in chat_json:
        messages_count = len(chat_json["messages"])

    # 4. вернём результат фронту
    return {
        "status": "ok",
        "filename": file.filename,
        "messages_count": messages_count,
        "note": "Файл получен и распарсен. Аналитика LLM будет добавлена позже.",
    }
