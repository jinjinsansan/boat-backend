from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os
import logging

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# V2 API imports
from api.v2.health import router as v2_health_router
from api.v2.points import router as v2_points_router
from api.v2.chat import router as v2_chat_router
from api.v2.column import router as v2_column_router
from api.v2.line import router as v2_line_router

app = FastAPI(title="D-Logic Boat API", version="2.0.0")

# CORS設定
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "").split(",") if os.getenv("CORS_ORIGINS") else []

DEFAULT_ORIGINS = [
    "http://localhost:3000",
    "https://boat.dlogicai.in",
    "https://boat-wdxs.onrender.com",  # 一時的に許可（後で削除）
]

allowed_origins = DEFAULT_ORIGINS + CORS_ORIGINS

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*", "X-User-Email", "Authorization"],
)

# V2 APIルーター登録
app.include_router(v2_health_router)
app.include_router(v2_points_router)
app.include_router(v2_chat_router)
app.include_router(v2_column_router)
app.include_router(v2_line_router)

@app.get("/")
async def root():
    return {
        "message": "D-Logic Boat API",
        "version": "2.0.0",
        "status": "running"
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
