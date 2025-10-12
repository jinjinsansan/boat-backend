"""
V2 LINE統合API（boat版）
LINE OAuth、紹介、Webhook を統合
"""
from fastapi import APIRouter

router = APIRouter(prefix="/api/v2/line", tags=["v2-line"])

# 既存のLINE関連ルーターから必要なエンドポイントをインポート
from api.v2.line_oauth import router as oauth_router
from api.v2.line_referral_improved import router as referral_router
from api.v2.line_webhook import router as webhook_router

# サブルーターを統合
router.include_router(oauth_router)
router.include_router(referral_router)
router.include_router(webhook_router)
