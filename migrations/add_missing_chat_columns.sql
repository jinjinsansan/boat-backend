-- =====================================================
-- v2_chat_sessions に不足しているカラムを追加
-- 実行日: 2025-10-12
-- 説明: 競馬版との互換性を保つため、必要なカラムを追加
-- =====================================================

-- user_email カラムを追加（オプション、デバッグ用）
ALTER TABLE v2_chat_sessions 
ADD COLUMN IF NOT EXISTS user_email VARCHAR(255);

-- インデックスを作成
CREATE INDEX IF NOT EXISTS idx_v2_chat_sessions_user_email 
ON v2_chat_sessions(user_email);

-- コメント追加
COMMENT ON COLUMN v2_chat_sessions.user_email IS 'ユーザーのメールアドレス（デバッグ・分析用）';

-- 完了メッセージ
DO $$
BEGIN
    RAISE NOTICE 'v2_chat_sessions カラム追加完了';
    RAISE NOTICE '追加カラム: user_email';
END $$;
