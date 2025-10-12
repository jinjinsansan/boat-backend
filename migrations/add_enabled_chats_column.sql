-- =====================================================
-- enabled_chats カラムを追加
-- 実行日: 2025-10-12
-- 説明: 複数AIタイプ（D-Logic、I-Logic、ViewLogicなど）の有効化管理
-- =====================================================

-- v2_chat_sessionsテーブルに enabled_chats カラムを追加
ALTER TABLE v2_chat_sessions 
ADD COLUMN IF NOT EXISTS enabled_chats JSONB DEFAULT '{"imlogic": true, "viewlogic": true}'::jsonb;

-- imlogic_settings_id カラムも追加（まだ存在しない場合）
ALTER TABLE v2_chat_sessions 
ADD COLUMN IF NOT EXISTS imlogic_settings_id UUID;

-- race_snapshot カラムも追加（まだ存在しない場合）
ALTER TABLE v2_chat_sessions 
ADD COLUMN IF NOT EXISTS race_snapshot JSONB;

-- コメント追加
COMMENT ON COLUMN v2_chat_sessions.enabled_chats IS '有効化されたAIタイプ（imlogic, dlogic, ilogic, viewlogic, metalogic）';
COMMENT ON COLUMN v2_chat_sessions.imlogic_settings_id IS 'IMLogic設定ID（オプション）';
COMMENT ON COLUMN v2_chat_sessions.race_snapshot IS 'レースデータのスナップショット（JSON）';

-- 完了メッセージ
DO $$
BEGIN
    RAISE NOTICE 'enabled_chats カラム追加完了';
    RAISE NOTICE 'デフォルト値: {"imlogic": true, "viewlogic": true}';
END $$;
