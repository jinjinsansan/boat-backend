-- =====================================================
-- 予想家部屋（オープンチャット）テーブル作成
-- 実行日: 2025-10-13
-- 説明: 競艇版オープンチャット機能のためのテーブル
-- =====================================================

-- =====================================================
-- 1. 予想家部屋マスタテーブル
-- =====================================================
CREATE TABLE IF NOT EXISTS expert_rooms (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title VARCHAR(200) NOT NULL,
    description TEXT,
    cover_image_url TEXT,
    required_points INTEGER DEFAULT 0,
    owner_user_id UUID REFERENCES v2_users(id) ON DELETE CASCADE,
    owner_display_name VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    total_members INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- インデックス
CREATE INDEX IF NOT EXISTS idx_expert_rooms_owner_user_id ON expert_rooms(owner_user_id);
CREATE INDEX IF NOT EXISTS idx_expert_rooms_is_active ON expert_rooms(is_active);
CREATE INDEX IF NOT EXISTS idx_expert_rooms_created_at ON expert_rooms(created_at);

-- =====================================================
-- 2. 入室記録テーブル
-- =====================================================
CREATE TABLE IF NOT EXISTS expert_room_members (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    room_id UUID REFERENCES expert_rooms(id) ON DELETE CASCADE,
    user_id UUID REFERENCES v2_users(id) ON DELETE CASCADE,
    points_consumed INTEGER DEFAULT 0,
    joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(room_id, user_id)
);

-- インデックス
CREATE INDEX IF NOT EXISTS idx_expert_room_members_room_id ON expert_room_members(room_id);
CREATE INDEX IF NOT EXISTS idx_expert_room_members_user_id ON expert_room_members(user_id);
CREATE INDEX IF NOT EXISTS idx_expert_room_members_joined_at ON expert_room_members(joined_at);

-- =====================================================
-- 3. チャットメッセージテーブル
-- =====================================================
CREATE TABLE IF NOT EXISTS chat_messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    room_id UUID REFERENCES expert_rooms(id) ON DELETE CASCADE,
    user_email VARCHAR(255) NOT NULL,
    display_name VARCHAR(100) NOT NULL,
    message TEXT NOT NULL,
    avatar_color VARCHAR(20) DEFAULT '#0f62fe',
    is_anonymous BOOLEAN DEFAULT FALSE,
    is_deleted BOOLEAN DEFAULT FALSE,
    reply_to_id UUID REFERENCES chat_messages(id) ON DELETE SET NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- インデックス
CREATE INDEX IF NOT EXISTS idx_chat_messages_room_id ON chat_messages(room_id);
CREATE INDEX IF NOT EXISTS idx_chat_messages_user_email ON chat_messages(user_email);
CREATE INDEX IF NOT EXISTS idx_chat_messages_created_at ON chat_messages(created_at);
CREATE INDEX IF NOT EXISTS idx_chat_messages_is_deleted ON chat_messages(is_deleted);
CREATE INDEX IF NOT EXISTS idx_chat_messages_reply_to_id ON chat_messages(reply_to_id);

-- =====================================================
-- 4. チャットリアクションテーブル
-- =====================================================
CREATE TABLE IF NOT EXISTS chat_reactions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    message_id UUID REFERENCES chat_messages(id) ON DELETE CASCADE,
    user_email VARCHAR(255) NOT NULL,
    emoji VARCHAR(10) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(message_id, user_email, emoji)
);

-- インデックス
CREATE INDEX IF NOT EXISTS idx_chat_reactions_message_id ON chat_reactions(message_id);
CREATE INDEX IF NOT EXISTS idx_chat_reactions_user_email ON chat_reactions(user_email);

-- =====================================================
-- 5. 自動更新トリガー
-- =====================================================
CREATE TRIGGER update_expert_rooms_updated_at
    BEFORE UPDATE ON expert_rooms
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_chat_messages_updated_at
    BEFORE UPDATE ON chat_messages
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- =====================================================
-- 6. メンバー数カウント関数（RPC）
-- =====================================================
CREATE OR REPLACE FUNCTION increment_room_member_count(p_room_id UUID)
RETURNS VOID AS $$
BEGIN
    UPDATE expert_rooms
    SET total_members = total_members + 1
    WHERE id = p_room_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION decrement_room_member_count(p_room_id UUID)
RETURNS VOID AS $$
BEGIN
    UPDATE expert_rooms
    SET total_members = GREATEST(total_members - 1, 0)
    WHERE id = p_room_id;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- 7. Row Level Security (RLS) 設定
-- =====================================================
-- メッセージは入室済みユーザーのみ閲覧可能
ALTER TABLE chat_messages ENABLE ROW LEVEL SECURITY;

-- 誰でも読める（認証は別途アプリケーション層で管理）
CREATE POLICY "Anyone can read messages" ON chat_messages
    FOR SELECT USING (true);

-- 認証済みユーザーは投稿可能
CREATE POLICY "Authenticated users can insert messages" ON chat_messages
    FOR INSERT WITH CHECK (true);

-- 自分のメッセージは更新可能
CREATE POLICY "Users can update own messages" ON chat_messages
    FOR UPDATE USING (user_email = current_setting('request.jwt.claims', true)::json->>'email');

-- 自分のメッセージは削除可能（論理削除）
CREATE POLICY "Users can delete own messages" ON chat_messages
    FOR UPDATE USING (user_email = current_setting('request.jwt.claims', true)::json->>'email');

-- =====================================================
-- 8. テーブルコメント
-- =====================================================
COMMENT ON TABLE expert_rooms IS '予想家部屋マスタ';
COMMENT ON TABLE expert_room_members IS '入室記録（誰がどの部屋に入ったか）';
COMMENT ON TABLE chat_messages IS 'チャットメッセージ（room_idがNULLの場合は全体チャット）';
COMMENT ON TABLE chat_reactions IS 'メッセージリアクション（絵文字）';

COMMENT ON COLUMN chat_messages.room_id IS '部屋ID（NULLの場合は全体チャット）';
COMMENT ON COLUMN chat_messages.is_anonymous IS '匿名モードで投稿';
COMMENT ON COLUMN chat_messages.is_deleted IS '論理削除フラグ';
COMMENT ON COLUMN chat_messages.reply_to_id IS '返信先メッセージID';

-- =====================================================
-- 完了メッセージ
-- =====================================================
DO $$
BEGIN
    RAISE NOTICE 'オープンチャットテーブル作成完了';
    RAISE NOTICE 'テーブル作成: 4個（expert_rooms, expert_room_members, chat_messages, chat_reactions）';
END $$;
