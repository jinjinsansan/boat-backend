-- =====================================================
-- コラム機能テーブル作成
-- 実行日: 2025-10-13
-- 説明: 競艇版コラム機能のための4つのテーブル
-- =====================================================

-- =====================================================
-- 1. コラムカテゴリテーブル
-- =====================================================
CREATE TABLE IF NOT EXISTS column_categories (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(100) NOT NULL UNIQUE,
    slug VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    display_order INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- インデックス
CREATE INDEX IF NOT EXISTS idx_column_categories_slug ON column_categories(slug);
CREATE INDEX IF NOT EXISTS idx_column_categories_display_order ON column_categories(display_order);

-- 初期カテゴリ作成
INSERT INTO column_categories (name, slug, description, display_order)
VALUES ('競艇予想コラム', 'boat-race-prediction', '競艇レースの予想やデータ分析に関するコラム', 1)
ON CONFLICT (name) DO NOTHING;

-- =====================================================
-- 2. コラム本体テーブル
-- =====================================================
CREATE TABLE IF NOT EXISTS v2_columns (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title VARCHAR(200) NOT NULL,
    summary TEXT,
    content TEXT NOT NULL,
    featured_image TEXT,
    category_id UUID REFERENCES column_categories(id) ON DELETE SET NULL,
    
    -- アクセス制御
    access_type VARCHAR(20) DEFAULT 'free' CHECK (access_type IN ('free', 'point_required', 'line_linked')),
    required_points INTEGER DEFAULT 0,
    
    -- 公開設定
    is_published BOOLEAN DEFAULT FALSE,
    published_at TIMESTAMP,
    
    -- 表示順序・統計
    display_order INTEGER DEFAULT 0,
    view_count INTEGER DEFAULT 0,
    
    -- メタデータ
    author_id UUID REFERENCES v2_users(id) ON DELETE SET NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- インデックス
CREATE INDEX IF NOT EXISTS idx_v2_columns_category_id ON v2_columns(category_id);
CREATE INDEX IF NOT EXISTS idx_v2_columns_is_published ON v2_columns(is_published);
CREATE INDEX IF NOT EXISTS idx_v2_columns_published_at ON v2_columns(published_at);
CREATE INDEX IF NOT EXISTS idx_v2_columns_display_order ON v2_columns(display_order);
CREATE INDEX IF NOT EXISTS idx_v2_columns_author_id ON v2_columns(author_id);

-- =====================================================
-- 3. コラム既読管理テーブル
-- =====================================================
CREATE TABLE IF NOT EXISTS v2_column_reads (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    column_id UUID REFERENCES v2_columns(id) ON DELETE CASCADE,
    user_id UUID REFERENCES v2_users(id) ON DELETE CASCADE,
    read_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    points_used INTEGER DEFAULT 0,
    
    UNIQUE(column_id, user_id)
);

-- インデックス
CREATE INDEX IF NOT EXISTS idx_v2_column_reads_column_id ON v2_column_reads(column_id);
CREATE INDEX IF NOT EXISTS idx_v2_column_reads_user_id ON v2_column_reads(user_id);
CREATE INDEX IF NOT EXISTS idx_v2_column_reads_read_at ON v2_column_reads(read_at);

-- =====================================================
-- 4. コラム閲覧記録テーブル（アクセス解析用）
-- =====================================================
CREATE TABLE IF NOT EXISTS v2_column_views (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    column_id UUID REFERENCES v2_columns(id) ON DELETE CASCADE,
    user_id UUID REFERENCES v2_users(id) ON DELETE SET NULL,
    viewed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- インデックス
CREATE INDEX IF NOT EXISTS idx_v2_column_views_column_id ON v2_column_views(column_id);
CREATE INDEX IF NOT EXISTS idx_v2_column_views_user_id ON v2_column_views(user_id);
CREATE INDEX IF NOT EXISTS idx_v2_column_views_viewed_at ON v2_column_views(viewed_at);

-- =====================================================
-- 5. 閲覧数カウント関数（RPC）
-- =====================================================
CREATE OR REPLACE FUNCTION increment_column_view_count(p_column_id UUID)
RETURNS VOID AS $$
BEGIN
    UPDATE v2_columns
    SET view_count = view_count + 1
    WHERE id = p_column_id;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- 6. 自動更新トリガー
-- =====================================================
CREATE TRIGGER update_column_categories_updated_at
    BEFORE UPDATE ON column_categories
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_v2_columns_updated_at
    BEFORE UPDATE ON v2_columns
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- =====================================================
-- 7. テーブルコメント
-- =====================================================
COMMENT ON TABLE column_categories IS 'コラムカテゴリマスタ';
COMMENT ON TABLE v2_columns IS 'コラム本体データ';
COMMENT ON TABLE v2_column_reads IS 'コラム既読管理（ポイント消費記録）';
COMMENT ON TABLE v2_column_views IS 'コラム閲覧ログ（アクセス解析用）';

COMMENT ON COLUMN v2_columns.access_type IS 'アクセスタイプ: free(無料), point_required(ポイント必要), line_linked(LINE連携必要)';
COMMENT ON COLUMN v2_columns.required_points IS 'access_type=point_requiredの場合の必要ポイント数';
COMMENT ON COLUMN v2_columns.view_count IS '閲覧数（キャッシュ値）';

-- =====================================================
-- 完了メッセージ
-- =====================================================
DO $$
BEGIN
    RAISE NOTICE 'コラム機能テーブル作成完了';
    RAISE NOTICE 'テーブル作成: 4個（column_categories, v2_columns, v2_column_reads, v2_column_views）';
    RAISE NOTICE '初期カテゴリ: 競艇予想コラム';
END $$;
