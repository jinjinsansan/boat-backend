# Supabase Migrations

## 初期セットアップ

競艇版Supabaseで以下のSQLを実行してください：

### 1. SQL Editorで実行

Supabase Dashboard → SQL Editor → New Query

`v2_complete_migration.sql` の内容をコピー＆ペーストして実行

### 2. 必要なテーブル

- `v2_users` - ユーザー情報
- `v2_user_points` - ポイント管理
- `v2_point_transactions` - ポイント履歴
- `v2_chat_sessions` - チャットセッション
- `v2_chat_messages` - チャットメッセージ
- `v2_referral_history` - 友達紹介履歴
- `v2_line_oauth_sessions` - LINE OAuth
- その他V2関連テーブル

### 3. 実行手順

```sql
-- 1. v2_complete_migration.sql を実行
-- 2. エラーがないか確認
-- 3. テーブル一覧を確認
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'public' AND table_name LIKE 'v2_%';
```

## 追加マイグレーション

### enabled_chats カラム追加

**実行順序：**
1. `v2_complete_migration.sql` - 初期テーブル作成
2. `add_enabled_chats_column.sql` - enabled_chatsカラム追加（複数AIタイプ管理用）

**add_enabled_chats_column.sql の実行：**

Supabase Dashboard → SQL Editor → New Query

```sql
-- v2_chat_sessionsテーブルに enabled_chats カラムを追加
ALTER TABLE v2_chat_sessions 
ADD COLUMN IF NOT EXISTS enabled_chats JSONB DEFAULT '{"imlogic": true, "viewlogic": true}'::jsonb;

ALTER TABLE v2_chat_sessions 
ADD COLUMN IF NOT EXISTS imlogic_settings_id UUID;

ALTER TABLE v2_chat_sessions 
ADD COLUMN IF NOT EXISTS race_snapshot JSONB;
```

## 環境変数

`.env`ファイルに以下を設定：

```
SUPABASE_URL=https://clpokimbrfgxopevzmkx.supabase.co
SUPABASE_SERVICE_KEY=your_service_key
SUPABASE_SERVICE_ROLE_KEY=your_service_role_key
```
