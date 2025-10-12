# D-Logic Boat Backend

競艇AI予想システム「D-Logic Boat」のバックエンドAPI

## 技術スタック

- FastAPI
- Python 3.12
- Supabase（データベース）
- OpenAI API（AI推論）

## 環境変数

`.env`ファイルを作成して以下を設定：

```env
# Supabase
SUPABASE_URL=your_supabase_url
SUPABASE_SERVICE_KEY=your_service_key

# OpenAI
OPENAI_API_KEY=your_api_key

# LINE（オプション）
LINE_CHANNEL_SECRET=your_channel_secret
LINE_CHANNEL_ACCESS_TOKEN=your_access_token

# 環境
ENVIRONMENT=production
PORT=8000
```

## ローカル開発

```bash
# 依存関係インストール
pip install -r requirements.txt

# サーバー起動
python main.py
```

## Renderデプロイ

- Build Command: `pip install -r requirements.txt`
- Start Command: `python main.py`
- Port: 環境変数`$PORT`を自動使用
