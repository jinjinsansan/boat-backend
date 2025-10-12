import os
import json
import requests
from typing import Dict, Any, Optional, List
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class RacerDataManager:
    """競艇レーサーナレッジデータの管理クラス"""
    
    def __init__(self):
        self.data_dir = Path(__file__).parent.parent / "data"
        self.knowledge_file = self.data_dir / "boat_race_knowledge.jsonl"
        self.racer_knowledge: List[Dict[str, Any]] = []
        self.load_knowledge()
    
    def load_knowledge(self) -> None:
        """競艇レーサーナレッジデータを読み込む"""
        try:
            # ローカルファイルが存在しない場合はダウンロード
            if not self.knowledge_file.exists():
                logger.info("競艇ナレッジファイルが見つかりません。CDNからダウンロードします...")
                self.download_knowledge_from_cdn()
            
            # JSONLファイルを読み込む
            self.racer_knowledge = []
            with open(self.knowledge_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        self.racer_knowledge.append(json.loads(line))
            
            logger.info(f"競艇ナレッジデータを読み込みました: {len(self.racer_knowledge)}件のレース記録")
        
        except Exception as e:
            logger.error(f"競艇ナレッジデータの読み込みに失敗しました: {e}")
            self.racer_knowledge = []
    
    def download_knowledge_from_cdn(self) -> None:
        """CDNから競艇ナレッジファイルをダウンロード"""
        cdn_url = "https://pub-059afaafefa84116b57d57e0a72b81bd.r2.dev/boat_race_2020_2025.jsonl"
        
        try:
            logger.info(f"CDNからダウンロード中: {cdn_url}")
            
            # ストリーミングダウンロード
            response = requests.get(cdn_url, stream=True, timeout=60)
            response.raise_for_status()
            
            # データディレクトリが存在しない場合は作成
            self.data_dir.mkdir(exist_ok=True)
            
            # ファイルに書き込む
            with open(self.knowledge_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info("競艇ナレッジファイルのダウンロードが完了しました")
            
        except Exception as e:
            logger.error(f"CDNからのダウンロードに失敗しました: {e}")
            raise
    
    def get_racer_stats(self, racer_number: str) -> Optional[Dict[str, Any]]:
        """指定された登録番号のレーサー統計を取得"""
        # 将来的にレーサー別の統計を計算する実装を追加
        return None
    
    def get_venue_stats(self, venue: str) -> Optional[Dict[str, Any]]:
        """指定された競艇場の統計を取得"""
        venue_races = [r for r in self.racer_knowledge if r.get('venue') == venue]
        
        if not venue_races:
            return None
        
        return {
            "venue": venue,
            "total_races": len(venue_races),
            "date_range": {
                "start": min(r.get('race_date', '') for r in venue_races),
                "end": max(r.get('race_date', '') for r in venue_races)
            }
        }
    
    def get_knowledge_stats(self) -> Dict[str, Any]:
        """ナレッジデータの統計情報を取得"""
        if not self.racer_knowledge:
            return {
                "loaded": False,
                "total_records": 0
            }
        
        # 競艇場一覧
        venues = set(r.get('venue') for r in self.racer_knowledge if r.get('venue'))
        
        # 日付範囲
        dates = [r.get('race_date') for r in self.racer_knowledge if r.get('race_date')]
        
        return {
            "loaded": True,
            "total_records": len(self.racer_knowledge),
            "venues_count": len(venues),
            "date_range": {
                "start": min(dates) if dates else None,
                "end": max(dates) if dates else None
            }
        }

# シングルトンインスタンス
_racer_manager: Optional[RacerDataManager] = None

def get_racer_manager() -> RacerDataManager:
    """シングルトンインスタンスを取得"""
    global _racer_manager
    if _racer_manager is None:
        _racer_manager = RacerDataManager()
    return _racer_manager
