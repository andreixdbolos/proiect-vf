import json
import csv
import os
from datetime import datetime
from typing import Dict, List, Optional, Any
import logging
import sqlite3

logger = logging.getLogger(__name__)

class DataStorage:

    def __init__(self, base_dir: str = "./data"):
        self.base_dir = base_dir
        self._ensure_directories()
        self.db_path = os.path.join(base_dir, "tools.db")
        self._init_database()

    def _ensure_directories(self):
        directories = [
            self.base_dir,
            os.path.join(self.base_dir, 'json'),
            os.path.join(self.base_dir, 'csv'),
            os.path.join(self.base_dir, 'backup')
        ]

        for dir_path in directories:
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
                logger.info(f"creat: {dir_path}")

    def _init_database(self):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS tools (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    category TEXT,
                    description TEXT,
                    source TEXT,
                    source_id TEXT,
                    doi TEXT,
                    url TEXT,
                    data JSON,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_category
                ON tools (category)
            ''')

            conn.commit()
            conn.close()
            logger.info("baza date init")

        except sqlite3.Error as e:
            logger.error(f"eroare db: {e}")

    def save_to_json(self, data: List[Dict], filename: Optional[str] = None) -> str:
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"tools_{timestamp}.json"

        filepath = os.path.join(self.base_dir, 'json', filename)

        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            logger.info(f"salvat {len(data)} in {filepath}")
            return filepath

        except IOError as e:
            logger.error(f"eroare json: {e}")
            raise

    def save_to_csv(self, data: List[Dict], filename: Optional[str] = None) -> str:
        if not data:
            logger.warning("nimic de salvat")
            return ""

        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"tools_{timestamp}.csv"

        filepath = os.path.join(self.base_dir, 'csv', filename)

        try:
            fieldnames = [
                'name', 'category', 'description', 'source',
                'source_id', 'doi', 'url', 'authors', 'keywords'
            ]

            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

                for item in data:
                    row = {
                        'name': item.get('name', ''),
                        'category': item.get('category', ''),
                        'description': item.get('description', '')[:200],
                        'source': item.get('source', ''),
                        'source_id': str(item.get('source_id', '')),
                        'doi': item.get('doi', ''),
                        'url': item.get('url', ''),
                        'authors': ', '.join(item.get('authors', [])),
                        'keywords': ', '.join(item.get('keywords', []))
                    }
                    writer.writerow(row)

            logger.info(f"salvat {len(data)} in csv")
            return filepath

        except IOError as e:
            logger.error(f"eroare csv: {e}")
            raise

    def save_to_database(self, data: List[Dict]) -> int:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        inserted = 0

        try:
            for item in data:
                cursor.execute(
                    "SELECT id FROM tools WHERE source_id = ?",
                    (item.get('source_id', ''),)
                )

                if cursor.fetchone() is None:
                    cursor.execute('''
                        INSERT INTO tools
                        (name, category, description, source, source_id, doi, url, data)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        item.get('name', ''),
                        item.get('category', ''),
                        item.get('description', ''),
                        item.get('source', ''),
                        item.get('source_id', ''),
                        item.get('doi', ''),
                        item.get('url', ''),
                        json.dumps(item)
                    ))
                    inserted += 1

            conn.commit()
            logger.info(f"inserat {inserted} inregistrari")

        except sqlite3.Error as e:
            logger.error(f"eroare db: {e}")
            conn.rollback()

        finally:
            conn.close()

        return inserted

    def load_from_json(self, filename: str) -> List[Dict]:
        filepath = os.path.join(self.base_dir, 'json', filename)

        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logger.info(f"incarcat {len(data)} din {filepath}")
            return data

        except (IOError, json.JSONDecodeError) as e:
            logger.error(f"eroare incarcare: {e}")
            return []

    def query_database(self, category: Optional[str] = None,
                       limit: int = 100) -> List[Dict]:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            if category:
                cursor.execute(
                    "SELECT data FROM tools WHERE category = ? LIMIT ?",
                    (category, limit)
                )
            else:
                cursor.execute("SELECT data FROM tools LIMIT ?", (limit,))

            results = cursor.fetchall()
            tools = [json.loads(row[0]) for row in results]

            return tools

        except sqlite3.Error as e:
            logger.error(f"eroare query: {e}")
            return []

        finally:
            conn.close()

    def create_backup(self) -> str:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = os.path.join(self.base_dir, 'backup', f'backup_{timestamp}.json')

        all_data = self.query_database(limit=10000)

        with open(backup_file, 'w') as f:
            json.dump(all_data, f, indent=2)

        logger.info(f"backup: {backup_file}")
        return backup_file

    def get_statistics(self) -> Dict:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        stats = {
            'total_records': 0,
            'categories': {},
            'sources': {}
        }

        try:
            cursor.execute("SELECT COUNT(*) FROM tools")
            stats['total_records'] = cursor.fetchone()[0]

            cursor.execute(
                "SELECT category, COUNT(*) FROM tools GROUP BY category"
            )
            for cat, count in cursor.fetchall():
                stats['categories'][cat] = count

            cursor.execute(
                "SELECT source, COUNT(*) FROM tools GROUP BY source"
            )
            for src, count in cursor.fetchall():
                stats['sources'][src] = count

        except sqlite3.Error as e:
            logger.error(f"eroare stats: {e}")

        finally:
            conn.close()

        return stats


def test_storage():
    storage = DataStorage()

    sample_data = [{
        'name': 'Test Tool',
        'category': 'functional_correctness',
        'description': 'Test description',
        'source': 'test',
        'source_id': 'test123'
    }]

    json_file = storage.save_to_json(sample_data)
    print(f"test: salvat {json_file}")

    count = storage.save_to_database(sample_data)
    print(f"test: {count} inregistrari")

    return storage.get_statistics()
