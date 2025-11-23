import requests
import time
import json
from typing import List, Dict, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ZenodoCrawler:
    BASE_URL = "https://zenodo.org/api/records"

    def __init__(self, access_token: Optional[str] = None):
        self.access_token = access_token
        self.session = requests.Session()
        self.crawled_data = []

        if access_token:
            self.session.headers.update({
                'Authorization': f'Bearer {access_token}'
            })

    def search_verification_tools(self, query: str = "verification tools",
                                  size: int = 20) -> List[Dict]:
        params = {
            'q': query,
            'size': size,
            'type': 'software',
            'keywords': 'verification OR termination OR complexity OR QBF'
        }

        try:
            logger.info(f"cautare: {query}")
            response = self.session.get(self.BASE_URL, params=params)
            response.raise_for_status()

            data = response.json()
            self.crawled_data = data.get('hits', {}).get('hits', [])

            time.sleep(0.5)  # delay api

            logger.info(f"gasit {len(self.crawled_data)}")
            return self.crawled_data

        except requests.RequestException as e:
            logger.error(f"eroare: {e}")
            return []

    def get_tool_details(self, record_id: str) -> Optional[Dict]:
        url = f"{self.BASE_URL}/{record_id}"

        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except:
            logger.warning(f"nu gasit: {record_id}")
            return None

    def extract_relevant_data(self, record: Dict) -> Dict:
        return {
            'id': record.get('id'),
            'title': record.get('metadata', {}).get('title'),
            'description': record.get('metadata', {}).get('description'),
            'creators': record.get('metadata', {}).get('creators', []),
            'keywords': record.get('metadata', {}).get('keywords', []),
            'doi': record.get('doi'),
            'links': record.get('links', {}),
            'resource_type': record.get('metadata', {}).get('resource_type'),
            'access_right': record.get('metadata', {}).get('access_right'),
            'crawled_at': time.strftime('%Y-%m-%d %H:%M:%S')
        }

    def run(self) -> List[Dict]:
        logger.info("start crawler...")
        keywords = [
            "verification tools",
            "functional correctness",
            "termination analysis",
            "complexity bounds",
            "neural network verification",
            "QBF solver"
        ]

        all_results = []

        for keyword in keywords:
            results = self.search_verification_tools(keyword, size=10)
            for record in results:
                processed = self.extract_relevant_data(record)
                if processed not in all_results:
                    all_results.append(processed)

            time.sleep(1)

        logger.info(f"gata. {len(all_results)} unelte")
        return all_results


def test_crawler():
    crawler = ZenodoCrawler()
    results = crawler.search_verification_tools("verification", size=5)
    print(f"test: {len(results)}")
    return results