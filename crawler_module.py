import requests
import time
import json
from typing import List, Dict, Optional, Set
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ZenodoCrawler:
    BASE_URL = "https://zenodo.org/api/records"

    SEARCH_QUERIES = {
        'functional_correctness': [
            'program verification',
            'formal verification software',
            'model checker',
            'theorem prover',
        ],
        'termination': [
            'termination analysis',
            'termination prover',
        ],
        'complexity_bounds': [
            'complexity analysis tool',
            'resource analysis',
        ],
        'neural_network_verification': [
            'neural network verification',
            'DNN verification',
        ],
        'qbf_solver': [
            'QBF solver',
            'quantified boolean formula',
        ],
    }

    def __init__(self, access_token: Optional[str] = None):
        self.access_token = access_token
        self.session = requests.Session()
        self.crawled_data = []
        self.seen_ids: Set[str] = set()

        if access_token:
            self.session.headers.update({
                'Authorization': f'Bearer {access_token}'
            })

    def search_verification_tools(self, query: str, size: int = 20,
                                   page: int = 1, software_only: bool = True) -> List[Dict]:
        if software_only:
            full_query = f'({query}) AND (resource_type.type:software OR keywords:tool OR keywords:software OR keywords:framework)'
        else:
            full_query = query

        params = {
            'q': full_query,
            'size': size,
            'page': page,
            'sort': 'bestmatch',
        }

        try:
            logger.info(f"Searching: '{query}' (page {page})")
            response = self.session.get(self.BASE_URL, params=params)
            response.raise_for_status()

            data = response.json()
            hits = data.get('hits', {}).get('hits', [])
            total = data.get('hits', {}).get('total', 0)

            time.sleep(0.5)

            logger.info(f"Found {len(hits)} results (total: {total})")
            return hits

        except requests.RequestException as e:
            logger.error(f"Search error: {e}")
            return []

    def search_with_pagination(self, query: str, max_results: int = 100) -> List[Dict]:
        all_results = []
        page = 1
        page_size = 50

        while len(all_results) < max_results:
            results = self.search_verification_tools(query, size=page_size, page=page)

            if not results:
                break

            all_results.extend(results)
            page += 1

            if len(results) < page_size:
                break

            time.sleep(0.5)

        return all_results[:max_results]

    def get_tool_details(self, record_id: str) -> Optional[Dict]:
        url = f"{self.BASE_URL}/{record_id}"

        try:
            response = self.session.get(url)
            response.raise_for_status()
            time.sleep(0.3)
            return response.json()
        except requests.RequestException as e:
            logger.warning(f"Could not fetch details for {record_id}: {e}")
            return None

    def extract_relevant_data(self, record: Dict) -> Dict:
        metadata = record.get('metadata', {})

        return {
            'id': record.get('id'),
            'title': metadata.get('title'),
            'description': metadata.get('description'),
            'creators': metadata.get('creators', []),
            'keywords': metadata.get('keywords', []),
            'doi': record.get('doi'),
            'links': record.get('links', {}),
            'resource_type': metadata.get('resource_type'),
            'access_right': metadata.get('access_right'),
            'publication_date': metadata.get('publication_date'),
            'version': metadata.get('version'),
            'license': metadata.get('license', {}).get('id') if metadata.get('license') else None,
            'related_identifiers': metadata.get('related_identifiers', []),
            'crawled_at': time.strftime('%Y-%m-%d %H:%M:%S')
        }

    def is_relevant_tool(self, record: Dict) -> bool:
        text = ' '.join([
            str(record.get('title', '')),
            str(record.get('description', '')),
            ' '.join(record.get('keywords', []))
        ]).lower()

        strong_keywords = [
            'verifier', 'prover', 'model checker', 'theorem prover',
            'static analyzer', 'formal verification', 'program analysis',
            'sat solver', 'smt solver', 'qbf solver', 'termination prover',
            'neural network verification', 'program verification',
            'software verification', 'bounded model checking'
        ]

        weak_keywords = [
            'verification', 'correctness', 'termination', 'complexity',
            'formal', 'solver', 'checker', 'analysis', 'proof',
            'specification', 'invariant', 'assertion', 'contract'
        ]

        exclusion_keywords = [
            'biology', 'medical', 'clinical', 'patient', 'disease',
            'species', 'ecological', 'geographic', 'survey', 'questionnaire',
            'interview', 'photograph', 'museum', 'archaeological'
        ]

        if any(kw in text for kw in exclusion_keywords):
            return False

        if any(kw in text for kw in strong_keywords):
            return True

        weak_count = sum(1 for kw in weak_keywords if kw in text)
        return weak_count >= 2

    def run(self, categories: Optional[List[str]] = None) -> List[Dict]:
        logger.info("Starting Zenodo crawler...")

        if categories is None:
            categories = list(self.SEARCH_QUERIES.keys())

        all_results = []

        for category in categories:
            if category not in self.SEARCH_QUERIES:
                logger.warning(f"Unknown category: {category}")
                continue

            logger.info(f"\n--- Crawling category: {category} ---")
            queries = self.SEARCH_QUERIES[category]

            for query in queries:
                results = self.search_verification_tools(query, size=25)

                for record in results:
                    record_id = str(record.get('id', ''))

                    if record_id and record_id not in self.seen_ids:
                        self.seen_ids.add(record_id)
                        processed = self.extract_relevant_data(record)
                        processed['search_query'] = query
                        processed['search_category'] = category

                        if self.is_relevant_tool(processed):
                            all_results.append(processed)

                time.sleep(0.5)

            logger.info(f"Category {category}: {len([r for r in all_results if r.get('search_category') == category])} tools found")
            time.sleep(1)

        logger.info(f"\nCrawl complete. Total unique tools: {len(all_results)}")
        self.crawled_data = all_results
        return all_results

    def run_quick(self) -> List[Dict]:
        logger.info("Starting quick crawl...")

        quick_queries = [
            "program verification tool",
            "termination prover",
            "complexity analysis tool",
            "neural network verification",
            "QBF solver",
        ]

        all_results = []

        for query in quick_queries:
            results = self.search_verification_tools(query, size=10)

            for record in results:
                record_id = str(record.get('id', ''))

                if record_id and record_id not in self.seen_ids:
                    self.seen_ids.add(record_id)
                    processed = self.extract_relevant_data(record)
                    all_results.append(processed)

            time.sleep(0.5)

        logger.info(f"Quick crawl complete. Found {len(all_results)} tools")
        return all_results


def test_crawler():
    crawler = ZenodoCrawler()
    results = crawler.search_verification_tools("verification tool", size=5)
    print(f"Test search: {len(results)} results")

    if results:
        print(f"First result: {results[0].get('metadata', {}).get('title', 'No title')}")

    return results


def test_full_crawl():
    crawler = ZenodoCrawler()
    results = crawler.run()

    print(f"\nFull crawl results: {len(results)} tools")

    categories = {}
    for tool in results:
        cat = tool.get('search_category', 'unknown')
        categories[cat] = categories.get(cat, 0) + 1

    print("By category:")
    for cat, count in categories.items():
        print(f"  {cat}: {count}")

    return results


if __name__ == '__main__':
    test_crawler()