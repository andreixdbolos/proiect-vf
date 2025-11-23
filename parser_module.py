import json
import yaml
import re
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class ToolDataParser:

    TOOL_CATEGORIES = [
        'functional_correctness',
        'termination',
        'complexity_bounds',
        'neural_network_verification',
        'qbf_solver',
        'other'
    ]

    def __init__(self):
        self.parsed_tools = []
        self.validation_errors = []

    def parse_zenodo_data(self, raw_data: List[Dict]) -> List[Dict]:
        parsed = []

        for item in raw_data:
            try:
                tool = self._standardize_tool_data(item)
                if self._validate_tool_data(tool):
                    parsed.append(tool)
                else:
                    logger.warning(f"invalid: {item.get('title', 'necunoscut')}")
            except Exception as e:
                logger.error(f"eroare parsare: {e}")
                self.validation_errors.append({
                    'item': item.get('id', 'unknown'),
                    'error': str(e)
                })

        self.parsed_tools = parsed
        return parsed

    def _standardize_tool_data(self, item: Dict) -> Dict:
        category = self._categorize_tool(item)

        standard_data = {
            'name': item.get('title', '').strip(),
            'description': self._clean_description(item.get('description', '')),
            'category': category,
            'source': 'zenodo',
            'source_id': item.get('id'),
            'doi': item.get('doi'),
            'authors': self._extract_authors(item.get('creators', [])),
            'keywords': item.get('keywords', []),
            'url': item.get('links', {}).get('self'),
            'license': item.get('access_right', 'unknown'),
            'metadata': {
                'crawled_at': item.get('crawled_at'),
                'resource_type': item.get('resource_type'),
                'version': 'beta-extract'
            }
        }

        return standard_data

    def _categorize_tool(self, item: Dict) -> str:
        text = (item.get('title', '') + ' ' +
                item.get('description', '') + ' ' +
                ' '.join(item.get('keywords', []))).lower()
        if 'neural' in text or 'deep learning' in text:
            return 'neural_network_verification'
        elif 'termination' in text:
            return 'termination'
        elif 'complexity' in text or 'bounds' in text:
            return 'complexity_bounds'
        elif 'qbf' in text or 'boolean' in text:
            return 'qbf_solver'
        elif 'correctness' in text or 'verification' in text:
            return 'functional_correctness'
        else:
            return 'other'

    def _clean_description(self, desc: str) -> str:
        if not desc:
            return ""

        desc = re.sub('<[^<]+?>', '', desc)  # sterge html

        if len(desc) > 500:
            desc = desc[:497] + "..."

        return desc.strip()

    def _extract_authors(self, creators: List) -> List[str]:
        authors = []
        for creator in creators:
            if isinstance(creator, dict):
                name = creator.get('name', '')
                if name:
                    authors.append(name)
        return authors

    def _validate_tool_data(self, tool: Dict) -> bool:
        required_fields = ['name', 'category', 'source']

        for field in required_fields:
            if not tool.get(field):
                return False

        if len(tool['name']) < 3:
            return False

        return True

    def parse_yaml_tools(self, yaml_content: str) -> List[Dict]:
        try:
            data = yaml.safe_load(yaml_content)
            logger.info("yaml nu implementat")
            return []
        except yaml.YAMLError as e:
            logger.error(f"YAML parse error: {e}")
            return []

    def export_to_json(self, filepath: str):
        with open(filepath, 'w') as f:
            json.dump(self.parsed_tools, f, indent=2)
        logger.info(f"salvat {len(self.parsed_tools)} in {filepath}")

    def get_statistics(self) -> Dict:
        stats = {
            'total_parsed': len(self.parsed_tools),
            'errors': len(self.validation_errors),
            'categories': {}
        }

        for tool in self.parsed_tools:
            cat = tool.get('category', 'other')
            stats['categories'][cat] = stats['categories'].get(cat, 0) + 1

        return stats


def test_parser():
    parser = ToolDataParser()
    sample_data = [{
        'id': '12345',
        'title': 'Sample Verification Tool',
        'description': 'A tool for testing',
        'keywords': ['verification', 'testing'],
        'creators': [{'name': 'Test Author'}],
        'doi': '10.5555/test',
        'crawled_at': '2025-11-23'
    }]

    result = parser.parse_zenodo_data(sample_data)
    print(f"test parser: {len(result)}")
    return result