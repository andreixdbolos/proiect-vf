import argparse
import logging
import os
import sys
from typing import Optional

from crawler_module import ZenodoCrawler
from parser_module import ToolDataParser
from storage_module import DataStorage
from github_module import GitHubIntegration

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class VerificationToolsPipeline:
    def __init__(self, config: dict):
        self.config = config
        self.crawler = ZenodoCrawler(access_token=config.get('zenodo_token'))
        self.parser = ToolDataParser()
        self.storage = DataStorage(base_dir=config.get('data_dir', './data'))
        self.github = GitHubIntegration(token=config.get('github_token'))

        if config.get('github_repo'):
            owner, repo = config['github_repo'].split('/')
            self.github.set_repository(owner, repo)

    def run_crawl(self) -> list:
        logger.info("Starting crawl phase...")
        raw_data = self.crawler.run()
        logger.info(f"Crawled {len(raw_data)} raw records from Zenodo")
        return raw_data

    def run_parse(self, raw_data: list) -> list:
        logger.info("Starting parse phase...")
        parsed_data = self.parser.parse_zenodo_data(raw_data)

        stats = self.parser.get_statistics()
        logger.info(f"Parsed {stats['total_parsed']} tools")
        logger.info(f"Categories: {stats['categories']}")

        if stats['errors'] > 0:
            logger.warning(f"Encountered {stats['errors']} parsing errors")

        return parsed_data

    def run_store(self, parsed_data: list) -> dict:
        logger.info("Starting storage phase...")

        json_path = self.storage.save_to_json(parsed_data)
        csv_path = self.storage.save_to_csv(parsed_data)
        db_count = self.storage.save_to_database(parsed_data)

        return {
            'json_path': json_path,
            'csv_path': csv_path,
            'db_records': db_count
        }

    def run_upload(self, parsed_data: list, dry_run: bool = True) -> Optional[str]:
        logger.info("Starting upload phase...")

        if not self.config.get('github_token'):
            logger.warning("No GitHub token provided, skipping upload")
            return None

        if dry_run:
            logger.info("Dry run mode - not uploading to GitHub")
            return None

        import json
        json_content = json.dumps(parsed_data, indent=2, ensure_ascii=False)

        success = self.github.upload_file(
            file_path='data/verification_tools.json',
            content=json_content,
            message='Update verification tools data from Zenodo crawl'
        )

        if success:
            logger.info("Successfully uploaded to GitHub")
            return 'data/verification_tools.json'
        else:
            logger.error("Failed to upload to GitHub")

        return None

    def run_full_pipeline(self, dry_run: bool = True) -> dict:
        logger.info("=" * 50)
        logger.info("Starting Verification Tools Pipeline")
        logger.info("=" * 50)

        raw_data = self.run_crawl()
        parsed_data = self.run_parse(raw_data)
        storage_result = self.run_store(parsed_data)
        upload_result = self.run_upload(parsed_data, dry_run=dry_run)

        result = {
            'crawled': len(raw_data),
            'parsed': len(parsed_data),
            'storage': storage_result,
            'upload': upload_result,
            'stats': self.storage.get_statistics()
        }

        logger.info("=" * 50)
        logger.info("Pipeline completed")
        logger.info(f"Results: {result}")
        logger.info("=" * 50)

        return result


def load_config() -> dict:
    config = {
        'zenodo_token': os.environ.get('ZENODO_TOKEN'),
        'github_token': os.environ.get('GITHUB_TOKEN'),
        'github_repo': os.environ.get('GITHUB_REPO', 'andreixdbolos/proiect-vf'),
        'data_dir': os.environ.get('DATA_DIR', './data')
    }

    config_file = 'config.json'
    if os.path.exists(config_file):
        import json
        with open(config_file, 'r') as f:
            file_config = json.load(f)
            config.update(file_config)

    return config


def main():
    parser = argparse.ArgumentParser(
        description='Crawl verification tools from Zenodo and upload to GitHub'
    )
    parser.add_argument(
        '--crawl', action='store_true',
        help='Run only the crawl phase'
    )
    parser.add_argument(
        '--parse', action='store_true',
        help='Run only the parse phase (requires existing crawled data)'
    )
    parser.add_argument(
        '--upload', action='store_true',
        help='Run only the upload phase (requires existing parsed data)'
    )
    parser.add_argument(
        '--dry-run', action='store_true', default=True,
        help='Dry run mode - do not actually upload to GitHub (default: True)'
    )
    parser.add_argument(
        '--live', action='store_true',
        help='Live mode - actually upload to GitHub'
    )
    parser.add_argument(
        '--stats', action='store_true',
        help='Show statistics from database'
    )
    parser.add_argument(
        '--query', type=str,
        help='Query tools by category'
    )

    args = parser.parse_args()

    config = load_config()
    pipeline = VerificationToolsPipeline(config)

    if args.stats:
        stats = pipeline.storage.get_statistics()
        print("\nDatabase Statistics:")
        print(f"  Total records: {stats['total_records']}")
        print(f"  Categories: {stats['categories']}")
        print(f"  Sources: {stats['sources']}")
        return

    if args.query:
        tools = pipeline.storage.query_database(category=args.query)
        print(f"\nFound {len(tools)} tools in category '{args.query}':")
        for tool in tools:
            print(f"  - {tool.get('name')}: {tool.get('description', '')[:60]}...")
        return

    dry_run = not args.live

    if args.crawl:
        raw_data = pipeline.run_crawl()
        print(f"Crawled {len(raw_data)} records")
    elif args.parse:
        logger.info("Parse-only mode not yet implemented")
    elif args.upload:
        logger.info("Upload-only mode not yet implemented")
    else:
        result = pipeline.run_full_pipeline(dry_run=dry_run)
        print(f"\nPipeline Results:")
        print(f"  Crawled: {result['crawled']} records")
        print(f"  Parsed: {result['parsed']} tools")
        print(f"  Stored: {result['storage']['db_records']} new records in DB")


if __name__ == '__main__':
    main()
    