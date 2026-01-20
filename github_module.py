import requests
import json
import base64
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import logging
import time

logger = logging.getLogger(__name__)

class GitHubIntegration:

    GITHUB_API_BASE = "https://api.github.com"

    def __init__(self, token: Optional[str] = None):
        self.token = token
        self.session = requests.Session()

        if token:
            self.session.headers.update({
                'Authorization': f'token {token}',
                'Accept': 'application/vnd.github.v3+json'
            })
        else:
            logger.warning("fara token github")

        self.repo_owner = None
        self.repo_name = None

    def set_repository(self, owner: str, repo: str):
        self.repo_owner = owner
        self.repo_name = repo
        logger.info(f"repo setat: {owner}/{repo}")

    def check_rate_limit(self) -> Dict:
        try:
            response = self.session.get(f"{self.GITHUB_API_BASE}/rate_limit")
            response.raise_for_status()
            limits = response.json()

            core_limit = limits.get('rate', {})
            remaining = core_limit.get('remaining', 0)
            reset_time = core_limit.get('reset', 0)

            logger.info(f"api ramas: {remaining}")

            return {
                'remaining': remaining,
                'reset': reset_time,
                'limit': core_limit.get('limit', 60)
            }

        except requests.RequestException as e:
            logger.error(f"eroare rate limit: {e}")
            return {'remaining': 0, 'reset': 0, 'limit': 0}

    def create_issue(self, title: str, body: str,
                     labels: List[str] = None) -> Optional[str]:
        if not self.token:
            logger.error("token necesar")
            return None

        url = f"{self.GITHUB_API_BASE}/repos/{self.repo_owner}/{self.repo_name}/issues"

        data = {
            'title': title,
            'body': body,
            'labels': labels or ['verification-tools', 'beta', 'automated']
        }

        try:
            response = self.session.post(url, json=data)
            response.raise_for_status()

            issue_data = response.json()
            issue_url = issue_data.get('html_url')

            logger.info(f"issue creat: {issue_url}")
            return issue_url

        except requests.RequestException as e:
            logger.error(f"eroare issue: {e}")
            return None

    def upload_file(self, file_path: str, content: str,
                    message: str, branch: str = "main") -> bool:
        if not self.token:
            logger.error("token necesar pt upload")
            return False

        url = (f"{self.GITHUB_API_BASE}/repos/{self.repo_owner}/"
               f"{self.repo_name}/contents/{file_path}")

        sha = self._get_file_sha(file_path, branch)

        encoded_content = base64.b64encode(content.encode()).decode()

        data = {
            'message': message,
            'content': encoded_content,
            'branch': branch
        }

        if sha:
            data['sha'] = sha
            logger.info(f"update fisier: {file_path}")
        else:
            logger.info(f"fisier nou: {file_path}")

        try:
            response = self.session.put(url, json=data)
            response.raise_for_status()

            logger.info(f"uploadat: {file_path}")
            return True

        except requests.RequestException as e:
            logger.error(f"eroare upload: {e}")
            return False

    def _get_file_sha(self, file_path: str, branch: str) -> Optional[str]:
        url = (f"{self.GITHUB_API_BASE}/repos/{self.repo_owner}/"
               f"{self.repo_name}/contents/{file_path}")

        params = {'ref': branch}

        try:
            response = self.session.get(url, params=params)
            if response.status_code == 200:
                return response.json().get('sha')
        except:
            pass

        return None

    def create_pull_request(self, branch_name: str, title: str,
                            body: str) -> Optional[str]:
        if not self.token:
            logger.error("token necesar pr")
            return None

        url = f"{self.GITHUB_API_BASE}/repos/{self.repo_owner}/{self.repo_name}/pulls"

        data = {
            'title': title,
            'body': body,
            'head': branch_name,
            'base': 'main'
        }

        try:
            response = self.session.post(url, json=data)
            response.raise_for_status()

            pr_data = response.json()
            pr_url = pr_data.get('html_url')

            logger.info(f"pr creat: {pr_url}")
            return pr_url

        except requests.RequestException as e:
            logger.error(f"eroare pr: {e}")
            return None

    def check_repository_access(self) -> bool:
        url = f"{self.GITHUB_API_BASE}/repos/{self.repo_owner}/{self.repo_name}"

        try:
            response = self.session.get(url)
            if response.status_code == 200:
                repo_data = response.json()
                logger.info(f"repo gasit: {repo_data.get('full_name')}")

                if self.token:
                    perms = repo_data.get('permissions', {})
                    can_push = perms.get('push', False)
                    logger.info(f"acces push: {can_push}")
                    return can_push
                return True

        except requests.RequestException as e:
            logger.error(f"eroare acces: {e}")

        return False

    def prepare_batch_upload(self, tools: List[Dict]) -> List[Dict]:
        BATCH_SIZE = 10  # limita beta

        batches = []
        for i in range(0, len(tools), BATCH_SIZE):
            batch = tools[i:i + BATCH_SIZE]
            batches.append({
                'batch_number': i // BATCH_SIZE + 1,
                'tools': batch,
                'timestamp': datetime.now().isoformat()
            })

        logger.info(f"pregatit {len(batches)} batch-uri")
        return batches


def test_github():
    github = GitHubIntegration()
    github.set_repository("andreixdbolos", "proiect-vf")

    limits = github.check_rate_limit()
    print(f"test github: ramas {limits['remaining']}")

    has_access = github.check_repository_access()
    print(f"test: acces repo {has_access}")

    return True