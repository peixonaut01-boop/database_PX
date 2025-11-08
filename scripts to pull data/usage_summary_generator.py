from __future__ import annotations

import os
from pathlib import Path
from typing import List

import requests

REPO_OWNER = 'guithom04'
REPO_NAME = 'database_PX'
FIREBASE_EXPORT_URL = 'https://peixo-28d2d-default-rtdb.firebaseio.com/.json?format=export'
SUMMARY_PATH = Path('usage_summary.txt')


def human_bytes(num: int | float) -> str:
    step = 1024.0
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    size = float(num)
    for unit in units:
        if size < step:
            return f"{size:.2f} {unit}"
        size /= step
    return f"{size:.2f} EB"


def fetch_firebase_size() -> int | None:
    try:
        response = requests.get(FIREBASE_EXPORT_URL, timeout=60)
        response.raise_for_status()
        length = response.headers.get('Content-Length')
        if length:
            return int(length)
    except requests.RequestException as exc:
        print(f"[WARNING] Failed to fetch Firebase export size: {exc}")
    return None


def fetch_github_repo_size() -> int | None:
    token = os.getenv('GITHUB_TOKEN')
    headers = {'Accept': 'application/vnd.github+json'}
    if token:
        headers['Authorization'] = f'token {token}'
    try:
        response = requests.get(
            f'https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}',
            headers=headers,
            timeout=30,
        )
        if response.status_code == 200:
            repo_json = response.json()
            size_kb = repo_json.get('size')  # size reported in KB
            if size_kb is not None:
                return int(size_kb * 1024)
        else:
            print(f"[WARNING] GitHub repo size request returned {response.status_code}")
    except requests.RequestException as exc:
        print(f"[WARNING] Failed to fetch GitHub repo size: {exc}")
    return None


def build_summary() -> List[str]:
    lines: List[str] = []
    lines.append('=== Usage Summary ===')

    fb_size = fetch_firebase_size()
    if fb_size is not None:
        lines.append(f"Firebase export size: {human_bytes(fb_size)} (approximate JSON export)")
    else:
        lines.append('Firebase export size: unavailable')

    gh_size = fetch_github_repo_size()
    if gh_size is not None:
        lines.append(f"GitHub repository size (source files): {human_bytes(gh_size)}")
    else:
        lines.append('GitHub repository size: unavailable (set GITHUB_TOKEN to query)')

    lines.append('GitHub Actions usage: base free tier offers 2,000 minutes/month; current workflows typically consume <5 minutes per full run. Check https://github.com/settings/billing for exact usage.')

    return lines


def main() -> None:
    lines = build_summary()
    SUMMARY_PATH.write_text('\n'.join(lines) + '\n', encoding='utf-8')
    print('\n'.join(lines))


if __name__ == '__main__':
    main()
