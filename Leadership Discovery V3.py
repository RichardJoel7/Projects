import csv
import codecs
import aiohttp
import asyncio
import logging
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup, Comment
import unicodedata

# Logging setup
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s')

# Keywords
link_keywords = [
    'team', 'leader', 'management', 'executive', 'directors', 'senior', 'professional', 'mgmt',
    'staff', 'personnel', 'bod', 'corp', 'corporate', 'officer', 'imprint', 'firm', 'roster',
    'principals', 'profile', 'advisor', 'partners', 'advisors', 'board', 'founder', 'people',
    'supervisory', 'who-we', 'who we'
]
exclude_keywords = [
    'press', 'news', 'blog', 'media', 'events', 'careers', 'jobs', 'diversity', 'innovation',
    'sustainablity', 'sitemap', 'meeting', 'privacy', 'terms', 'cookie', 'main', 'footer', 
    'article', 'archive', 'awards', 'announce', 'appoint', 'report', 'document'
]
content_keywords = [
    'ceo', 'chair', 'president', 'chief', 'officer', 'vp', 'trustee', 'director', 'secretary', 'treasurer'
]

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
}

output_data = []
written_urls = set()  # Deduplication set

def normalize(text):
    return unicodedata.normalize("NFKD", text).lower().strip()

def is_valid_leadership_url(url):
    parsed = urlparse(url)

    if parsed.fragment or '#' in url:
        return False
    if parsed.path.lower().endswith('.pdf'):
        return False
    if any(ex in parsed.path.lower() for ex in exclude_keywords):
        return False
    if any(ex in parsed.fragment.lower() for ex in exclude_keywords):
        return False

    last_segment = parsed.path.strip('/').split('/')[-1].lower()
    return any(kw in last_segment for kw in link_keywords)

def get_visible_text(soup):
    for tag in soup(['style', 'script', 'head', 'title', 'meta', '[document]', 'nav', 'footer']):
        tag.decompose()

    for tag in soup.find_all(attrs={"aria-hidden": "true"}): tag.decompose()
    for tag in soup.find_all(style=lambda v: v and ("display:none" in v or "visibility:hidden" in v)): tag.decompose()
    for tag in soup.find_all("div", {"hidden": True}): tag.decompose()
    for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
        comment.extract()
    for a in soup.find_all('a'):
        a.unwrap()

    visible_text = soup.get_text(separator=' ', strip=True)
    return normalize(visible_text)

def content_has_keywords(html):
    soup = BeautifulSoup(html, 'lxml')
    visible_text = get_visible_text(soup)
    matched = [kw for kw in content_keywords if kw in visible_text]
    return matched

async def fetch(session, url):
    try:
        async with session.get(url, headers=headers, timeout=15, ssl=False) as resp:
            logging.info(f"Processing: {url}")
            if resp.status == 200:
                html = await resp.text()
                return html
    except Exception as e:
        logging.warning(f"Failed {url}: {e}")
    return None

def extract_internal_links(base_url, soup):
    parsed_base = urlparse(base_url)
    domain = parsed_base.netloc
    links = set()

    for a in soup.find_all('a', href=True):
        href = a['href'].strip()
        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)

        if parsed.netloc != domain:
            continue
        if any(ex in parsed.path.lower() for ex in exclude_keywords):
            continue  # Skip if it contains excluded keyword
        links.add(full_url)
    return links

async def process_domain(matrix_id, domain_url, max_depth=1):
    visited = set()
    all_links = set()
    found_any = False

    session_timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=session_timeout) as session:
        queue = [(domain_url, 0)]

        while queue:
            current_url, depth = queue.pop(0)
            if current_url in visited or depth > max_depth:
                continue
            if any(ex in urlparse(current_url).path.lower() for ex in exclude_keywords):
                continue  # Prevent even crawling excluded pages

            visited.add(current_url)

            html = await fetch(session, current_url)
            if not html:
                continue

            soup = BeautifulSoup(html, 'lxml')
            internal_links = extract_internal_links(current_url, soup)
            all_links.update(internal_links)

            if depth < max_depth:
                for link in internal_links:
                    queue.append((link, depth + 1))

        for link in all_links:
            if link in written_urls:
                continue
            if any(ex in urlparse(link).path.lower() for ex in exclude_keywords):
                continue  # Double-check exclusion before final processing
            if is_valid_leadership_url(link):
                written_urls.add(link)
                link_html = await fetch(session, link)
                matched_keywords = content_has_keywords(link_html) if link_html else []
                output_data.append([
                    matrix_id,
                    domain_url,
                    link,
                    'Yes',
                    'Yes' if matched_keywords else 'No',
                    '-'.join(matched_keywords) if matched_keywords else ''
                ])
                found_any = True

        if not found_any:
            output_data.append([
                matrix_id,
                domain_url,
                'No URLs matching filters',
                'No',
                'No',
                ''
            ])

async def main():
    input_file = 'input.csv'
    with codecs.open(input_file, 'r', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        tasks = []
        for row in reader:
            row = {k.strip(): v.strip() for k, v in row.items()}
            matrix_id = row['Matrix ID']
            domain_url = row['Domain URL'].rstrip('/')
            tasks.append(process_domain(matrix_id, domain_url, max_depth=1))

    await asyncio.gather(*tasks)

    with open('LE Discovery Analysis Output.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Matrix ID', 'Domain URL', 'Leadership Page URL',
            'Leadership URL Match', 'Content Match', 'Matched Keywords'
        ])
        writer.writerows(output_data)

if __name__ == '__main__':
    asyncio.run(main())