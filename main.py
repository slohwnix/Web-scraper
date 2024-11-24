import threading
import requests
from bs4 import BeautifulSoup
from queue import Queue
from urllib.parse import urljoin
import os
import sqlite3

DB_NAME = "web_data.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE,
            title TEXT,
            description TEXT,
            logo TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS keywords (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            page_id INTEGER,
            keyword TEXT,
            FOREIGN KEY (page_id) REFERENCES pages (id)
        )
    """)
    conn.commit()
    conn.close()

def add_page(url, title, description, logo):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR IGNORE INTO pages (url, title, description, logo)
        VALUES (?, ?, ?, ?)
    """, (url, title, description, logo))
    page_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return page_id

def add_keywords(page_id, keywords):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.executemany("""
        INSERT INTO keywords (page_id, keyword)
        VALUES (?, ?)
    """, [(page_id, keyword) for keyword in keywords])
    conn.commit()
    conn.close()

links_queue = Queue()
visited_urls = set()

def extract_keywords(text):
    words = text.lower().split()
    stopwords = {"et", "le", "la", "les", "de", "des", "un", "une", "à", "en", "du", "pour", "par", "sur"}
    return set(word.strip('.,!?()"') for word in words if word not in stopwords and len(word) > 2)
def get_links_and_metadata():
    while True:
        url = links_queue.get()
        if url is None:
            break
        try:
            if url in visited_urls:
                links_queue.task_done()
                continue

            visited_urls.add(url)

            if not url.startswith('http://') and not url.startswith('https://'):
                url = 'https://' + url

            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')

                title = soup.title.string.strip() if soup.title else "Titre non disponible"
                meta_description = soup.find('meta', attrs={'name': 'description'})
                description = meta_description['content'].strip() if meta_description else "Description non disponible"
                favicon = soup.find('link', rel=lambda x: x and 'icon' in x.lower())
                logo_url = urljoin(url, favicon['href']) if favicon else "Logo non disponible"

                page_id = add_page(url, title, description, logo_url)

                page_text = soup.get_text(separator=" ")
                keywords = extract_keywords(page_text)
                if page_id:
                    add_keywords(page_id, keywords)

                links = soup.find_all('a', href=True)
                for link in links:
                    full_url = urljoin(url, link['href'])
                    if full_url not in visited_urls:
                        links_queue.put(full_url)

                print(f"Informations récupérées pour {url}")
        except Exception as e:
            print(f"Erreur lors de la récupération des informations depuis {url}: {e}")
        finally:
            links_queue.task_done()

def main():
    init_db()

    link_metadata_thread = threading.Thread(target=get_links_and_metadata)
    link_metadata_thread.start()

    initial_urls = [
        "https://example.com" # url here
    ]

    for url in initial_urls:
        links_queue.put(url)

    links_queue.join()
    links_queue.put(None)
    link_metadata_thread.join()

    print("Scraping terminé.")

if __name__ == "__main__":
    main()
