import requests
from bs4 import BeautifulSoup
import time
import datetime
import pandas as pd
from sqlalchemy import create_engine
from paperswithcode import PapersWithCodeClient

# Configuration
BASE_URL = "https://paperswithcode.com/papers"  # Base URL for paper listings
PAGES_TO_SCRAPE = 10
DB_PATH = "trending_papers.db"  # SQLite database file path
TABLE_NAME = "trending_papers"

# Initialize the official client
client = PapersWithCodeClient()


def save_to_sqlite(df, db_path=DB_PATH, table_name=TABLE_NAME):
    """
    Saves the given DataFrame to a SQLite database table using pandas.
    If the table exists, appends new records; otherwise, creates the table.
    """
    engine = create_engine(f"sqlite:///{db_path}")
    df.to_sql(table_name, engine, if_exists='append', index=False)
    engine.dispose()


def get_current_trending(pages=PAGES_TO_SCRAPE):
    """
    Scrapes the current trending papers across the given number of pages and returns
    a DataFrame with columns ['id', 'spr', 'stars', 'timestamp'].
    - id: paper identifier from the URL
    - spr: stars per hour
    - stars: total number of stars
    - timestamp: Unix epoch time (UTC) for when the scrape was performed
    """
    records = []

    for page in range(1, pages + 1):
        url = f"{BASE_URL}?page={page}"
        response = requests.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        # Remove unwanted elements
        for tag in soup.find_all(["head", "header", "footer", "nav", "svg", "img"]):
            tag.decompose()

        # Find each paper card
        cards = soup.find_all("div", class_="paper-card infinite-item")
        for card in cards:
            # Extract paper ID
            img_col = card.find("div", class_="col-lg-3 item-image-col")
            paper_id = None
            if img_col:
                link = img_col.find("a", href=True)
                if link and link["href"].startswith("/paper/"):
                    paper_id = link["href"].split("/paper/")[-1]

            # Extract stars per hour
            stars_acc = card.find("div", class_="stars-accumulated text-center")
            spr = stars_acc.get_text(strip=True) if stars_acc else None

            # Extract total stars
            badge = card.find("span", class_="badge badge-secondary")
            stars = badge.get_text(strip=True) if badge else None

            if paper_id:
                records.append({
                    'id': paper_id,
                    'spr': spr,
                    'stars': stars
                })

        # Politeness delay
        time.sleep(1)

    # Build DataFrame and add timestamp
    df = pd.DataFrame(records)
    df['timestamp'] = int(datetime.datetime.utcnow().timestamp())
    return df


def get_paper_metadata(paper_id):
    """
    Retrieves metadata for a given paper_id using the official PapersWithCodeClient.
    Returns a dict containing:
      - title: paper title
      - arxiv_id: associated ArXiv identifier (if any)
      - url_pdf: direct PDF URL (if available)
      - abstract: paper abstract text
      - published: publication date
    """
    paper_obj = client.paper_get(paper_id)
    return {
        'title': paper_obj.title,
        'arxiv_id': paper_obj.arxiv_id,
        'url_pdf': paper_obj.url_pdf,
        'abstract': paper_obj.abstract,
        'published': paper_obj.published
    }


if __name__ == "__main__":
    # Fetch trending papers
    df = get_current_trending()

    # Initialize metadata columns
    metadata_cols = ['title', 'arxiv_id', 'url_pdf', 'abstract', 'published']
    for col in metadata_cols:
        df[col] = None

    # Populate metadata for each paper
    if not df.empty:
        for idx, row in df.iterrows():
            paper_id = row['id']
            try:
                paper_data = get_paper_metadata(paper_id)
                for col in metadata_cols:
                    df.at[idx, col] = paper_data.get(col)
            except Exception as e:
                print(f"Error fetching metadata for {paper_id}: {e}")

    # Save to SQLite (append or create)
    save_to_sqlite(df)
    print(f"Appended {len(df)} records to SQLite database table '{TABLE_NAME}' in '{DB_PATH}'.")
