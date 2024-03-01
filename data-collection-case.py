import httpx
from bs4 import BeautifulSoup
import polars as pl
import asyncio
from tqdm import tqdm
import anyio
import json

async def fetch_data(session, level, page):
    url = f'https://www.fortiguard.com/encyclopedia?type=ips&risk={level}&page={page}'
    try:
        response = await session.get(url, timeout=30)
        response.raise_for_status()  # Check for HTTP errors
        content = response.content
        return content
    
    except Exception as e:
        error_message = str(e)
        error_data = {
            "url": url,
            "error_message": error_message
        }
        with open("datasets/skipped.json", "w") as json_file:
            json.dump(error_data, json_file, indent=4)

        return None

async def parse_data(html_content):
    if html_content is None:
        return []

    soup = BeautifulSoup(html_content, "html.parser")
    body = soup.find('section', class_='table-body')
    container = body.find('div', class_='container')
    row_list = container.find_all('div',class_='row')

    data = []
    for row in row_list:
        title = row.find('div', class_='col-lg').text
        url = row.get('onclick')
        url = 'https://www.fortiguard.com' + url[url.find("'")+1:-1]

        data.append({
            'title':title,
            'url':url
        })
    return data

async def forti_scrape_async(level, max_page=10):
    async with httpx.AsyncClient() as client:
        tasks = []
        for page in tqdm(range(1, max_page + 1)):
            tasks.append(fetch_data(client, level, page))

        html_contents = await asyncio.gather(*tasks)

        tasks = []
        for html_content in html_contents:
            tasks.append(parse_data(html_content))

        parsed_data = await asyncio.gather(*tasks)
        return [item for sublist in parsed_data for item in sublist]

async def main_async():
    for level in range(1, 6):
        try:
            data = await forti_scrape_async(level=level)
            df = pl.DataFrame(data)

            # Write into csv file
            filename = f'forti_lists_{level}.csv'
            df.write_csv(f'datasets/{filename}')
        except anyio.EndOfStream as e:
            print(f"Error occurred during scraping: {e}")

if __name__ == "__main__":
    asyncio.run(main_async())
