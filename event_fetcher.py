import requests
import json
import sys
import argparse
import asyncio
import aiohttp
from datetime import datetime, timedelta
from aiohttp import ClientSession

URL = 'https://ra.co/graphql'
HEADERS = {
    'Content-Type': 'application/json',
    'Referer': 'https://ra.co/events/us/sanfrancisco',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0'
}
DELAY = 1  # Adjust as needed
CONCURRENT_REQUESTS = 10  # Max concurrent requests

class EventFetcher:
    def __init__(self):
        pass

    @staticmethod
    def generate_payload(listing_date_gte, listing_date_lte):
        payload = {
            "operationName": "GET_EVENT_LISTINGS",
            "variables": {
                "filters": {
                    "listingDate": {
                        "gte": listing_date_gte,
                        "lte": listing_date_lte
                    }
                },
                "pageSize": 100,
                "page": 1
            },
            "query": """
            query GET_EVENT_LISTINGS($filters: FilterInputDtoInput, $page: Int, $pageSize: Int) {
                eventListings(filters: $filters, pageSize: $pageSize, page: $page) {
                    data {
                        id
                        listingDate
                        event {
                            id
                            title
                            date
                            startTime
                            endTime
                            venue {
                                id
                                name
                            }
                            artists {
                                id
                                name
                            }
                            genres {
                                id
                                name
                            }
                            cost
                            isFestival
                            lineup
                            attending
                        }
                    }
                    totalResults
                }
            }
            """
        }
        return payload

    async def fetch_page(self, session, payload, page_number, semaphore):
        async with semaphore:
            payload["variables"]["page"] = page_number
            async with session.post(URL, headers=HEADERS, json=payload) as response:
                try:
                    data = await response.json()
                    if 'data' in data and 'eventListings' in data['data']:
                        events = data["data"]["eventListings"]["data"]
                        total_results = data["data"]["eventListings"]["totalResults"]
                        return events, total_results
                except Exception as e:
                    print(f"Error fetching page {page_number}: {e}")
                return [], 0

    async def fetch_all_pages(self, session, payload, total_results, semaphore):
        all_events = []
        total_pages = (total_results // 100) + 1
        tasks = []

        for page_number in range(1, total_pages + 1):
            task = asyncio.ensure_future(self.fetch_page(session, payload, page_number, semaphore))
            tasks.append(task)

        responses = await asyncio.gather(*tasks)
        for events, _ in responses:
            all_events.extend(events)

        return all_events

    async def fetch_weekly_data(self, session, start_date, end_date, semaphore):
        all_events = []
        date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        current_start = start_date

        # Split into weekly intervals
        while current_start < end_date:
            current_end = min(current_start + timedelta(days=13), end_date)

            gte = current_start.strftime(date_format)
            lte = current_end.strftime(date_format)
            print(f"\nFetching events from {gte} to {lte}...")

            payload = self.generate_payload(gte, lte)
            initial_events, total_results = await self.fetch_page(session, payload, 1, semaphore)
            weekly_events = await self.fetch_all_pages(session, payload, total_results, semaphore)

            # Combine initial and subsequent page events for the current week
            combined_events = initial_events + weekly_events
            all_events.extend(combined_events)

            # Print the total number of events fetched for the current week
            print(f"Total events for this week: {len(combined_events)}")

            current_start = current_end + timedelta(days=1)

        return all_events


    async def fetch_yearly_events(self, start_year, end_year):
        all_events_by_year = {}
        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

        async with ClientSession() as session:
            for year in range(start_year, end_year + 1):
                print(f"\nFetching events for year {year}...")
                year_start = datetime(year, 1, 1)
                year_end = datetime(year, 12, 31)

                events = await self.fetch_weekly_data(session, year_start, year_end, semaphore)
                all_events_by_year[year] = events

                print(f"Total events fetched for {year}: {len(events)}")

        return all_events_by_year

    def save_events_to_json(self, all_events_by_year):
        for year, events in all_events_by_year.items():
            output_file = f"events/events{year}.json"
            with open(output_file, mode='w', encoding='utf-8') as json_file:
                json.dump(events, json_file, indent=2, ensure_ascii=False)
            print(f"Events for {year} saved to {output_file}.")

def main():
    parser = argparse.ArgumentParser(description="Fetch events from ra.co and save them to JSON files by year.")
    parser.add_argument("start_year", type=int, help="The start year for event listings (inclusive).")
    parser.add_argument("end_year", type=int, help="The end year for event listings (inclusive).")
    args = parser.parse_args()

    if args.start_year > args.end_year:
        print("Error: Start year must be less than or equal to end year.")
        sys.exit(1)

    event_fetcher = EventFetcher()
    print("Starting concurrent fetch for multiple years...")

    loop = asyncio.get_event_loop()
    all_events_by_year = loop.run_until_complete(event_fetcher.fetch_yearly_events(args.start_year, args.end_year))

    event_fetcher.save_events_to_json(all_events_by_year)

if __name__ == "__main__":
    main()
