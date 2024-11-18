import json
import sys
import argparse
import asyncio
import aiohttp
from datetime import datetime, timedelta
from aiohttp import ClientSession
import calendar

URL = 'https://ra.co/graphql'
HEADERS = {
    'Content-Type': 'application/json',
    'Referer': 'https://ra.co/events/us/sanfrancisco',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0'
}
DELAY = 0  # Adjust as needed
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

    async def fetch_events_by_interval(self, session, start_date, end_date, semaphore, interval_type):
        all_events = []
        date_format = "%Y-%m-%d"
        current_start = start_date

        # Determine the interval length based on the interval_type
        if interval_type == 'week':
            interval_length = 7  # 1 week
        elif interval_type == 'biweekly':
            interval_length = 14  # 2 weeks
        elif interval_type == 'month':
            interval_length = None  # Will calculate dynamically per month
        elif interval_type == 'year':
            interval_length = None  # Full year
        else:
            raise ValueError("Invalid interval_type. Use 'week', 'biweekly', 'month' or 'year'.")

        # Loop through intervals based on the specified type
        while current_start < end_date:

            if interval_type == 'month':
                # Get the number of days in the current month
                _, days_in_month = calendar.monthrange(current_start.year, current_start.month)
                current_end = min(current_start.replace(day=days_in_month), end_date)
            elif interval_type == 'year':
                # Use the entire year
                current_end = current_start.replace(month=12, day=31)
            else:
                # Calculate the end date based on the interval length
                current_end = min(current_start + timedelta(days=interval_length - 1), end_date)

            gte = current_start.strftime(date_format)
            lte = current_end.strftime(date_format)
            print(f"\nFetching events from {gte} to {lte}...")

            payload = self.generate_payload(gte, lte)
            initial_events, total_results = await self.fetch_page(session, payload, 1, semaphore)
            interval_events = await self.fetch_all_pages(session, payload, total_results, semaphore)

            # Combine initial and subsequent page events for the current interval
            combined_events = initial_events + interval_events
            all_events.extend(combined_events)

            # Print the total number of events fetched for the current interval
            print(f"Total events for this {interval_type}: {len(combined_events)}")

            # Handling Monthly events exceeding 10,000
            if interval_type == 'month' and len(combined_events) > 10000:
                print(f"Total events for this month exceed 10,000, breaking down biweekly...")

                # Switch to biweekly if monthly events exceed 10,000
                current_start = current_start.replace(day=1)  # Set to the first of the month
                while current_start <= current_end:
                    # Create biweekly intervals inside the current month
                    next_biweekly_end = min(current_start + timedelta(days=13), current_end)
                    gte = current_start.strftime(date_format)
                    lte = next_biweekly_end.strftime(date_format)
                    print(f"Fetching biweekly events from {gte} to {lte}...")

                    payload = self.generate_payload(gte, lte)
                    biweekly_events = await self.fetch_all_pages(session, payload, total_results, semaphore)

                    # Add the fetched biweekly events to the all_events list
                    all_events.extend(biweekly_events)

                    # Move to the next biweekly interval
                    current_start = next_biweekly_end + timedelta(days=1)

                # After biweekly fetching is done, move to the next month
                continue  # Continue with the next month processing

            # Move to the next month if monthly events do not exceed 10,000
            if interval_type == 'month' and len(all_events) <= 10000:
                # Only transition to the next month after month-based fetching
                next_month = current_start.month % 12 + 1
                
                if next_month == 1:  # Handle year transition (from December to January)
                    current_start = current_start.replace(year=current_start.year + 1, month=next_month, day=1)
                else:
                    current_start = current_start.replace(month=next_month, day=1)

            else:
                # For biweekly or weekly intervals, move to the next interval
                current_start = current_end + timedelta(days=1)

        return all_events

    async def fetch_events(self, start_date, end_date, semaphore, year):
        async with ClientSession() as session:
            all_events = await self.fetch_events_by_interval(session, start_date, end_date, semaphore, 'year')
            total_results = len(all_events)

            # If yearly events exceed 10,000, do not print the total
            # if total_results <= 10000:
                #print(f"Total events for the year {year}: {total_results}")

            # If yearly events exceed 10,000, break them down by month
            if total_results <= 10000:
                print("Total events in the year is below 10,000, no further breakdown needed.")
                return all_events
            else:
                print("Yearly events exceed 10,000, breaking down by month...")
                all_events = await self.fetch_events_by_interval(session, start_date, end_date, semaphore, 'month')
                return all_events

    def save_events_to_json(self, all_events, year):
        output_file = f"events/events{year}.json"  # Save the file with the year as part of the filename
        with open(output_file, mode='w', encoding='utf-8') as json_file:
            json.dump(all_events, json_file, indent=2, ensure_ascii=False)
        print(f"Events saved to {output_file}.")

def main():
    parser = argparse.ArgumentParser(description="Fetch events from ra.co and save them to JSON files.")
    parser.add_argument("start_year", type=int, help="The start year for event listings (inclusive).")
    parser.add_argument("end_year", type=int, help="The end year for event listings (inclusive).")
    args = parser.parse_args()

    if args.start_year > args.end_year:
        print("Error: Start year must be less than or equal to end year.")
        sys.exit(1)

    event_fetcher = EventFetcher()
    
    # Loop through each year in the specified range
    all_events = []
    for year in range(args.start_year, args.end_year + 1):
        print(f"Starting concurrent fetch for the year {year}...")
        year_start = datetime(year, 1, 1)
        year_end = datetime(year, 12, 31)
        
        events_for_year = asyncio.run(event_fetcher.fetch_events(year_start, year_end, asyncio.Semaphore(CONCURRENT_REQUESTS), year))
        all_events.extend(events_for_year)

        # Save events for this year separately
        event_fetcher.save_events_to_json(events_for_year, year)
    
    # Optionally, you can save all events into one combined file
    # event_fetcher.save_events_to_json(all_events, "combined")

if __name__ == "__main__":
    main()
