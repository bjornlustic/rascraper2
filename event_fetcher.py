import json
import csv
import os
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
        year = start_date.year
        month = start_date.month
        event_counter = 0

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

            if len(combined_events) < 10000:
                # Print the total number of events fetched for the current interval
                event_counter = len(combined_events)
                if interval_type == 'month':
                    print(f"MONTH: {current_start.month}")
                    EventFetcher.update_event_statistics(year, current_start.month, event_counter)
                print(f"Total events for this {interval_type}: {len(combined_events)}")
                event_counter = 0

            # Handling Monthly events exceeding 10,000
            if interval_type == 'month' and len(combined_events) > 10000:
                event_counter = 0
                print(f"Total events for this month exceed 10,000, breaking down biweekly...")

                # Switch to biweekly if monthly events exceed 10,000
                current_start = current_start.replace(day=1)  # Set to the first of the month
                while current_start <= current_end:
                    # Create biweekly intervals inside the current month
                    next_biweekly_end = min(current_start + timedelta(days=13), current_end)
                    gte = current_start.strftime(date_format)
                    lte = next_biweekly_end.strftime(date_format)
                    print(f"\nFetching biweekly events from {gte} to {lte}...")

                    payload = self.generate_payload(gte, lte)
                    biweekly_events = await self.fetch_all_pages(session, payload, total_results, semaphore)

                    # Add the fetched biweekly events to the all_events list
                    all_events.extend(biweekly_events)

                    # Move to the next biweekly interval
                    event_counter += len(biweekly_events)
                    print(f"MONTH: {current_start.month}")
                    EventFetcher.update_event_statistics(year, current_start.month, event_counter)
                    current_start = next_biweekly_end + timedelta(days=1)
                    print(f"Total events for this biweekly interval: {len(biweekly_events)}")
                    
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

            # Break them down by month
            print("Breaking down by month...")
            all_events = await self.fetch_events_by_interval(session, start_date, end_date, semaphore, 'month')
            return all_events

    def save_events_to_json(self, all_events, year):
        output_file = f"events/events{year}.json"  # Save the file with the year as part of the filename
        with open(output_file, mode='w', encoding='utf-8') as json_file:
            json.dump(all_events, json_file, indent=2, ensure_ascii=False)
        print(f"Events saved to {output_file}.")

    @staticmethod
    def update_event_statistics(year, month, num_events):
        # Define the CSV file path
        statistics_file = "event_statistics.csv"
        
        # Read the existing data (if any)
        rows = []
        file_exists = os.path.exists(statistics_file)
        
        # If the file exists, read the current data into a list of rows
        if file_exists:
            with open(statistics_file, mode='r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                rows = list(reader)

        # Check if the header exists and add it if not
        if len(rows) == 0 or rows[0] != ["Year", "Month", "Num_Events"]:
            rows.insert(0, ["Year", "Month", "Num_Events"])  # Insert header if missing

        # Flag to check if we found the row to update
        updated = False

        # Iterate through the rows and update the matching year and month
        for i, row in enumerate(rows[1:], 1):  # Start from 1 to skip the header
            existing_year, existing_month, _ = row
            if existing_year == str(year) and existing_month == f"{str(month).zfill(2)}":
                rows[i][2] = str(num_events)  # Update the num_events column
                updated = True
                break

        # If the year and month don't exist, append a new row
        if not updated:
            rows.append([str(year), f"{str(month).zfill(2)}", str(num_events)])

        # Write the updated data back to the CSV file
        with open(statistics_file, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(rows)  # Write all the rows, including the header

        print(f"Updated statistics: Year {year}, Month {str(month).zfill(2)}, Events: {num_events}")
    
    def convert_csv_to_json(csv_file, json_file):
        # Check if the CSV file exists
        if not os.path.exists(csv_file):
            print(f"Error: {csv_file} not found.")
            return

        # Initialize a dictionary to store the data
        data = {}

        # Read the CSV file
        with open(csv_file, mode='r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  # Read the header row

            # Ensure the header is correct
            if header != ["Year", "Month", "Num_Events"]:
                print("Error: CSV file does not have the expected header.")
                return

            # Process each row
            for row in reader:
                year, month, num_events = row
                year = int(year)  # Convert year to integer
                month = int(month)  # Convert month to integer
                num_events = int(num_events)  # Convert num_events to integer

                # Initialize the year if it doesn't exist
                if year not in data:
                    data[year] = {
                        "total_events": 0,
                        "months": {f"{str(i).zfill(2)}": 0 for i in range(1, 13)}
                    }

                # Update the statistics
                data[year]["total_events"] += num_events
                data[year]["months"][f"{str(month).zfill(2)}"] += num_events

        # Write the data to a JSON file
        with open(json_file, mode='w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        print(f"CSV data successfully converted to JSON and saved as {json_file}.")

def convert_csv_to_json(csv_file, json_file):
        # Check if the CSV file exists
        if not os.path.exists(csv_file):
            print(f"Error: {csv_file} not found.")
            return

        # Initialize a dictionary to store the data
        data = {}

        # Read the CSV file
        with open(csv_file, mode='r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  # Read the header row

            # Ensure the header is correct
            if header != ["Year", "Month", "Num_Events"]:
                print("Error: CSV file does not have the expected header.")
                return

            # Process each row
            for row in reader:
                year, month, num_events = row
                year = int(year)  # Convert year to integer
                month = int(month)  # Convert month to integer
                num_events = int(num_events)  # Convert num_events to integer

                # Initialize the year if it doesn't exist
                if year not in data:
                    data[year] = {
                        "total_events": 0,
                        "months": {f"{str(i).zfill(2)}": 0 for i in range(1, 13)}
                    }

                # Update the statistics
                data[year]["total_events"] += num_events
                data[year]["months"][f"{str(month).zfill(2)}"] += num_events

        # Write the data to a JSON file
        with open(json_file, mode='w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        if os.path.exists(csv_file):
            os.remove(csv_file)
            print(f"Deleted the CSV file: {csv_file}")

        print(f"CSV data successfully converted to JSON and saved as {json_file}.")

def main():

    parser = argparse.ArgumentParser(description="Fetch events from ra.co and save them to JSON files.")
    parser.add_argument("start_year", type=int, help="The start year for event listings (inclusive).")
    parser.add_argument("end_year", type=int, help="The end year for event listings (inclusive).")
    args = parser.parse_args()

    if args.start_year > args.end_year:
        print("Error: Start year must be less than or equal to end year.")
        sys.exit(1)

    event_fetcher = EventFetcher()

    if os.path.exists("event_statistics.csv"):
        os.remove("event_statistics.csv")
    
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

    convert_csv_to_json("event_statistics.csv", "event_statistics.json")

if __name__ == "__main__":
    main()
