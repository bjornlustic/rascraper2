# RA.co Event Scraper

A Python tool to fetch event data from the RA.co GraphQL API and save it as JSON files. This tool accepts a start year and an end year as command-line arguments. It fetches events for each year within the specified range and saves them into individual JSON files. It also generates a summary of event counts per month and year.

## Features

- Fetches event data concurrently using `aiohttp`.
- Handles large datasets by breaking down requests by year, then by month. If monthly data exceeds 10,000 events, it further breaks it down bi-weekly.
- Saves events for each year in a separate JSON file (e.g., `events/events2023.json`).
- Generates an `event_statistics.json` file summarizing the number of events fetched per month for each year.

## Requirements

- Python 3.7 or higher
- `aiohttp`
- `calendar` (standard library)

Install dependencies using:
```bash
pip install -r requirements.txt
```

## Usage

Run the script from the command line, providing the start and end years for fetching events.

### Command-Line Arguments

- `start_year`: The start year for event listings (inclusive, integer format: `YYYY`).
- `end_year`: The end year for event listings (inclusive, integer format: `YYYY`).

### Example

To fetch events between 2022 and 2023:

```bash
python event_fetcher.py 2022 2023
```

This command will:
1. Fetch events for the year 2022 and save them to `events/events2022.json`.
2. Fetch events for the year 2023 and save them to `events/events2023.json`.
3. Create/update `event_statistics.json` with the count of events for each month of 2022 and 2023.

## Output Files

- `events/events{year}.json`: Contains a list of all events for the specified year. Each event object includes details like:
    - `id`
    - `contentUrl`
    - `title`
    - `date`
    - `venue` (id, name, address, capacity)
    - `country` (id, name)
    - `artists` (id, name, contentUrl, followerCount)
    - `genres` (id, name)
    - `cost`
    - `attending`
- `event_statistics.json`: A JSON file containing statistics about the fetched events, structured by year and month. Example structure:
    ```json
    {
      "2023": {
        "total_events": 1500,
        "months": {
          "01": 100,
          "02": 120,
          // ...
          "12": 150
        }
      },
      "2024": {
        // ...
      }
    }
    ```

## Notes

- The script will automatically create an `events/` directory if it does not already exist before saving the event files.
- The `CONCURRENT_REQUESTS` constant in the script can be adjusted to change the maximum number of concurrent API requests.
- The `DELAY` constant (currently 0) can be used to add a delay between requests if needed, though the primary rate-limiting safeguard is the `CONCURRENT_REQUESTS` semaphore.
- The script initially creates an `event_statistics.csv` file to keep track of monthly event counts during the fetching process, which is then converted to `event_statistics.json`, and the CSV file is deleted.