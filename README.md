# RA.co Event Fetcher

A Python tool to fetch event data from the RA.co GraphQL API and save it as a JSON file. This tool accepts start year, and end year as command-line arguments and saves the fetched events to a JSON file by default.

## Requirements

- Python 3.6 or higher
- aiohttp==3.8.4

## Installation

1. Clone the repository or download the source code.
2. Run pip install -r requirements.txt to install the required libraries.


### Command-Line Arguments

- `start_year`: The start date for event listings (inclusive, format: `YYYY-MM-DD`).
- `end_year`: The end date for event listings (inclusive, format: `YYYY-MM-DD`).

### Example

To fetch events between 2001 and 2002 and save them to a JSON file named `events2001.json` and `events2002.json`, run the following command:

```
python event_fetcher.py 2001 2002
```

## Output

The fetched events will be saved to the specified output file (CSV by default) with the following columns:

- Event id
- Event title
- Start Time
- End Time
- Cost
- isFestival
- Area
- Country
- Venue
- viewCount
- Attending
- Artists
- Lineup
- Genres