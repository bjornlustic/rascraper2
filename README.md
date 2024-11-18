# RA.co Event Fetcher

A Python tool to fetch event data from the RA.co GraphQL API and save it as a JSON file. This tool accepts start date, and end date as command-line arguments and saves the fetched events to a JSON file by default.

Also includes a Node.JS file called filecombiner.js that automatically combines every json file in the directory that it is run in.

## Requirements

- Python 3.6 or higher
- requests library (pip install requests)
- pandas library (pip install pandas)

## Installation

1. Clone the repository or download the source code.
2. Run pip install -r requirements.txt to install the required libraries.

## Usage

### Command-Line Arguments

- `start_date`: The start date for event listings (inclusive, format: `YYYY-MM-DD`).
- `end_date`: The end date for event listings (inclusive, format: `YYYY-MM-DD`).
- `-j`: The output file path (default: `events.json`).

### Example

To fetch events between January 01, 2001, and December 31, 2001, and save them to a JSON file named `events.json`, run the following command:

```
python event_fetcher.py 2001-01-01 2001-12-31 -j events.json
```

To combines all files:

```
node filecombiner.js
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