# Immigration Data Web Scraper

A Python-based web scraper designed for collecting immigration-related data for machine learning training. This scraper is built with ethical web scraping principles in mind, respecting website terms of service and robots.txt directives.

## Features

- **Robots.txt Compliance**: Automatically parses and respects robots.txt directives
- **Rate Limiting**: Configurable rate limiting with exponential backoff to avoid overwhelming servers
- **User-Agent Rotation**: Rotates User-Agent headers to mimic real browsers
- **Error Handling**: Robust error handling with retry logic
- **Multiple Parsers**: Support for both static (BeautifulSoup) and dynamic (Selenium) content
- **Data Cleaning**: Cleans and normalizes scraped data (removing duplicates, filling missing values)
- **Multiple Output Formats**: Save data as CSV, JSON, or SQLite database
- **Scheduling**: Schedule scraping tasks with cron or Apache Airflow
- **Parallel Processing**: Process multiple URLs concurrently for faster scraping
- **Comprehensive Data Extraction**: Extract all possible content types and data structures
- **Extensible**: Easy to extend with custom parsers for specific websites

## Installation

### Prerequisites

- Python 3.8 or higher
- Chrome or Firefox browser (for Selenium functionality)

### Setup

1. Clone the repository:
   ```
   git clone https://github.com/sajid-karim/web-scraper.git
   cd web-scraper
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

3. For Selenium functionality, install the appropriate webdriver. The scraper uses `webdriver-manager` to handle this automatically.

## Command Reference

### Basic Commands

The scraper provides several commands for different operations:

```bash
# Run the scraper
python -m web_scraper.cli run [options]

# Schedule a scraping task with cron
python -m web_scraper.cli schedule "cron-expression" [options]

# List scheduled scraping jobs
python -m web_scraper.cli list-jobs

# Remove a scheduled job
python -m web_scraper.cli remove-job "job-pattern"
```

### URL Sources

Specify the URLs to scrape:

```bash
# Scrape a single URL
python -m web_scraper.cli run --url "https://example.com/immigration-page"

# Scrape multiple URLs
python -m web_scraper.cli run --url "https://example.com/page1" --url "https://example.com/page2"

# Scrape URLs from a file (one URL per line)
python -m web_scraper.cli run --url-file urls.txt

# Use a configuration file
python -m web_scraper.cli run --config config.json
```

### Parallel Processing

Process multiple URLs concurrently for faster scraping:

```bash
# Enable parallel processing
python -m web_scraper.cli run --url-file urls.txt --parallel

# Configure parallel processing
python -m web_scraper.cli run --url-file urls.txt --parallel --max-workers 5 --batch-delay 2.0 --timeout 180
```

### Content Extraction Options

Control what content is extracted:

```bash
# Extract specific content types
python -m web_scraper.cli run --url "https://example.com" --extract-text --extract-links --extract-tables --extract-metadata

# Extract everything (comprehensive data extraction)
python -m web_scraper.cli run --url "https://example.com" --extract-all

# Target specific elements with a CSS selector
python -m web_scraper.cli run --url "https://example.com" --extract-text --selector "article.main-content"
```

### Output Options

Configure the output format and location:

```bash
# Save output to a specific directory
python -m web_scraper.cli run --url "https://example.com" --output-dir "./data"

# Specify the output filename
python -m web_scraper.cli run --url "https://example.com" --output-file "immigration_data"

# Choose the output format
python -m web_scraper.cli run --url "https://example.com" --output-format json
python -m web_scraper.cli run --url "https://example.com" --output-format csv
python -m web_scraper.cli run --url "https://example.com" --output-format sqlite --table-name "immigration_data"
```

### Request Handling

Configure how requests are made:

```bash
# Set rate limiting between requests
python -m web_scraper.cli run --url "https://example.com" --rate-limit 2.0

# Ignore robots.txt rules (use with caution)
python -m web_scraper.cli run --url "https://example.com" --ignore-robots

# Set maximum retries for failed requests
python -m web_scraper.cli run --url "https://example.com" --max-retries 5

# Disable SSL certificate verification
python -m web_scraper.cli run --url "https://example.com" --no-verify-ssl
```

### JavaScript Rendering

Handle JavaScript-rendered content with Selenium:

```bash
# Enable Selenium for JavaScript rendering
python -m web_scraper.cli run --url "https://example.com" --selenium

# Disable headless mode (shows browser window)
python -m web_scraper.cli run --url "https://example.com" --selenium --no-headless
```

### Custom Parsers

Use custom parsers for specific websites:

```bash
# Use a custom parser
python -m web_scraper.cli run --url "https://www.uscis.gov/forms/all-forms" --custom-parser web_scraper/parsers/custom_parsers/uscis_parser.py
```

### Other Options

Additional options for controlling the scraper:

```bash
# Stop on first error
python -m web_scraper.cli run --url-file urls.txt --fail-fast

# Enable verbose logging
python -m web_scraper.cli run --url "https://example.com" --verbose
```

### Common Usage Examples

Extract all data from multiple immigration websites in parallel and save as JSON:

```bash
python -m web_scraper.cli run --url-file immigration_urls.txt --parallel --max-workers 3 --no-verify-ssl --extract-all --output-format json --output-file immigration_data
```

Extract only tables from JavaScript-rendered pages:

```bash
python -m web_scraper.cli run --url-file urls.txt --selenium --extract-tables --output-format csv
```

Schedule daily scraping at 2 AM:

```bash
python -m web_scraper.cli schedule "0 2 * * *" --url-file urls.txt --extract-all --output-format json
```

## Configuration File

For more complex scraping tasks, you can use a configuration file:

```bash
python -m web_scraper.cli run --config config.json
```

Example configuration file:

```json
{
  "urls": [
    "https://www.uscis.gov/humanitarian/refugees-and-asylum",
    "https://www.uscis.gov/forms/all-forms"
  ],
  "output_dir": "./immigration_data",
  "output_format": "json",
  "rate_limit": 2.0,
  "respect_robots_txt": true,
  "use_selenium": false,
  "max_retries": 5,
  "extract_options": {
    "extract_text": true,
    "extract_links": true,
    "extract_tables": true,
    "extract_metadata": true
  }
}
```

## Custom Parsers

You can create custom parsers for specific websites. Place your parser files in the `web_scraper/parsers/custom_parsers` directory.

Custom parsers should implement either a `Parser` class with a `parse` method or a `parse` function that takes HTML content and URL as arguments.

Example custom parser:

```python
class CustomParser:
    def parse(self, html_content, url):
        # Parse the HTML content
        # ...
        return parsed_data
```

## Ethical Use

This tool is designed for educational and research purposes. Please ensure your scraping activities comply with:

1. Website terms of service
2. Robots.txt directives
3. Legal requirements in your jurisdiction
4. Ethical data collection practices

## License

MIT License 