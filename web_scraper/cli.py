#!/usr/bin/env python3
import argparse
import logging
import sys
import os
import json
from typing import Dict, List, Any, Optional
import importlib.util

from web_scraper.core.scraper import Scraper
from web_scraper.parsers.html_parser import HTMLParser
from web_scraper.parsers.js_parser import JSParser
from web_scraper.database.data_processor import DataProcessor
from web_scraper.scheduler.cron_scheduler import CronScheduler
from web_scraper.utils.parallel_processor import ParallelProcessor


def setup_logging(verbose: bool = False):
    """Set up logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Setup console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Setup file handler
    file_handler = logging.FileHandler('scraper.log')
    file_handler.setFormatter(formatter)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)


def load_config(config_file: str) -> Dict[str, Any]:
    """
    Load configuration from a JSON file.
    
    Args:
        config_file: Path to the configuration file
        
    Returns:
        The configuration as a dictionary
    """
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        logging.info(f"Loaded configuration from {config_file}")
        return config
    except Exception as e:
        logging.error(f"Error loading configuration: {str(e)}")
        sys.exit(1)


def load_custom_parser(parser_file: str) -> Any:
    """
    Load a custom parser module.
    
    Args:
        parser_file: Path to the Python file with the parser
        
    Returns:
        The parser class or function
    """
    try:
        module_name = os.path.basename(parser_file).replace('.py', '')
        spec = importlib.util.spec_from_file_location(module_name, parser_file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        # Look for a class named "Parser" or a function named "parse"
        if hasattr(module, 'Parser'):
            return module.Parser
        elif hasattr(module, 'parse'):
            return module.parse
        else:
            logging.error(f"No Parser class or parse function found in {parser_file}")
            sys.exit(1)
    except Exception as e:
        logging.error(f"Error loading custom parser: {str(e)}")
        sys.exit(1)


def run_scraper(args):
    """
    Run the scraper with the given arguments.
    
    Args:
        args: Command-line arguments
    """
    # Load configuration if provided
    config = {}
    if args.config:
        config = load_config(args.config)
    
    # Set up parameters from args or config
    url_list = []
    
    # Get URLs from file if provided
    if args.url_file:
        parallel_processor = ParallelProcessor(
            max_workers=args.max_workers,
            timeout=args.timeout
        )
        url_list = parallel_processor.read_urls_from_file(args.url_file)
    # Otherwise use URLs from command line or config
    else:
        url_list = args.url or config.get('urls', [])
        if isinstance(url_list, str):
            url_list = [url_list]
        
    output_dir = args.output_dir or config.get('output_dir', './data')
    output_format = args.output_format or config.get('output_format', 'json')
    rate_limit = args.rate_limit or config.get('rate_limit', 1.0)
    respect_robots = not args.ignore_robots and config.get('respect_robots_txt', True)
    use_selenium = args.selenium or config.get('use_selenium', False)
    max_retries = args.max_retries or config.get('max_retries', 3)
    verify_ssl = not args.no_verify_ssl
    
    # Initialize scraper and parsers
    scraper = Scraper(
        rate_limit=rate_limit,
        respect_robots_txt=respect_robots,
        max_retries=max_retries,
        verify_ssl=verify_ssl
    )
    
    html_parser = HTMLParser()
    
    # Initialize Selenium parser if needed
    js_parser = None
    if use_selenium:
        js_parser = JSParser(headless=not args.no_headless)
    
    # Initialize data processor
    data_processor = DataProcessor(output_dir=output_dir)
    
    # Load custom parser if provided
    custom_parser = None
    if args.custom_parser:
        custom_parser = load_custom_parser(args.custom_parser)
    
    # If using parallel processing
    if args.parallel:
        results = run_parallel_scraper(
            url_list, 
            scraper, 
            html_parser, 
            js_parser, 
            custom_parser, 
            args,
            max_workers=args.max_workers,
            batch_delay=args.batch_delay
        )
    else:
        results = run_sequential_scraper(
            url_list, 
            scraper, 
            html_parser, 
            js_parser, 
            custom_parser, 
            args
        )
    
    # Clean and process the data
    cleaned_results = data_processor.clean_data(
        results,
        remove_duplicates=True,
        fill_missing=True
    )
    
    # Save the results
    if output_format.lower() == 'json':
        output_file = args.output_file or 'scraped_data.json'
        data_processor.save_to_json(cleaned_results, output_file)
    elif output_format.lower() == 'csv':
        output_file = args.output_file or 'scraped_data.csv'
        data_processor.save_to_csv(cleaned_results, output_file)
    elif output_format.lower() == 'sqlite':
        output_file = args.output_file or 'scraped_data.db'
        table_name = args.table_name or 'scraped_data'
        data_processor.save_to_sqlite(cleaned_results, output_file, table_name)
    else:
        logging.error(f"Unsupported output format: {output_format}")
        
    # Clean up resources
    scraper.close()
    if js_parser:
        js_parser.close()
        
    logging.info(f"Scraping completed. Processed {len(url_list)} URLs, extracted {len(cleaned_results)} results.")


def schedule_scraper(args):
    """
    Schedule the scraper using cron.
    
    Args:
        args: Command-line arguments
    """
    scheduler = CronScheduler()
    
    # Build the command to run the scraper
    command_args = []
    command_args.append(sys.executable)
    command_args.append(os.path.abspath(__file__))
    command_args.append("run")
    
    # Add all the original arguments except 'schedule' and 'command'
    if args.config:
        command_args.append(f"--config={args.config}")
    if args.url:
        for url in args.url:
            command_args.append(f"--url={url}")
    if args.output_dir:
        command_args.append(f"--output-dir={args.output_dir}")
    if args.output_file:
        command_args.append(f"--output-file={args.output_file}")
    if args.output_format:
        command_args.append(f"--output-format={args.output_format}")
    if args.rate_limit:
        command_args.append(f"--rate-limit={args.rate_limit}")
    if args.ignore_robots:
        command_args.append("--ignore-robots")
    if args.selenium:
        command_args.append("--selenium")
    if args.no_headless:
        command_args.append("--no-headless")
    if args.max_retries:
        command_args.append(f"--max-retries={args.max_retries}")
    if args.custom_parser:
        command_args.append(f"--custom-parser={args.custom_parser}")
    if args.extract_text:
        command_args.append("--extract-text")
    if args.extract_links:
        command_args.append("--extract-links")
    if args.extract_tables:
        command_args.append("--extract-tables")
    if args.extract_metadata:
        command_args.append("--extract-metadata")
    if args.selector:
        command_args.append(f"--selector={args.selector}")
    if args.fail_fast:
        command_args.append("--fail-fast")
    if args.verbose:
        command_args.append("--verbose")
    
    # Join the command
    command = " ".join(command_args)
    
    # Schedule the job
    if scheduler.create_scraper_job(args.schedule, command, job_name=args.job_name):
        logging.info(f"Scraper scheduled with cron expression: {args.schedule}")
    else:
        logging.error("Failed to schedule scraper")
        sys.exit(1)


def run_parallel_scraper(url_list, scraper, html_parser, js_parser, custom_parser, args, 
                         max_workers=5, batch_delay=1.0):
    """
    Run the scraper in parallel mode.
    
    Args:
        url_list: List of URLs to scrape
        scraper: Scraper instance
        html_parser: HTMLParser instance
        js_parser: JSParser instance or None
        custom_parser: Custom parser or None
        args: Command-line arguments
        max_workers: Maximum number of worker threads
        batch_delay: Delay between batches in seconds
        
    Returns:
        List of scraped data
    """
    def process_url(url):
        logging.info(f"Processing URL: {url}")
        
        try:
            # Check if we need Selenium
            if js_parser and args.selenium:
                logging.info(f"Using Selenium to load: {url}")
                html_content = js_parser.load_page(url)
            else:
                # Use regular HTTP request
                response = scraper.get(url)
                html_content = response.text
            
            # Parse the content
            parsed_data = {}
            
            # Apply custom parser if provided
            if custom_parser:
                if callable(custom_parser):
                    parsed_data = custom_parser(html_content, url)
                else:
                    parser_instance = custom_parser()
                    parsed_data = parser_instance.parse(html_content, url)
            else:
                # Use comprehensive extraction if requested
                if args.extract_all:
                    parsed_data = html_parser.extract_all_data(html_content)
                    # Always add URL
                    parsed_data['url'] = url
                else:
                    # Use default parsing based on command-line arguments
                    soup = html_parser.parse(html_content)
                    
                    # Extract text if requested
                    if args.extract_text:
                        parsed_data['text'] = html_parser.extract_text(html_content, args.selector)
                    
                    # Extract links if requested
                    if args.extract_links:
                        parsed_data['links'] = html_parser.extract_links(html_content, url)
                    
                    # Extract tables if requested
                    if args.extract_tables:
                        parsed_data['tables'] = html_parser.extract_table(html_content, args.selector)
                    
                    # Extract metadata if requested
                    if args.extract_metadata:
                        parsed_data['metadata'] = html_parser.extract_metadata(html_content)
                    
                    # If no specific extraction was requested, extract everything
                    if not any([args.extract_text, args.extract_links, args.extract_tables, args.extract_metadata, args.extract_all]):
                        parsed_data = {
                            'url': url,
                            'title': soup.title.get_text() if soup.title else '',
                            'text': html_parser.extract_text(html_content),
                            'links': html_parser.extract_links(html_content, url),
                            'metadata': html_parser.extract_metadata(html_content)
                        }
            
            # Add the URL to the parsed data
            if 'url' not in parsed_data:
                parsed_data['url'] = url
                
            return parsed_data
            
        except Exception as e:
            logging.error(f"Error processing URL {url}: {str(e)}")
            return {'url': url, 'error': str(e)}
    
    # Use the ParallelProcessor to process URLs in parallel
    parallel_processor = ParallelProcessor(max_workers=max_workers)
    return parallel_processor.process_urls(url_list, process_url, delay_between_batches=batch_delay)


def run_sequential_scraper(url_list, scraper, html_parser, js_parser, custom_parser, args):
    """
    Run the scraper in sequential mode.
    
    Args:
        url_list: List of URLs to scrape
        scraper: Scraper instance
        html_parser: HTMLParser instance
        js_parser: JSParser instance or None
        custom_parser: Custom parser or None
        args: Command-line arguments
        
    Returns:
        List of scraped data
    """
    results = []
    
    for url in url_list:
        logging.info(f"Processing URL: {url}")
        
        try:
            # Check if we need Selenium
            if js_parser and args.selenium:
                logging.info(f"Using Selenium to load: {url}")
                html_content = js_parser.load_page(url)
            else:
                # Use regular HTTP request
                response = scraper.get(url)
                html_content = response.text
            
            # Parse the content
            parsed_data = {}
            
            # Apply custom parser if provided
            if custom_parser:
                if callable(custom_parser):
                    parsed_data = custom_parser(html_content, url)
                else:
                    parser_instance = custom_parser()
                    parsed_data = parser_instance.parse(html_content, url)
            else:
                # Use comprehensive extraction if requested
                if args.extract_all:
                    parsed_data = html_parser.extract_all_data(html_content)
                    # Always add URL
                    parsed_data['url'] = url
                else:
                    # Use default parsing based on command-line arguments
                    soup = html_parser.parse(html_content)
                    
                    # Extract text if requested
                    if args.extract_text:
                        parsed_data['text'] = html_parser.extract_text(html_content, args.selector)
                    
                    # Extract links if requested
                    if args.extract_links:
                        parsed_data['links'] = html_parser.extract_links(html_content, url)
                    
                    # Extract tables if requested
                    if args.extract_tables:
                        parsed_data['tables'] = html_parser.extract_table(html_content, args.selector)
                    
                    # Extract metadata if requested
                    if args.extract_metadata:
                        parsed_data['metadata'] = html_parser.extract_metadata(html_content)
                    
                    # If no specific extraction was requested, extract everything
                    if not any([args.extract_text, args.extract_links, args.extract_tables, args.extract_metadata, args.extract_all]):
                        parsed_data = {
                            'url': url,
                            'title': soup.title.get_text() if soup.title else '',
                            'text': html_parser.extract_text(html_content),
                            'links': html_parser.extract_links(html_content, url),
                            'metadata': html_parser.extract_metadata(html_content)
                        }
            
            # Add the URL to the parsed data
            if 'url' not in parsed_data:
                parsed_data['url'] = url
                
            # Add the result
            results.append(parsed_data)
            
        except Exception as e:
            logging.error(f"Error processing URL {url}: {str(e)}")
            if args.fail_fast:
                break
    
    return results


def main():
    """
    Main entry point for the CLI.
    """
    parser = argparse.ArgumentParser(description="Web Scraper for Immigration Data Collection")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Run command
    run_parser = subparsers.add_parser("run", help="Run the scraper")
    
    # Basic configuration
    run_parser.add_argument("--config", help="Path to configuration file")
    run_parser.add_argument("--url", action="append", help="URL to scrape (can be specified multiple times)")
    run_parser.add_argument("--url-file", help="Path to a file containing URLs to scrape (one URL per line)")
    run_parser.add_argument("--output-dir", help="Directory to save output files")
    run_parser.add_argument("--output-file", help="Output file name (without extension)")
    run_parser.add_argument("--output-format", choices=["json", "csv", "sqlite"], help="Output format")
    run_parser.add_argument("--table-name", help="Table name for SQLite output")
    
    # Parallel processing options
    run_parser.add_argument("--parallel", action="store_true", help="Process URLs in parallel")
    run_parser.add_argument("--max-workers", type=int, default=5, help="Maximum number of parallel workers (default: 5)")
    run_parser.add_argument("--batch-delay", type=float, default=1.0, help="Delay between batches in seconds (default: 1.0)")
    run_parser.add_argument("--timeout", type=int, default=120, help="Timeout for each URL in seconds (default: 120)")
    
    # Scraper settings
    run_parser.add_argument("--rate-limit", type=float, help="Rate limit in seconds between requests")
    run_parser.add_argument("--ignore-robots", action="store_true", help="Ignore robots.txt rules")
    run_parser.add_argument("--selenium", action="store_true", help="Use Selenium for JavaScript rendering")
    run_parser.add_argument("--no-headless", action="store_true", help="Don't run Selenium in headless mode")
    run_parser.add_argument("--max-retries", type=int, help="Maximum number of retries for failed requests")
    run_parser.add_argument("--no-verify-ssl", action="store_true", help="Disable SSL certificate verification")
    
    # Parser settings
    run_parser.add_argument("--custom-parser", help="Path to custom parser Python file")
    run_parser.add_argument("--extract-text", action="store_true", help="Extract text content")
    run_parser.add_argument("--extract-links", action="store_true", help="Extract links")
    run_parser.add_argument("--extract-tables", action="store_true", help="Extract tables")
    run_parser.add_argument("--extract-metadata", action="store_true", help="Extract metadata")
    run_parser.add_argument("--extract-all", action="store_true", help="Extract all content types comprehensively")
    run_parser.add_argument("--selector", help="CSS selector for targeting specific elements")
    
    # Other settings
    run_parser.add_argument("--fail-fast", action="store_true", help="Stop on first error")
    run_parser.add_argument("--verbose", action="store_true", help="Verbose output")
    
    # Schedule command
    schedule_parser = subparsers.add_parser("schedule", help="Schedule the scraper")
    schedule_parser.add_argument("schedule", help="Cron schedule expression (e.g., '0 0 * * *' for daily at midnight)")
    schedule_parser.add_argument("--job-name", help="Name for the scheduled job")
    
    # Add the same arguments as the run command
    for action in run_parser._actions:
        if action.dest != 'command' and not action.dest == 'help':
            schedule_parser._add_action(action)
    
    # List jobs command
    list_parser = subparsers.add_parser("list-jobs", help="List scheduled jobs")
    
    # Remove job command
    remove_parser = subparsers.add_parser("remove-job", help="Remove a scheduled job")
    remove_parser.add_argument("job_pattern", help="Pattern to match in the job command")
    
    # Parse arguments
    args = parser.parse_args()
    
    # Set up logging
    setup_logging(args.verbose if hasattr(args, 'verbose') else False)
    
    # Run the appropriate command
    if args.command == "run":
        run_scraper(args)
    elif args.command == "schedule":
        schedule_scraper(args)
    elif args.command == "list-jobs":
        scheduler = CronScheduler()
        jobs = scheduler.list_cron_jobs()
        if jobs:
            print("Scheduled jobs:")
            for job in jobs:
                print(f"  {job}")
        else:
            print("No scheduled jobs found")
    elif args.command == "remove-job":
        scheduler = CronScheduler()
        if scheduler.remove_cron_job(args.job_pattern):
            print(f"Removed job(s) matching: {args.job_pattern}")
        else:
            print(f"Failed to remove job(s) matching: {args.job_pattern}")
    else:
        parser.print_help()


if __name__ == "__main__":
    main() 