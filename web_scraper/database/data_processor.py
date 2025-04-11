import logging
import json
import csv
import os
import pandas as pd
import sqlite3
from typing import List, Dict, Any, Optional, Union, Callable
from pathlib import Path

logger = logging.getLogger(__name__)


class DataProcessor:
    """
    Handles data cleaning, normalization, and storage.
    """
    def __init__(self, output_dir: str = "./data"):
        """
        Initialize the DataProcessor.
        
        Args:
            output_dir: Directory for storing output data (default: "./data")
        """
        self.output_dir = output_dir
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
    def clean_data(self, data: List[Dict[str, Any]], 
                  remove_duplicates: bool = True,
                  fill_missing: bool = True,
                  fill_value: Any = "",
                  required_fields: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Clean the scraped data.
        
        Args:
            data: The data to clean
            remove_duplicates: Whether to remove duplicate entries (default: True)
            fill_missing: Whether to fill missing values (default: True)
            fill_value: The value to use for filling missing values (default: "")
            required_fields: Optional list of required fields to check (default: None)
            
        Returns:
            The cleaned data
        """
        if not data:
            logger.warning("Empty data provided for cleaning")
            return []
            
        # Convert to DataFrame for easier manipulation
        df = pd.DataFrame(data)
        
        # Remove duplicates if requested
        if remove_duplicates and not df.empty:
            original_count = len(df)
            
            # Get columns with hashable types (exclude list, dict, etc.)
            hashable_columns = []
            for col in df.columns:
                # Skip columns with unhashable types like lists or dicts
                if (df[col].dropna().empty or
                    all(isinstance(x, (str, int, float, bool, type(None))) for x in df[col].dropna())):
                    hashable_columns.append(col)
            
            # Only drop duplicates based on hashable columns if any exist
            if hashable_columns:
                df = df.drop_duplicates(subset=hashable_columns)
                new_count = len(df)
                if original_count != new_count:
                    logger.info(f"Removed {original_count - new_count} duplicate entries")
            else:
                logger.warning("No hashable columns found for duplicate removal")
                
        # Fill missing values if requested
        if fill_missing and not df.empty:
            df = df.fillna(fill_value)
            
        # Check for required fields
        if required_fields and not df.empty:
            for field in required_fields:
                if field not in df.columns:
                    logger.warning(f"Required field '{field}' is missing from the data")
                else:
                    # Count rows with missing values for this field
                    missing_count = df[field].isna().sum()
                    if missing_count > 0:
                        logger.warning(f"Field '{field}' has {missing_count} missing values")
                        
        # Convert back to list of dictionaries
        cleaned_data = df.to_dict('records')
        return cleaned_data
        
    def normalize_text(self, text: str, 
                       lowercase: bool = True,
                       remove_extra_spaces: bool = True) -> str:
        """
        Normalize text content.
        
        Args:
            text: The text to normalize
            lowercase: Whether to convert to lowercase (default: True)
            remove_extra_spaces: Whether to remove extra whitespace (default: True)
            
        Returns:
            The normalized text
        """
        if not text:
            return ""
            
        # Convert to lowercase if requested
        if lowercase:
            text = text.lower()
            
        # Remove extra whitespace if requested
        if remove_extra_spaces:
            import re
            text = re.sub(r'\s+', ' ', text)
            text = text.strip()
            
        return text
        
    def apply_custom_transform(self, data: List[Dict[str, Any]], 
                              transform_func: Callable[[Dict[str, Any]], Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Apply a custom transformation function to each data item.
        
        Args:
            data: The data to transform
            transform_func: A function that takes a data item and returns a transformed version
            
        Returns:
            The transformed data
        """
        if not data:
            return []
            
        transformed_data = []
        for item in data:
            try:
                transformed_item = transform_func(item)
                transformed_data.append(transformed_item)
            except Exception as e:
                logger.error(f"Error applying transformation: {str(e)}")
                transformed_data.append(item)  # Keep the original item on error
                
        return transformed_data
        
    def save_to_json(self, data: List[Dict[str, Any]], 
                    filename: str, 
                    pretty: bool = True) -> str:
        """
        Save data to a JSON file.
        
        Args:
            data: The data to save
            filename: The filename to save to (without extension)
            pretty: Whether to format the JSON with indentation (default: True)
            
        Returns:
            The path to the saved file
        """
        if not filename.endswith('.json'):
            filename = f"{filename}.json"
            
        filepath = os.path.join(self.output_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                if pretty:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                else:
                    json.dump(data, f, ensure_ascii=False)
                    
            logger.info(f"Saved {len(data)} records to JSON file: {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Error saving to JSON: {str(e)}")
            raise
            
    def save_to_csv(self, data: List[Dict[str, Any]], 
                   filename: str,
                   delimiter: str = ',',
                   include_header: bool = True) -> str:
        """
        Save data to a CSV file.
        
        Args:
            data: The data to save
            filename: The filename to save to (without extension)
            delimiter: The CSV delimiter character (default: ',')
            include_header: Whether to include a header row (default: True)
            
        Returns:
            The path to the saved file
        """
        if not filename.endswith('.csv'):
            filename = f"{filename}.csv"
            
        filepath = os.path.join(self.output_dir, filename)
        
        try:
            if not data:
                logger.warning("No data to save to CSV")
                with open(filepath, 'w', newline='', encoding='utf-8') as f:
                    f.write('')
                return filepath
                
            # Get all possible fieldnames
            fieldnames = set()
            for item in data:
                fieldnames.update(item.keys())
            fieldnames = sorted(fieldnames)
            
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=delimiter)
                
                if include_header:
                    writer.writeheader()
                    
                writer.writerows(data)
                
            logger.info(f"Saved {len(data)} records to CSV file: {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Error saving to CSV: {str(e)}")
            raise
            
    def save_to_sqlite(self, data: List[Dict[str, Any]], 
                      db_file: str,
                      table_name: str,
                      if_exists: str = 'replace') -> str:
        """
        Save data to a SQLite database.
        
        Args:
            data: The data to save
            db_file: The database file name (without extension)
            table_name: The name of the table to save to
            if_exists: What to do if the table exists ('fail', 'replace', or 'append')
            
        Returns:
            The path to the saved database file
        """
        if not db_file.endswith('.db'):
            db_file = f"{db_file}.db"
            
        filepath = os.path.join(self.output_dir, db_file)
        
        try:
            if not data:
                logger.warning("No data to save to SQLite")
                conn = sqlite3.connect(filepath)
                conn.close()
                return filepath
                
            # Convert to DataFrame for easier saving
            df = pd.DataFrame(data)
            
            # Create database directory if it doesn't exist
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            # Create connection and save
            conn = sqlite3.connect(filepath)
            df.to_sql(table_name, conn, if_exists=if_exists, index=False)
            conn.close()
            
            logger.info(f"Saved {len(data)} records to SQLite database: {filepath}, table: {table_name}")
            return filepath
        except Exception as e:
            logger.error(f"Error saving to SQLite: {str(e)}")
            raise
            
    def load_from_json(self, filename: str) -> List[Dict[str, Any]]:
        """
        Load data from a JSON file.
        
        Args:
            filename: The filename to load from (without extension)
            
        Returns:
            The loaded data
        """
        if not filename.endswith('.json'):
            filename = f"{filename}.json"
            
        filepath = os.path.join(self.output_dir, filename)
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            logger.info(f"Loaded {len(data) if isinstance(data, list) else 1} records from JSON file: {filepath}")
            return data if isinstance(data, list) else [data]
        except Exception as e:
            logger.error(f"Error loading from JSON: {str(e)}")
            return []
            
    def load_from_csv(self, filename: str, delimiter: str = ',') -> List[Dict[str, Any]]:
        """
        Load data from a CSV file.
        
        Args:
            filename: The filename to load from (without extension)
            delimiter: The CSV delimiter character (default: ',')
            
        Returns:
            The loaded data
        """
        if not filename.endswith('.csv'):
            filename = f"{filename}.csv"
            
        filepath = os.path.join(self.output_dir, filename)
        
        try:
            df = pd.read_csv(filepath, delimiter=delimiter)
            data = df.to_dict('records')
            
            logger.info(f"Loaded {len(data)} records from CSV file: {filepath}")
            return data
        except Exception as e:
            logger.error(f"Error loading from CSV: {str(e)}")
            return []
            
    def merge_datasets(self, datasets: List[List[Dict[str, Any]]],
                      merge_on: Optional[str] = None,
                      remove_duplicates: bool = True) -> List[Dict[str, Any]]:
        """
        Merge multiple datasets.
        
        Args:
            datasets: List of datasets to merge
            merge_on: Optional field to use for merging (default: None)
            remove_duplicates: Whether to remove duplicate entries (default: True)
            
        Returns:
            The merged dataset
        """
        if not datasets:
            return []
            
        # Simple concatenation if no merge field
        if merge_on is None:
            merged = []
            for dataset in datasets:
                merged.extend(dataset)
                
            # Remove duplicates if requested
            if remove_duplicates:
                df = pd.DataFrame(merged)
                if not df.empty:
                    df = df.drop_duplicates()
                    merged = df.to_dict('records')
                    
            return merged
            
        # Merge on a specific field
        else:
            # Convert all datasets to DataFrames
            dfs = []
            for i, dataset in enumerate(datasets):
                if dataset:
                    df = pd.DataFrame(dataset)
                    if merge_on not in df.columns:
                        logger.warning(f"Merge field '{merge_on}' not found in dataset {i}")
                    else:
                        dfs.append(df)
                        
            if not dfs:
                return []
                
            # Start with the first DataFrame
            result_df = dfs[0]
            
            # Merge with other DataFrames
            for i, df in enumerate(dfs[1:], 1):
                result_df = pd.merge(result_df, df, on=merge_on, how='outer')
                
            # Convert back to list of dictionaries
            merged = result_df.to_dict('records')
            
            return merged 