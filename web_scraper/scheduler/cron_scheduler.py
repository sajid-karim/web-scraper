import logging
import os
import sys
import subprocess
from typing import Optional, List, Dict, Any
import platform

logger = logging.getLogger(__name__)


class CronScheduler:
    """
    Handles scheduling scraping tasks using cron jobs.
    """
    def __init__(self):
        """
        Initialize the CronScheduler.
        """
        self.is_windows = platform.system() == "Windows"
        
    def validate_cron_expression(self, cron_expression: str) -> bool:
        """
        Validate a cron expression.
        
        Args:
            cron_expression: The cron expression to validate
            
        Returns:
            True if the expression is valid, False otherwise
        """
        parts = cron_expression.split()
        
        # Basic validation: check number of fields
        # minute hour day_of_month month day_of_week
        if len(parts) != 5:
            logger.error(f"Invalid cron expression: {cron_expression}. Expected 5 fields, got {len(parts)}.")
            return False
            
        return True
        
    def _create_crontab_entry(self, cron_expression: str, command: str) -> str:
        """
        Create a crontab entry.
        
        Args:
            cron_expression: The cron expression for scheduling
            command: The command to execute
            
        Returns:
            The formatted crontab entry
        """
        # Format the command with proper escaping and full path
        full_command = command
        
        # Add logging to the command
        log_part = ">> /tmp/scraper_cron.log 2>&1"
        if ">" not in full_command:
            full_command = f"{full_command} {log_part}"
            
        # Create the crontab entry
        entry = f"{cron_expression} {full_command}"
        return entry
        
    def add_cron_job(self, cron_expression: str, command: str, 
                     comment: Optional[str] = None) -> bool:
        """
        Add a cron job to the user's crontab.
        
        Args:
            cron_expression: The cron expression for scheduling
            command: The command to execute
            comment: Optional comment to add before the job (default: None)
            
        Returns:
            True if successful, False otherwise
        """
        if self.is_windows:
            logger.error("Cron jobs are not supported on Windows. Use Windows Task Scheduler instead.")
            return False
            
        if not self.validate_cron_expression(cron_expression):
            return False
            
        try:
            # Get the current crontab
            process = subprocess.run(['crontab', '-l'], 
                                    stdout=subprocess.PIPE, 
                                    stderr=subprocess.PIPE,
                                    text=True)
                                    
            if process.returncode != 0 and "no crontab" not in process.stderr:
                logger.error(f"Error getting crontab: {process.stderr}")
                return False
                
            current_crontab = process.stdout if process.returncode == 0 else ""
            
            # Create the new entry
            entry = self._create_crontab_entry(cron_expression, command)
            
            # Add comment if provided
            if comment:
                entry = f"# {comment}\n{entry}"
                
            # Append the new entry to the current crontab
            new_crontab = current_crontab.rstrip() + "\n" + entry + "\n"
            
            # Write the new crontab
            process = subprocess.run(['crontab', '-'], 
                                    input=new_crontab, 
                                    stdout=subprocess.PIPE, 
                                    stderr=subprocess.PIPE,
                                    text=True)
                                    
            if process.returncode != 0:
                logger.error(f"Error updating crontab: {process.stderr}")
                return False
                
            logger.info(f"Added cron job: {entry}")
            return True
            
        except Exception as e:
            logger.error(f"Error adding cron job: {str(e)}")
            return False
            
    def remove_cron_job(self, command_pattern: str) -> bool:
        """
        Remove a cron job matching the given command pattern.
        
        Args:
            command_pattern: Pattern to match in the command part of the cron job
            
        Returns:
            True if successful, False otherwise
        """
        if self.is_windows:
            logger.error("Cron jobs are not supported on Windows. Use Windows Task Scheduler instead.")
            return False
            
        try:
            # Get the current crontab
            process = subprocess.run(['crontab', '-l'], 
                                    stdout=subprocess.PIPE, 
                                    stderr=subprocess.PIPE,
                                    text=True)
                                    
            if process.returncode != 0 and "no crontab" not in process.stderr:
                logger.error(f"Error getting crontab: {process.stderr}")
                return False
                
            if process.returncode != 0:
                # No crontab exists
                return True
                
            current_crontab = process.stdout
            
            # Filter out the entries matching the pattern
            new_lines = []
            found = False
            
            for line in current_crontab.splitlines():
                if command_pattern in line:
                    found = True
                    logger.info(f"Removing cron job: {line}")
                else:
                    new_lines.append(line)
                    
            if not found:
                logger.warning(f"No cron jobs found matching pattern: {command_pattern}")
                return True
                
            # Write the new crontab
            new_crontab = "\n".join(new_lines) + "\n"
            
            process = subprocess.run(['crontab', '-'], 
                                    input=new_crontab, 
                                    stdout=subprocess.PIPE, 
                                    stderr=subprocess.PIPE,
                                    text=True)
                                    
            if process.returncode != 0:
                logger.error(f"Error updating crontab: {process.stderr}")
                return False
                
            logger.info(f"Removed cron job(s) matching: {command_pattern}")
            return True
            
        except Exception as e:
            logger.error(f"Error removing cron job: {str(e)}")
            return False
            
    def list_cron_jobs(self) -> List[str]:
        """
        List all cron jobs for the current user.
        
        Returns:
            A list of cron job entries
        """
        if self.is_windows:
            logger.error("Cron jobs are not supported on Windows. Use Windows Task Scheduler instead.")
            return []
            
        try:
            # Get the current crontab
            process = subprocess.run(['crontab', '-l'], 
                                    stdout=subprocess.PIPE, 
                                    stderr=subprocess.PIPE,
                                    text=True)
                                    
            if process.returncode != 0 and "no crontab" not in process.stderr:
                logger.error(f"Error getting crontab: {process.stderr}")
                return []
                
            if process.returncode != 0:
                # No crontab exists
                return []
                
            # Filter out empty lines and comments
            cron_jobs = []
            for line in process.stdout.splitlines():
                line = line.strip()
                if line and not line.startswith('#'):
                    cron_jobs.append(line)
                    
            return cron_jobs
            
        except Exception as e:
            logger.error(f"Error listing cron jobs: {str(e)}")
            return []
            
    def create_scraper_job(self, cron_expression: str, script_path: str, 
                          python_executable: Optional[str] = None,
                          job_name: Optional[str] = None) -> bool:
        """
        Create a cron job for a scraper script.
        
        Args:
            cron_expression: The cron expression for scheduling
            script_path: Path to the Python scraper script
            python_executable: Optional path to the Python executable (default: sys.executable)
            job_name: Optional name for the job (default: None)
            
        Returns:
            True if successful, False otherwise
        """
        if self.is_windows:
            logger.warning("Cron jobs are not supported on Windows. Consider using Windows Task Scheduler.")
            return False
            
        # Use the current Python executable if not specified
        python_executable = python_executable or sys.executable
        
        # Construct the command
        command = f"{python_executable} {script_path}"
        
        # Add comment for the job
        comment = job_name or f"Scraper job for {os.path.basename(script_path)}"
        
        # Add the cron job
        return self.add_cron_job(cron_expression, command, comment)
        
    def remove_scraper_job(self, script_path: str) -> bool:
        """
        Remove a cron job for a scraper script.
        
        Args:
            script_path: Path to the Python scraper script
            
        Returns:
            True if successful, False otherwise
        """
        if self.is_windows:
            logger.warning("Cron jobs are not supported on Windows. Consider using Windows Task Scheduler.")
            return False
            
        # Remove the cron job
        return self.remove_cron_job(script_path) 