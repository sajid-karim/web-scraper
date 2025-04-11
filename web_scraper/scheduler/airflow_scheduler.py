import logging
from typing import Dict, List, Any, Optional, Callable, Union
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)


class AirflowScheduler:
    """
    Handles scheduling and orchestrating scraping tasks using Apache Airflow.
    """
    def __init__(self, airflow_home: Optional[str] = None):
        """
        Initialize the AirflowScheduler.
        
        Args:
            airflow_home: Optional path to Airflow home directory (default: None)
        """
        self.airflow_home = airflow_home or os.environ.get('AIRFLOW_HOME')
        
        if not self.airflow_home:
            logger.warning("AIRFLOW_HOME not set, will use default location")
            
    def create_dag_template(self, 
                           dag_id: str,
                           schedule: str,
                           description: str,
                           scraper_script: str,
                           default_args: Optional[Dict[str, Any]] = None,
                           catchup: bool = False) -> str:
        """
        Create a DAG file template for Airflow.
        
        Args:
            dag_id: The ID for the DAG
            schedule: Airflow schedule interval (cron expression or preset)
            description: DAG description
            scraper_script: Path to the Python scraper script to run
            default_args: Optional default arguments for the DAG (default: None)
            catchup: Whether to catchup on missed runs (default: False)
            
        Returns:
            The generated DAG file content
        """
        # Default arguments if not provided
        if default_args is None:
            default_args = {
                'owner': 'airflow',
                'depends_on_past': False,
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 1,
                'retry_delay': 'timedelta(minutes=5)',
                'start_date': f"datetime({datetime.now().year}, {datetime.now().month}, {datetime.now().day})"
            }
            
        # Convert Python objects to strings for template
        for key, value in default_args.items():
            if isinstance(value, (datetime, timedelta)):
                default_args[key] = repr(value)
                
        # Create the DAG file content
        dag_content = f'''
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {{
    {", ".join(f"'{k}': {v}" for k, v in default_args.items())}
}}

dag = DAG(
    '{dag_id}',
    default_args=default_args,
    description='{description}',
    schedule_interval='{schedule}',
    catchup={str(catchup).lower()},
    tags=['scraper', 'data-collection'],
)

run_scraper = BashOperator(
    task_id='run_scraper',
    bash_command='python {scraper_script}',
    dag=dag,
)

# Define the task dependencies (if any)
# For example:
# task1 >> task2 >> task3
'''
        return dag_content.strip()
        
    def save_dag_file(self, dag_content: str, dag_id: str) -> str:
        """
        Save a DAG file to the Airflow dags directory.
        
        Args:
            dag_content: The DAG file content
            dag_id: The ID for the DAG (used for the filename)
            
        Returns:
            The path to the saved DAG file
        """
        if not self.airflow_home:
            raise ValueError("AIRFLOW_HOME not set, cannot save DAG file")
            
        dags_dir = os.path.join(self.airflow_home, 'dags')
        os.makedirs(dags_dir, exist_ok=True)
        
        dag_filename = f"{dag_id}.py"
        dag_path = os.path.join(dags_dir, dag_filename)
        
        with open(dag_path, 'w') as f:
            f.write(dag_content)
            
        logger.info(f"Saved DAG file to {dag_path}")
        return dag_path
        
    def create_scraper_dag(self, 
                          dag_id: str,
                          schedule: str,
                          description: str,
                          scraper_script: str,
                          save: bool = True) -> str:
        """
        Create a DAG for a scraper task.
        
        Args:
            dag_id: The ID for the DAG
            schedule: Airflow schedule interval (cron expression or preset)
            description: DAG description
            scraper_script: Path to the Python scraper script to run
            save: Whether to save the DAG file (default: True)
            
        Returns:
            The DAG file content or path if saved
        """
        # Create the DAG content
        dag_content = self.create_dag_template(
            dag_id=dag_id,
            schedule=schedule,
            description=description,
            scraper_script=scraper_script
        )
        
        # Save the DAG file if requested
        if save:
            return self.save_dag_file(dag_content, dag_id)
        else:
            return dag_content
            
    def create_multi_scraper_dag(self, 
                                dag_id: str,
                                schedule: str,
                                description: str,
                                scraper_tasks: List[Dict[str, Any]],
                                dependencies: Optional[List[List[str]]] = None,
                                save: bool = True) -> str:
        """
        Create a DAG with multiple scraper tasks.
        
        Args:
            dag_id: The ID for the DAG
            schedule: Airflow schedule interval (cron expression or preset)
            description: DAG description
            scraper_tasks: List of task dictionaries with 'task_id' and 'script_path' keys
            dependencies: Optional list of task dependency lists (default: None)
            save: Whether to save the DAG file (default: True)
            
        Returns:
            The DAG file content or path if saved
        """
        # Default arguments for the DAG
        default_args = {
            'owner': 'airflow',
            'depends_on_past': False,
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': 'timedelta(minutes=5)',
            'start_date': f"datetime({datetime.now().year}, {datetime.now().month}, {datetime.now().day})"
        }
        
        # Create the DAG file header
        dag_content = f'''
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

default_args = {{
    {", ".join(f"'{k}': {v}" for k, v in default_args.items())}
}}

dag = DAG(
    '{dag_id}',
    default_args=default_args,
    description='{description}',
    schedule_interval='{schedule}',
    catchup=False,
    tags=['scraper', 'data-collection'],
)

# Create a start task
start = DummyOperator(task_id='start', dag=dag)

# Create an end task
end = DummyOperator(task_id='end', dag=dag)
'''
        
        # Add the tasks
        for task in scraper_tasks:
            task_id = task['task_id']
            script_path = task['script_path']
            
            dag_content += f'''
# Task for {task_id}
{task_id} = BashOperator(
    task_id='{task_id}',
    bash_command='python {script_path}',
    dag=dag,
)
'''
        
        # Add the dependencies
        dag_content += "\n# Define the task dependencies\n"
        
        # Start depends on nothing (implicit)
        # Connect start to all tasks that have no upstream dependencies
        if dependencies:
            # Find all tasks that are mentioned as downstream but not as upstream
            downstream_tasks = set()
            upstream_tasks = set()
            
            for dep in dependencies:
                if len(dep) >= 2:
                    upstream, downstream = dep[0], dep[1]
                    upstream_tasks.add(upstream)
                    downstream_tasks.add(downstream)
                    
            # Tasks with no upstream dependencies
            no_upstream = set(task['task_id'] for task in scraper_tasks) - upstream_tasks
            
            # Connect start to tasks with no upstream dependencies
            for task_id in no_upstream:
                dag_content += f"start >> {task_id}\n"
                
            # Add the explicit dependencies
            for dep in dependencies:
                if len(dep) >= 2:
                    upstream, downstream = dep[0], dep[1]
                    dag_content += f"{upstream} >> {downstream}\n"
                    
            # Connect all tasks to end that have no downstream dependencies
            no_downstream = set(task['task_id'] for task in scraper_tasks) - downstream_tasks
            for task_id in no_downstream:
                dag_content += f"{task_id} >> end\n"
        else:
            # If no dependencies specified, create a simple linear flow
            dag_content += "start"
            for task in scraper_tasks:
                dag_content += f" >> {task['task_id']}"
            dag_content += " >> end\n"
            
        # Save the DAG file if requested
        if save:
            return self.save_dag_file(dag_content, dag_id)
        else:
            return dag_content 