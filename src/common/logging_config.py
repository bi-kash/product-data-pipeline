"""
Centralized logging configuration for the application.
"""
import os
import logging
from logging.handlers import RotatingFileHandler

# Create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)

def setup_logging(module_name):
    """
    Set up logging for a module with file and console handlers.
    
    Args:
        module_name: Name of the module for which logging is being set up
        
    Returns:
        Logger instance configured with file and console handlers
    """
    # Create logger
    logger = logging.getLogger(module_name)
    logger.setLevel(logging.INFO)
    
    # Create formatters
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Avoid adding handlers multiple times
    if not logger.handlers:
        # Create file handler with rotation (10MB max size, keep 5 backup logs)
        file_name = module_name.split('.')[-1] if '.' in module_name else module_name
        log_file = f"logs/{file_name}.log"
        file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(file_formatter)
        
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(console_formatter)
        
        # Add handlers to logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
    return logger
