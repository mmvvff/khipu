import logging
import os
from datetime import datetime

def setup_logger(log_dir: str = "logs", log_level: int = logging.DEBUG) -> logging.Logger:
    """
    Configure and return a logger with both file and console handlers.
    Args:
        log_dir: Directory for log files
        log_level: Logging level for the logger
    Returns:
        Configured logger instance
    """
    # Create logs directory if it doesn't exist
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Create logger
    logger = logging.getLogger('data_processor')
    logger.setLevel(log_level)
    
    # Prevent adding handlers multiple times
    if not logger.handlers:
        # File handler - daily rotation
        log_file = os.path.join(
            log_dir,
            f"data_processing_{datetime.now().strftime('%Y%m%d')}.log"
        )
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Add formatter to handlers
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Add handlers to logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
    return logger

def log_file_processing(logger: logging.Logger, filename: str) -> None:
    """Log the start of processing for a file."""
    logger.info(f"Processing file: {filename}")

def log_column_status(logger: logging.Logger, status: str, cols: list) -> None:
    """Log column list status and updates."""
    logger.debug(f"cols_list {status}: {cols}")

def log_dataframe_creation(logger: logging.Logger, success: bool, error: str = None) -> None:
    """Log DataFrame creation status."""
    if success:
        logger.info("Dataframe successfully created")
    else:
        logger.error(f"DataFrame creation failed: {error}")

def log_column_list(logger: logging.Logger, columns: list, prefix: str = "") -> None:
    """Log column list with optional prefix."""
    logger.debug(f"{prefix} columns: {columns}")

def log_data_error(logger: logging.Logger, error: Exception, context: str = "") -> None:
    """Log errors with context and traceback."""
    logger.error(f"{context} Error occurred: {str(error)}", exc_info=True)

def log_process_separator(logger: logging.Logger) -> None:
    """Log a separator line between processing steps."""
    logger.debug("---")

# Example usage function
def get_logger(log_dir: str = "logs") -> logging.Logger:
    """
    Get or create a configured logger instance.
    Args:
        log_dir: Directory for log files
    Returns:
        Configured logger instance
    """
    return setup_logger(log_dir)