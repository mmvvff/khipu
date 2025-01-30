import os

import pandas as pd

from src.custom_logging import log_exception

# Custom exceptions
class ImageProcessingError(Exception):
    pass

class DataParsingError(Exception):
    pass

class ValidationError(Exception):
    pass

class DataFrameError(Exception):
    pass

def validate_file_path(path: str, required_extensions: tuple) -> bool:
    """Validate if file exists and has correct extension."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Path does not exist: {path}")
    if not path.lower().endswith(required_extensions):
        raise ValueError(f"Invalid file extension. Expected {required_extensions}")
    return True

def validate_image_file(image_path: str) -> bool:
    """Validate image file before processing."""
    try:
        validate_file_path(image_path, ('.jpeg', '.jpg'))
        file_size = os.path.getsize(image_path)
        if file_size == 0:
            raise ValidationError(f"Empty image file: {image_path}")
        return True
    except Exception as e:
        raise ImageProcessingError(f"Image validation failed: {str(e)}")

def validate_parsed_data(parsed_data: list) -> bool:
    """Validate parsed data structure and content."""
    if not parsed_data or not isinstance(parsed_data, list):
        raise DataParsingError("Invalid parsed data structure")
    if not all(isinstance(row, list) for row in parsed_data):
        raise DataParsingError("Invalid row structure in parsed data")
    return True

def validate_columns(columns: list, required_column: str = "vaca") -> bool:
    """Validate column headers."""
    if not columns or not isinstance(columns, list):
        raise ValidationError("Invalid column list")
    if not any(required_column in str(col).lower() for col in columns):
        raise ValidationError(f"Required column '{required_column}' not found")
    return True

def validate_dataframe_structure(df: pd.DataFrame, required_columns: list) -> bool:
    """Validate DataFrame structure and required columns."""
    if df.empty:
        raise DataFrameError("Empty DataFrame")
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise DataFrameError(f"Missing required columns: {missing_cols}")
    return True

def safe_merge_dataframes(left_df: pd.DataFrame, right_df: pd.DataFrame, 
                         on_column: str) -> pd.DataFrame:
    """Safely merge two DataFrames with validation."""
    try:
        validate_dataframe_structure(left_df, [on_column])
        validate_dataframe_structure(right_df, [on_column])
        
        result = left_df.merge(right_df, on=on_column, how="left")
        
        if result.empty:
            raise DataFrameError("Merge resulted in empty DataFrame")
        return result
    except Exception as e:
        raise DataFrameError(f"DataFrame merge failed: {str(e)}")

def validate_date_format(date_str: str, expected_format: str) -> bool:
    """Validate date string format."""
    try:
        pd.to_datetime(date_str, format=expected_format)
        return True
    except ValueError as e:
        raise ValidationError(f"Invalid date format: {str(e)}")

def handle_processing_errors(func):
    """Decorator for handling processing errors."""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ImageProcessingError as e:
            log_exception(e)  # Use log_exception for logging
            raise
        except DataParsingError as e:
            log_exception(e)  # Use log_exception for logging
            raise
        except ValidationError as e:
            log_exception(e)  # Use log_exception for logging
            raise
        except DataFrameError as e:
            log_exception(e)  # Use log_exception for logging
            raise
        except Exception as e:
            log_exception(e)  # Use log_exception for logging
            raise
    return wrapper