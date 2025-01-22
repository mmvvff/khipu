# config.py
import os
from pathlib import Path

def get_base_paths(root_dir: str = None) -> dict:
    """Returns dictionary of base paths for project"""
    base = root_dir or os.getcwd()
    return {
        'root': base,
        'data': os.path.join(base, 'data'),
        'logs': os.path.join(base, 'logs'),
        'output': os.path.join(base, 'output')
    }

def get_file_patterns() -> dict:
    """Returns dictionary of allowed file patterns"""
    return {
        'images': ('.jpeg', '.jpg'),
        'data': ('.csv', '.xlsx'),
        'logs': '.log'
    }

def get_column_settings() -> dict:
    """Returns dictionary of column configurations"""
    return {
        'date_range': (3, 10),
        'required_columns': ['Número animal', 'Fecha Parto'],
        'drop_columns': ['Nombre', 'Becerro', 'Fecha PP', '#'],
        'rename_map': {'Kg/Leche': 'Kg/Leche'}
    }

def get_data_settings() -> dict:
    """Returns dictionary of data processing settings"""
    return {
        'default_year': 2024,
        'null_marker': 'X*',
        'date_format': '%Y-%m-%d',
        'separators': {
            'old': '-',
            'new': '/'
        }
    }

def ensure_paths_exist(paths: dict) -> None:
    """Creates directories if they don't exist"""
    for path in paths.values():
        Path(path).mkdir(parents=True, exist_ok=True)

def get_logging_config() -> dict:
    """Returns logging configuration settings"""
    return {
        'level': 'INFO',
        'format': '%(asctime)s - %(levelname)s - %(message)s',
        'file': os.path.join(get_base_paths()['logs'], 'process.log')
    }

def validate_config() -> tuple[bool, str]:
    """
    Validates configuration settings
    Returns: Tuple of (success: bool, error_message: str)
    """
    try:
        paths = get_base_paths()
        ensure_paths_exist(paths)
        required_configs = [
            get_file_patterns(),
            get_column_settings(),
            get_data_settings(),
            get_logging_config()
        ]
        return all(bool(config) for config in required_configs), ""
    except Exception as error:
        return False, str(error)
