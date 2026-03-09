# config.py
import os
from datetime import datetime
from pathlib import Path
from typing import Any


def get_base_paths(root_dir: str | None = None) -> dict[str, str]:
    """Returns dictionary of base paths for project"""
    base = root_dir or os.path.dirname(os.path.dirname(os.getcwd()))
    return {
        "root": base,
        "data": os.path.join(base, "_data"),
        "logs": os.path.join(base, "logs"),
    }


def get_batch_structure() -> dict[str, str]:
    """Returns dictionary of batch folder structure"""
    return {"images": "1_img", "sg_excel": "2_sg_excel", "output": "3_output"}


def get_batch_paths(batch_id: str) -> dict[str, str]:
    """Returns paths for specific batch processing"""
    base_paths = get_base_paths()
    structure = get_batch_structure()
    batch_base = os.path.join(base_paths["data"], batch_id)
    return {
        "img": os.path.join(batch_base, structure["images"]),
        "sg_excel": os.path.join(batch_base, structure["sg_excel"]),
        "output": os.path.join(batch_base, structure["output"]),
    }


def ensure_batch_paths(batch_id: str) -> dict[str, str]:
    """Creates and returns batch directories"""
    paths = get_batch_paths(batch_id)
    for path in paths.values():
        Path(path).mkdir(parents=True, exist_ok=True)
    return paths


def get_file_patterns() -> dict[str, Any]:
    """Returns dictionary of allowed file patterns"""
    return {
        "images": (".jpeg", ".jpg"),
        "data": (".csv", ".xlsx"),
        "logs": ".log",
        "sg_excel": "[Ff]echa*[Pp]arto*.xlsx",
    }


def get_column_settings() -> dict[str, Any]:
    """Returns dictionary of column configurations"""
    return {
        "date_range": (3, 10),
        "required_columns": ["Número animal", "Fecha Parto"],
        "drop_columns": ["Nombre", "Becerro", "Fecha PP", "#"],
        "rename_map": {
            "Kg/Leche": "Kg/Leche",
            "Número": "Número animal",
            "F.Últ.Par": "Fecha Parto",
        },
        "sg_columns": ["Número animal", "Fecha Parto"],
    }


def get_data_settings() -> dict[str, Any]:
    """Returns dictionary of data processing settings"""
    return {
        "year": datetime.now().year,
        "null_marker": "X*",
        "date_formats": {
            "input": "%Y-%m-%d",
            "output": "%-d/%m/%Y",
        },
        "excel_settings": {"header_row": 1},
        "separators": {"old": "-", "new": "/"},
        "char_replacements": {
            "clean_data": {"-*": "", "-": ""},
            "id_format": {"-": "/"},
        },
        "flag_marker": r"\*",
    }


def get_df_settings() -> dict[str, Any]:
    """Returns DataFrame operation settings"""
    return {
        "merge_settings": {"on": "Número animal", "how": "left"},
        "copy_on_ops": True,
        "ignore_errors": True,
    }


def get_logging_config() -> dict[str, Any]:
    """Returns logging configuration settings"""
    return {
        "level": "INFO",
        "format": "%(asctime)s - %(levelname)s - %(message)s",
        "file": os.path.join(get_base_paths()["logs"], "process.log"),
    }
