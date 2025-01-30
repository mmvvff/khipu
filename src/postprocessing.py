# parsing
import os
import csv
from io import StringIO

# date handling
import locale
import datetime as dt

# data processing
import pandas as pd

def parse_csv_string(csv_string):
    """Converts a CSV string into a list of lists."""
    # Clean input string and split into lines
    lines = csv_string.strip().split('\n')
    
    # Parse CSV using StringIO to simulate file input
    reader = csv.reader(StringIO('\n'.join(lines)))
    
    return list(reader)

def insert_column_name(df, column_name):
    """Inserts a new column containing the column name as a constant value."""
    # Get position of target column
    col_index = df.columns.get_loc(column_name)
    
    # Insert new column with name as value
    df.insert(col_index, f'{column_name}_name', column_name)
    
    return df

def convert_to_date(date_string, year):
    """Converts Spanish date string to formatted date string (d/mm/yyyy)."""
    # Set Spanish locale for date parsing
    locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')
    
    # Parse date string with year
    date_obj = dt.datetime.strptime(f"{date_string} {year}", "%B %A %d %Y")
    
    # Reset locale to system default
    locale.setlocale(locale.LC_TIME, '')
    
    return date_obj.strftime('%-d/%m/%Y')

def normalize_day(input_string):
    """Normalizes Spanish day abbreviations to full day names."""
    # Mapping of abbreviated to full day names
    day_mapping = {
        "Lun":"Lunes",
        "Mar": "Martes",
        "Mié": "Miércoles",
        "Mie": "Miércoles",
        "Jue": "Jueves",
        "Vie": "Viernes",
        "Sab": "Sábado",
        "Sáb": "Sábado",
        "Dom": "Domingo"
    }
    
    # Split and normalize case
    parts = input_string.title().split()
    
    # Replace abbreviation if found
    if len(parts) > 1:
        day_abbr = parts[1][:3]
        if day_abbr in day_mapping:
            parts[1] = day_mapping[day_abbr]
    
    return " ".join(parts)

def normalize_month(input_string):
    """Normalizes Spanish month abbreviations to full month names."""
    # Mapping of abbreviated to full month names
    month_mapping = {
        "Ene": "Enero",
        "Feb": "Febrero",
        "Mar": "Marzo",
        "Apr": "Abril",
        "May": "Mayo",
        "Jun": "Junio",
        "Jul": "Julio",
        "Aug": "Agosto",
        "Sep": "Septiembre",
        "Oct": "Octubre",
        "Nov": "Noviembre",
        "Dic": "Diciembre"
    }
    
    # Split and normalize case
    parts = input_string.title().split()
    
    # Replace abbreviation if found
    if len(parts) > 1:
        month_abbr = parts[0][:3]
        if month_abbr in month_mapping:
            parts[0] = month_mapping[month_abbr]
    
    return " ".join(parts)

def clean_column_values(series: pd.Series) -> pd.Series:
    """Remove special characters from column values."""
    return series.astype(str).str.replace("-*", "").str.replace("-", "")

def calculate_flag_counts(df: pd.DataFrame) -> pd.Series:
    """
    Calculate flag counts for entire DataFrame at once."""
    string_df = df.astype(str)
    return string_df.apply(lambda x: x.str.count('\*')).sum(axis=1)

def create_filename(base_name: str, batch_id: str, suffix: str = "") -> str:
    """
    Create standardized filename for Excel output."""
    suffix = f"_{suffix}" if suffix else ""
    return f"{base_name}_{batch_id}{suffix}.xlsx"

def save_dataframes_to_excel(
    dataframes: list,
    output_path: str,
    filename: str,
    sheet_prefix: str = "Sheet"
    ) -> str:
    """
    Save list of dataframes to Excel file with named sheets."""
    full_path = os.path.join(output_path, filename)
    
    with pd.ExcelWriter(full_path) as writer:
        for idx, df in enumerate(dataframes, start=1):
            df.to_excel(writer, 
                       sheet_name=f'{sheet_prefix}_{idx}', 
                       index=False)
    
    return full_path

def export_data(
    data_list: list,
    folder_output: str,
    batch_id: str,
    base_name: str = "leche"
    ) -> tuple[str, str]:
    """
    Export milk data to two Excel files with different names."""
    # Create regular output file
    regular_filename = create_filename(base_name, batch_id)
    regular_path = save_dataframes_to_excel(
        dataframes=data_list,
        output_path=folder_output,
        filename=regular_filename
    )
    
    # Create final output file
    final_filename = create_filename(base_name, batch_id, "final")
    final_path = save_dataframes_to_excel(
        dataframes=data_list,
        output_path=folder_output,
        filename=final_filename
    )
    
    return regular_path, final_path
