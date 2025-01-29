# parsing
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