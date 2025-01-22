# file handling
import os

# image handling
import base64

# parsing
import csv
from io import StringIO

# date handling
import locale
import datetime as dt

# AI API
import anthropic

# setup constructor
api_key = os.getenv("CLAUDE_API_KEY")
if not api_key:
    raise ValueError("CLAUDE_API_KEY not found")
client = anthropic.Anthropic(api_key=api_key)

# use AI API to extract text from image
def extract_img2text(image_path, prompt):

    with open(image_path, "rb") as image_file:
        image_data = base64.b64encode(image_file.read()).decode('utf-8')

    message = client.messages.create(
        model="claude-3-5-sonnet-20240620",
        max_tokens=3200,
        messages=[{
            "role":
            "user",
            "content": [{
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": "image/jpeg",
                    "data": image_data,
                },
            }, {
                "type": "text",
                "text": prompt
            }],
        }],
    )
    return message

# parse csv string to list of lists
def parse_csv_string(csv_string):
    # Remove the leading newline and split the string into lines
    lines = csv_string.strip().split('\n')

    # Parse the CSV data
    reader = csv.reader(StringIO('\n'.join(lines)))

    # Convert to list of lists
    data = list(reader)
    # return output
    return data

def insert_column_name(df, column_name):
    # Get the index of the specified column
    col_index = df.columns.get_loc(column_name)

    # Insert a new column with the column name as the constant value
    df.insert(col_index, f'{column_name}_name', column_name)

    return df


def convert_to_date(date_string, year):
    # Set locale to Spanish
    locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')

    # Parse the date string
    date_obj = dt.datetime.strptime(f"{date_string} {year}", "%B %A %d %Y")

    # Reset locale
    locale.setlocale(locale.LC_TIME, '')

    # format: 2/03/2024
    return date_obj.strftime('%-d/%m/%Y')


def normalize_day(input_string):
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

    # Convert to title case and split
    parts = input_string.title().split()

    # Check if the second part is a day abbreviation
    if len(parts) > 1:
        day_abbr = parts[1][:3]  # Take first 3 characters
        if day_abbr in day_mapping:
            parts[1] = day_mapping[day_abbr]

    return " ".join(parts)


def normalize_month(input_string):

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

    # Convert to title case and split
    parts = input_string.title().split()

    # Check if the second part is a day abbreviation
    if len(parts) > 1:
        month_abbr = parts[0][:3]  # Take first 3 characters
        if month_abbr in month_mapping:
            parts[0] = month_mapping[month_abbr]

    return " ".join(parts)
