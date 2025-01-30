# file handling
import glob
import os
from dotenv import load_dotenv
import sys

# tabular data
import numpy as np
import pandas as pd

# User defined functions
# Set path for scripts folder
path_scripts = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.append(path_scripts)
from src import custom_logging as clogs, config, processing, postprocessing

# Load environment variables from .env file
load_dotenv()

logger = clogs.get_logger()

# Replace original path setup with:
batch_id = "01_2025_03"
batch_paths = config.ensure_batch_paths(batch_id)

# Use the paths in your code
folder_img_path = batch_paths['img']
folder_sg_path = batch_paths['sg_excel']
folder_output = batch_paths['output']

# Get configuration
paths = config.get_base_paths()
patterns = config.get_file_patterns()
columns = config.get_column_settings()
settings = config.get_data_settings()

# Find the file matching the pattern
file_path = glob.glob(os.path.join(folder_sg_path, patterns['sg_file']))[0]

# Read the matched file
data_sg = pd.read_excel(
    file_path,
    header=settings['excel_settings']['header_row']
)

# Rename columns using config
data_sg = data_sg.rename(
    columns=columns['rename_map']
).copy()

# Select and drop NA using config
data_sg = data_sg[columns['sg_columns']].dropna().copy()

# Format date using config
data_sg["Fecha Parto"] = data_sg["Fecha Parto"].dt.strftime(
    settings['date_formats']['output'])

conf_level = 99.75
prompt_input = f"""Instruction 1: Convert the text in the image to csv.
Instruction 2: Employ a strict approach: add 1 asterisk next to the estimated values for those cells whose text-to-digit conversion are below a {conf_level} percent confidence threshold; it does not matter if data is over-flagged.
Instruction 3: Include in comments the confidence threshold used.
Instruction 4: Do not use outlier-detection as criteria to flag the data.
Instruction 5: Make sure to not use outlier-detection as criteria to flag the data.
Instruction 6: If headers are present, include them. If no headers are found, do not include any.
Instruction 7: Include any comments before returning output. Limit verbosity.
Instruction 8: Return output enclosed in brackets to facilitate parsing.
Instruction 9: Do not include any additional comments after final output.
"""

# main loop
data_list = []
cols_list = []
year = settings['year']

class NoColsError(Exception):
    pass

for filename in sorted(os.listdir(folder_img_path)):
    if filename.endswith(('.jpeg', '.jpg')):
        clogs.log_file_processing(logger, filename)
        clogs.log_column_status(logger, "initialized", cols_list)

        image_path = os.path.join(folder_img_path, filename)

        try:
            result = processing.extract_img2text(image_path, prompt_input)
            clogs.log_api_comment(logger, result.content[0].text)
        except Exception as e:
            print(f"An error occurred: {e}")
            continue  # Skip to next file if there's an error

        data_string = result.content[0].text.split("[")[1].replace("]", "")
        parsed_data = postprocessing.parse_csv_string(data_string)

        if not cols_list:
            if any("vaca" in s.lower() for s in parsed_data[0]):
                cols_list = parsed_data[0]
                clogs.log_column_status(logger, "initialized", cols_list)
            else:
                clogs.log_validation_error(logger, parsed_data[0], "No 'vaca' found")
                raise NoColsError("No columns found: Check image folder")
        else:
            if any("vaca" in s.lower()
                   for s in parsed_data[0]) and (cols_list != parsed_data[0]):
                cols_list = parsed_data[0]
                clogs.log_column_status(logger, "updated", cols_list)
            else:
                clogs.log_column_status(logger, "current (no update)", cols_list)

        # Create the DataFrame
        try:
            data_df = pd.DataFrame(parsed_data[1:], columns=cols_list)
            clogs.log_dataframe_creation(logger, success=True)
        except Exception as e:
            clogs.log_dataframe_creation(logger, success=False, error=str(e))
            clogs.log_column_list(logger, cols_list, "Current columns")
            
            for c in parsed_data[1:]:
                if len(c) > len(cols_list):
                    logger.error(f"Row longer than columns: {c}")
                    break
            
            logger.warning("Breaking processing loop due to DataFrame creation error")
            break

        clogs.log_column_status(logger, "at end of iteration", cols_list)

        data_df['flag_count'] = postprocessing.calculate_flag_counts(data_df)

        col_label_num = 1
        for col in data_df.iloc[:, 3:10].columns.tolist():

            data_df[col] = postprocessing.clean_column_values(data_df[col])

            # Get the index of the specified column
            col_index = data_df.columns.get_loc(col)

            col_label_str = postprocessing.normalize_month(
                postprocessing.normalize_day(col.replace(".", "")))

            # Insert a new column with the column name as the constant value
            data_df.insert(col_index, f'Fecha {col_label_num}',
                           postprocessing.convert_to_date(
                               col_label_str, year = year))
            col_label_num += 1
            data_df = data_df.rename(columns={col: "Kg/Leche"}).copy()

        data_df = data_df.drop(
            columns=["Nombre", "Becerro", "Fecha PP", "#"],
            errors="ignore").copy()

        data_df = data_df.rename(columns={
            data_df.columns[0]: "Número animal"
        }).copy()

        data_df["Número animal"] = data_df["Número animal"].str.replace(
            "-", "/").copy()
        
        clogs.log_dataframe_columns(logger, "data_df", data_df.columns.tolist())
        data_final = data_df.merge(data_sg, on="Número animal", how="left")
        clogs.log_dataframe_columns(logger, "data_final", data_final.columns.tolist())
        data_final["Fecha Parto"] = data_final["Fecha Parto"].fillna(
            "X*").copy()

        # Reorder columns
        cols_to_move = ["Número animal", "Fecha Parto"]
        data_final = processing.reorder_columns(data_final, cols_to_move)

        clogs.log_column_list(logger, data_final.columns.tolist())
        data_list.append(data_final)
        clogs.log_process_separator(logger)

regular_file, final_file = postprocessing.export_data(
    data_list=data_list,
    folder_output=folder_output,
    batch_id=batch_id
    )
