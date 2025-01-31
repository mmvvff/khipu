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
from src import (
    custom_logging as clogs,
    config,
    processing,
    postprocessing,
    validation
)

# Load environment variables from .env file
load_dotenv()

logger = clogs.get_logger()

@validation.handle_processing_errors
def setup_batch_environment(batch_id: str) -> dict:
    """Set up and validate batch processing environment."""
    batch_paths = config.ensure_batch_paths(batch_id)
    for path in batch_paths.values():
        if not os.path.exists(path):
            raise FileNotFoundError(f"Required directory not found: {path}")
    return batch_paths

@validation.handle_processing_errors
def load_sg_data(folder_path: str, file_pattern: str, settings: dict, columns: dict) -> pd.DataFrame:
    """Load and validate SG Excel data."""
    # Find the file matching the pattern
    file_path = glob.glob(os.path.join(folder_path, file_pattern))
    validation.validate_file_path(file_path, ('.xlsx',))

    # Read and process the matched file
    data_sg = pd.read_excel(
        file_path,
        header=settings['excel_settings']['header_row']
    )
    
    # Validate DataFrame structure
    validation.validate_dataframe_structure(data_sg, columns['required_columns'])
    
    # Process DataFrame
    data_sg = data_sg.rename(columns=columns['rename_map']).copy()
    data_sg = data_sg[columns['sg_columns']].dropna().copy()
    data_sg["Fecha Parto"] = data_sg["Fecha Parto"].dt.strftime(
        settings['date_formats']['output'])
    
    return data_sg

@validation.handle_processing_errors
def process_image_data(image_path: str, prompt: str) -> tuple[list, str]:
    """Process image and validate extracted data."""
    # Validate image file
    validation.validate_image_file(image_path)
    
    # Extract text from image
    result = processing.extract_img2text(image_path, prompt)
    data_string = result.content[0].text.split("[")[1].replace("]", "")
    parsed_data = postprocessing.parse_csv_string(data_string)
    
    # Validate parsed data
    validation.validate_parsed_data(parsed_data)
    
    return parsed_data, result.content[0].text

@validation.handle_processing_errors
def validate_and_update_columns(parsed_data: list, existing_cols: list) -> list:
    """Validate and update column headers."""
    if not existing_cols:
        validation.validate_columns(parsed_data[0])
        return parsed_data[0]
    elif any("vaca" in s.lower() for s in parsed_data[0]) and (existing_cols != parsed_data[0]):
        validation.validate_columns(parsed_data[0])
        return parsed_data[0]
    return existing_cols

@validation.handle_processing_errors
def create_and_process_dataframe(parsed_data: list, cols_list: list, year: int) -> pd.DataFrame:
    """Create and process DataFrame with validation."""
    data_df = pd.DataFrame(parsed_data[1:], columns=cols_list)
    validation.validate_dataframe_structure(data_df, ["Número animal"])
    
    # Process DataFrame
    data_df['flag_count'] = postprocessing.calculate_flag_counts(data_df)
    
    col_label_num = 1
    for col in data_df.iloc[:, 3:10].columns.tolist():
        data_df[col] = postprocessing.clean_column_values(data_df[col])
        col_index = data_df.columns.get_loc(col)
        col_label_str = postprocessing.normalize_month(
            postprocessing.normalize_day(col.replace(".", "")))
        
        # Validate date format before conversion
        date_str = postprocessing.convert_to_date(col_label_str, year)
        validation.validate_date_format(date_str, "%d/%m/%Y")
        
        data_df.insert(col_index, f'Fecha {col_label_num}', date_str)
        col_label_num += 1
        data_df = data_df.rename(columns={col: "Kg/Leche"}).copy()
    
    # Clean up DataFrame
    data_df = data_df.drop(
        columns=["Nombre", "Becerro", "Fecha PP", "#"],
        errors="ignore"
    ).copy()
    
    data_df = data_df.rename(columns={
        data_df.columns[0]: "Número animal"
    }).copy()
    
    data_df["Número animal"] = data_df["Número animal"].str.replace(
        "-", "/"
    ).copy()
    
    return data_df

def main(batch_id):
    try:
        validation.validate_input_string(batch_id)
        # Your program logic here
        print(f"Valid input: {batch_id}")
        month, year, number = batch_id.split('_')
        print(f"Month: {month}, Year: {year}, Number: {number}")
        
    except ValueError as e:
        print(f"Error: {e}")
        print("Usage: python my_program.py <MM_YYYY_N>")
        sys.exit(1)
    # Initialize configurations
    print(f"Processing: {batch_id}")
    try:
        # Setup and validate environment
        batch_paths = setup_batch_environment(batch_id)
        folder_img_path = batch_paths['img']
        folder_sg_path = batch_paths['sg_excel']
        folder_output = batch_paths['output']

        # Get configurations
        paths = config.get_base_paths()
        patterns = config.get_file_patterns()
        columns = config.get_column_settings()
        settings = config.get_data_settings()

        # Load and validate SG data
        data_sg = load_sg_data(
            folder_sg_path,
            patterns['sg_file'],
            settings,
            columns
        )

        # Setup prompt
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

        # Process images
        data_list = []
        cols_list = []
        
        for filename in sorted(os.listdir(folder_img_path)):
            if filename.endswith(('.jpeg', '.jpg')):
                clogs.log_file_processing(logger, filename)
                image_path = os.path.join(folder_img_path, filename)
                
                # Process image and validate data
                parsed_data, api_response = process_image_data(image_path, prompt_input)
                clogs.log_api_comment(logger, api_response)
                
                # Validate and update columns
                cols_list = validate_and_update_columns(parsed_data, cols_list)
                clogs.log_column_status(logger, "current", cols_list)
                
                # Create and process DataFrame
                data_df = create_and_process_dataframe(parsed_data[1:], cols_list, settings['year'])
                
                # Merge with SG data
                data_final = validation.safe_merge_dataframes(
                    data_df,
                    data_sg,
                    "Número animal"
                )
                
                data_final["Fecha Parto"] = data_final["Fecha Parto"].fillna("X*").copy()
                
                # Reorder columns
                cols_to_move = ["Número animal", "Fecha Parto"]
                data_final = processing.reorder_columns(data_final, cols_to_move)
                
                clogs.log_column_list(logger, data_final.columns.tolist())
                data_list.append(data_final)
                clogs.log_process_separator(logger)

        # Export processed data
        regular_file, final_file = postprocessing.export_data(
            data_list=data_list,
            folder_output=folder_output,
            batch_id=batch_id
        )
        
    except Exception as e:
        logger.error(f"Processing failed: {str(e)}")
        raise

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 khipu.py <batch_id>")
        print("Example: python3 khipu.py 01_2024_4")
        sys.exit(1)
    
    batch_id = sys.argv[1]
    main(batch_id)