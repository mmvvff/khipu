# file handling
import glob
import os
from dotenv import load_dotenv
import sys

# tabular data
import numpy as np
import pandas as pd

# User defined functions
path_scripts = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.append(path_scripts)
from src import (
    custom_logging as clogs,
    config,
    processing,
    postprocessing,
    validation
)

# Load environment variables
load_dotenv()

def setup_batch_environment(batch_id: str, validator: validation.Validator) -> dict:
    """Set up batch processing environment with validation"""
    batch_paths = config.ensure_batch_paths(batch_id)
    return validator.validate_batch_paths(batch_paths)

def load_sg_data(folder_path: str, file_pattern: str, settings: dict, 
                 columns: dict, validator: validation.Validator) -> pd.DataFrame:
    """Load and validate SG Excel data"""
    try:
        # Find and validate file
        file_path = glob.glob(os.path.join(folder_path, file_pattern))[0]
        if not file_path.endswith('.xlsx'):
            raise validation.ValidationError("FORMAT_ERROR", "Invalid Excel file format")

        # Read Excel file
        data_sg = pd.read_excel(
            file_path,
            header=settings['excel_settings']['header_row']
        )
        
        # Validate DataFrame
        validator.validate_dataframe(data_sg, columns['required_columns'])
        
        # Process DataFrame
        data_sg = data_sg.rename(columns=columns['rename_map'])
        data_sg = data_sg[columns['sg_columns']].dropna()
        data_sg["Fecha Parto"] = data_sg["Fecha Parto"].dt.strftime(
            settings['date_formats']['output']
        )
        
        return data_sg
        
    except IndexError:
        raise validation.ValidationError("FILE_ERROR", f"No matching file found in {folder_path}")
    except Exception as e:
        raise validation.ValidationError("SG_DATA_ERROR", str(e))

def process_image_data(image_path: str, prompt: str, 
                      validator: validation.Validator) -> tuple[list, str]:
    """Process image and extract data with validation"""
    # Validate image
    validator.validate_image(image_path)
    
    # Extract text
    result = processing.extract_img2text(image_path, prompt)
    data_string = result.content[0].text.split("[")[1].replace("]", "")
    parsed_data = postprocessing.parse_csv_string(data_string)
    
    if not parsed_data:
        raise validation.ValidationError("PARSE_ERROR", "No data extracted from image")
        
    return parsed_data, result.content[0].text

def process_and_validate_columns(parsed_data: list, existing_cols: list) -> list:
    """Process and validate column headers"""
    if not existing_cols:
        if not parsed_data[0]:
            raise validation.ValidationError("COLUMN_ERROR", "No column headers found")
        return parsed_data[0]
        
    if any("vaca" in s.lower() for s in parsed_data[0]) and (existing_cols != parsed_data[0]):
        return parsed_data[0]
        
    return existing_cols

def create_processed_dataframe(parsed_data: list, cols_list: list, year: int, 
                             validator: validation.Validator) -> pd.DataFrame:
    """Create and process DataFrame with validation"""
    # Create initial DataFrame
    data_df = pd.DataFrame(parsed_data[1:], columns=cols_list)
    validator.validate_dataframe(data_df, ["Número animal"])
    
    # Calculate flags
    data_df['flag_count'] = postprocessing.calculate_flag_counts(data_df)
    
    # Process date columns
    col_label_num = 1
    for col in data_df.iloc[:, 3:10].columns.tolist():
        # Clean values
        data_df[col] = postprocessing.clean_column_values(data_df[col])
        col_index = data_df.columns.get_loc(col)
        
        # Process date
        col_label_str = postprocessing.normalize_month(
            postprocessing.normalize_day(col.replace(".", ""))
        )
        date_str = postprocessing.convert_to_date(col_label_str, year)
        
        # Insert and rename columns
        data_df.insert(col_index, f'Fecha {col_label_num}', date_str)
        col_label_num += 1
        data_df = data_df.rename(columns={col: "Kg/Leche"})
    
    # Clean up DataFrame
    data_df = data_df.drop(
        columns=["Nombre", "Becerro", "Fecha PP", "#"],
        errors="ignore"
    )
    
    data_df = data_df.rename(columns={
        data_df.columns[0]: "Número animal"
    })
    
    data_df["Número animal"] = data_df["Número animal"].str.replace("-", "/")
    
    return data_df

def process_batch(batch_id: str, validator: validation.Validator) -> list[pd.DataFrame]:
    """Process entire batch with validation"""
    logger = validator.logger
    
    try:
        # Setup environment
        batch_paths = setup_batch_environment(batch_id, validator)
        
        # Get configurations
        patterns = config.get_file_patterns()
        columns = config.get_column_settings()
        settings = config.get_data_settings()
        
        # Load SG data
        data_sg = load_sg_data(
            batch_paths['sg_excel'],
            patterns['sg_excel'],
            settings,
            columns,
            validator
        )
        
        # Setup image processing
        prompt_input = f"""Instruction 1: Convert the text in the image to csv.
        Instruction 2: Employ a strict approach: add 1 asterisk next to the estimated values for those cells whose text-to-digit conversion are below a 99.75 percent confidence threshold.
        Instruction 3: Include in comments the confidence threshold used.
        Instruction 4: Do not use outlier-detection as criteria to flag the data.
        Instruction 5: If headers are present, include them.
        Instruction 6: Include any comments before returning output. Limit verbosity.
        Instruction 7: Return output enclosed in brackets to facilitate parsing.
        Instruction 8: Do not include any additional comments after final output.
        """
        
        # Process images
        data_list = []
        cols_list = []
        
        for filename in sorted(os.listdir(batch_paths['img'])):
            if filename.endswith(('.jpeg', '.jpg')):
                logger.info(f"Processing file: {filename}")
                image_path = os.path.join(batch_paths['img'], filename)
                
                try:
                    # Process image
                    parsed_data, api_response = process_image_data(
                        image_path, prompt_input, validator
                    )
                    logger.info(f"API Comment: {api_response.split('[')[0].strip()}")
                    
                    # Process columns
                    cols_list = process_and_validate_columns(parsed_data, cols_list)
                    logger.debug(f"Current columns: {cols_list}")
                    
                    # Create DataFrame
                    data_df = create_processed_dataframe(
                        parsed_data[1:], cols_list, settings['year'], validator
                    )
                    
                    # Merge with SG data
                    data_final = validator.validate_merge(
                        data_df, data_sg, "Número animal"
                    )
                    
                    data_final["Fecha Parto"] = data_final["Fecha Parto"].fillna("X*")
                    
                    # Reorder columns
                    cols_to_move = ["Número animal", "Fecha Parto"]
                    data_final = processing.reorder_columns(data_final, cols_to_move)
                    
                    logger.debug(f"Final columns: {data_final.columns.tolist()}")
                    data_list.append(data_final)
                    logger.debug("---")
                    
                except validation.ValidationError as e:
                    logger.error(f"Error processing {filename}: {e}")
                    continue
        
        if not data_list:
            raise validation.ValidationError("BATCH_ERROR", "No valid data processed")
            
        # Export data
        regular_file, final_file = postprocessing.export_data(
            data_list=data_list,
            folder_output=batch_paths['output'],
            batch_id=batch_id
        )
        
        return data_list
        
    except validation.ValidationError as e:
        logger.error(f"Batch processing failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise

def main(batch_id: str):
    logger = clogs.get_logger()
    validator = validation.Validator(logger)
    
    try:
        data_list = process_batch(batch_id, validator)
        logger.info("Processing completed successfully")
        
    except validation.ValidationError as e:
        logger.error(f"Processing failed: {e.code} - {e.message}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 khipu.py <batch_id>")
        print("Example: python3 khipu.py 01_2024_4")
        sys.exit(1)
    
    batch_id = sys.argv[1]
    main(batch_id)