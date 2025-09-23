# Import required libraries
# file handling
import glob
import os
from dotenv import load_dotenv
import sys

# tabular data
import pandas as pd

# Add scripts folder to Python path and import custom modules
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..")))

from src import custom_logging as clogs, config, processing, postprocessing

# Custom exception for column errors
class NoColsError(Exception):
    pass


def process_image(
        image_path: str,
        prompt_input: str,
        cols_list: list,
        year: int,
        data_sg: pd.DataFrame, logger
        ) -> tuple[pd.DataFrame, list]:
    """
    Process a single image and
    return the processed DataFrame and updated column list.
    """
    # Process image with Claude API
    try:
        result = processing.extract_img2text(image_path, prompt_input)
        clogs.log_api_comment(logger, result.content[0].text)
    except Exception as e:
        logger.error(f"Error processing image: {e}")
        sys.exit(1)  # Exit with error code 1
        # return None, cols_list

    # Parse the API response
    data_string = result.content[0].text.split("[")[1].replace("]", "")
    parsed_data = postprocessing.parse_csv_string(data_string)

    # Handle column headers
    if not cols_list:
        if any("vaca" in s.lower() for s in parsed_data[0]):
            cols_list = parsed_data[0]
            clogs.log_column_status(logger, "initialized", cols_list)
        else:
            clogs.log_validation_error(
                logger, parsed_data[0], "No 'vaca' found")
            raise NoColsError("No columns found: Check image folder")
    else:
        if any(
            "vaca" in s.lower() for s in parsed_data[0]) and (
                cols_list != parsed_data[0]):
            cols_list = parsed_data[0]
            clogs.log_column_status(logger, "updated", cols_list)
        else:
            clogs.log_column_status(logger, "current (no update)", cols_list)

    # Create and process DataFrame
    try:
        data_df = pd.DataFrame(parsed_data[1:], columns=cols_list)
        data_df = process_dataframe(data_df, year, data_sg, logger)
        return data_df, cols_list
    except Exception as e:
        logger.error(f"Error creating DataFrame: {e}")
        sys.exit(1)  # Exit with error code 1


def process_dataframe(
        data_df: pd.DataFrame,
        year: int,
        data_sg: pd.DataFrame,
        logger
        ) -> pd.DataFrame:
    """Process the DataFrame with all necessary transformations."""
    # Calculate flags
    data_df['flag_count'] = postprocessing.calculate_flag_counts(data_df)

    # Process date and milk production columns
    col_label_num = 1
    for col in data_df.iloc[:, 3:10].columns.tolist():
        data_df[col] = postprocessing.clean_column_values(data_df[col])
        col_index = data_df.columns.get_loc(col)
        col_label_str = postprocessing.normalize_month(
            postprocessing.normalize_day(col.replace(".", "")))

        data_df.insert(
            col_index, f'Fecha {col_label_num}',
            postprocessing.convert_to_date(col_label_str, year=year))
        col_label_num += 1
        data_df = data_df.rename(columns={col: "Kg/Leche"}).copy()

    # Clean up DataFrame
    data_df = data_df.drop(
        columns=["Nombre", "Becerro", "Fecha PP", "#"],
        errors="ignore").copy()

    # Format animal number column
    data_df = data_df.rename(columns={
        data_df.columns[0]: "Número animal"
    }).copy()
    data_df["Número animal"] = data_df["Número animal"].str.replace(
        "-", "/").copy()

    # Merge with Excel data
    clogs.log_dataframe_columns(logger, "data_df", data_df.columns.tolist())
    data_final = data_df.merge(data_sg, on="Número animal", how="left")
    clogs.log_dataframe_columns(
        logger, "data_final", data_final.columns.tolist())

    # Handle missing dates and reorder columns
    data_final["Fecha Parto"] = data_final["Fecha Parto"].fillna("X*").copy()
    cols_to_move = ["Número animal", "Fecha Parto"]
    data_final = processing.reorder_columns(data_final, cols_to_move)

    return data_final


def setup_processing(batch_id: str) -> tuple:
    """Set up all necessary configurations and paths for processing."""
    # Initialize environment and logging
    load_dotenv()
    logger = clogs.get_logger()

    # Set up batch processing paths
    batch_paths = config.ensure_batch_paths(batch_id)

    # Load configuration settings
    patterns = config.get_file_patterns()
    columns = config.get_column_settings()
    settings = config.get_data_settings()

    # Process Excel configuration file
    file_path = glob.glob(
        os.path.join(
            batch_paths['sg_excel'],
            patterns['sg_excel']))[0]
    data_sg = pd.read_excel(
        file_path,
        header=settings['excel_settings']['header_row']
    )

    # Clean up Excel data
    data_sg = data_sg.rename(columns=columns['rename_map']).copy()
    data_sg = data_sg[columns['sg_columns']].dropna().copy()
    data_sg["Fecha Parto"] = data_sg["Fecha Parto"].dt.strftime(
        settings['date_formats']['output'])

    return batch_paths, settings, data_sg, logger


def main(batch_id: str) -> None:
    """Main function to process batch of images."""
    try:
        # Setup initial configurations
        batch_paths, settings, data_sg, logger = setup_processing(batch_id)

        # Define prompt for image processing
        conf_level = 95
        prompt_input = f"""Instruction 1: Convert the text in the image to csv.
Instruction 2: Employ a strict approach: add 1 asterisk next
to the estimated values for those cells whose text-to-digit conversion
are below a {conf_level} percent confidence threshold;
it does not matter if data is over-flagged.
Instruction 3: Include in comments the confidence threshold used.
Instruction 4: Do not use outlier-detection as criteria to flag the data.
Instruction 5: Make sure to not use outlier-detection as criteria to flag data.
Instruction 6: If headers are present, include them.
If no headers are found, do not include any.
Instruction 7: Include any comments before returning output. Limit verbosity.
Instruction 8: Return output enclosed in brackets to facilitate parsing.
Instruction 9: Do not include any additional comments after final output.
"""

        # Initialize data storage
        data_list = []
        cols_list = []

        # Process each image
        for filename in sorted(os.listdir(batch_paths['img'])):
            if filename.endswith(('.jpeg', '.jpg')):
                clogs.log_file_processing(logger, filename)
                image_path = os.path.join(batch_paths['img'], filename)

                data_df, cols_list = process_image(
                    image_path, prompt_input, cols_list, 
                    settings['year'], data_sg, logger
                )

                if data_df is not None:
                    data_list.append(data_df)
                    clogs.log_process_separator(logger)

        # Export processed data
        if data_list:
            regular_file, final_file = postprocessing.export_data(
                data_list=data_list,
                folder_output=batch_paths['output'],
                batch_id=batch_id
            )
            logger.info(f"Processing completed. Files saved: {regular_file}, {final_file}"
                    )
        else:
            logger.error("No data processed successfully")

    except Exception as e:
        logger.error(f"An error occurred during processing: {e}")
        raise


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 khipu_v01.py <batch_id>")
        print("Example: python3 khipu_v01.py 01_2024_4")
        sys.exit(1)

    batch_id = sys.argv[1]
    main(batch_id)
