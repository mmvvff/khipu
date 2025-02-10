import os
import pandas as pd

class ValidationError(Exception):
    """Base validation error with error code"""
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")

class Validator:
    """Centralized validation logic"""
    def __init__(self, logger):
        self.logger = logger
    
    def validate_batch_paths(self, batch_paths: dict) -> dict:
        """Validate and return batch processing paths"""
        for path in batch_paths.values():
            if not os.path.exists(path):
                self.logger.error(f"Directory not found: {path}")
                raise ValidationError("PATH_ERROR", f"Directory not found: {path}")
        return batch_paths
    
    def validate_image(self, image_path: str) -> str:
        """Validate and return image path"""
        if not os.path.exists(image_path):
            self.logger.error(f"Image not found: {image_path}")
            raise ValidationError("IMAGE_ERROR", f"Image not found: {image_path}")
            
        if not image_path.lower().endswith(('.jpeg', '.jpg')):
            self.logger.error(f"Invalid image format: {image_path}")
            raise ValidationError("FORMAT_ERROR", "Invalid image format")
            
        if os.path.getsize(image_path) == 0:
            self.logger.error(f"Empty image file: {image_path}")
            raise ValidationError("EMPTY_ERROR", "Empty image file")
            
        return image_path
    
    def validate_dataframe(self, df: pd.DataFrame, required_cols: list[str]) -> pd.DataFrame:
        """Validate and return DataFrame"""
        if df.empty:
            self.logger.error("Empty DataFrame")
            raise ValidationError("DF_ERROR", "Empty DataFrame")
            
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            self.logger.error(f"Missing columns: {missing}")
            raise ValidationError("COLUMN_ERROR", f"Missing columns: {missing}")
            
        return df
    
    def validate_merge(self, left_df: pd.DataFrame, right_df: pd.DataFrame, 
                      on_column: str) -> pd.DataFrame:
        """Validate and perform DataFrame merge"""
        # Validate input DataFrames
        self.validate_dataframe(left_df, [on_column])
        self.validate_dataframe(right_df, [on_column])
        
        try:
            result = left_df.merge(right_df, on=on_column, how="left")
            if result.empty:
                self.logger.error("Merge resulted in empty DataFrame")
                raise ValidationError("MERGE_ERROR", "Merge resulted in empty DataFrame")
            return result
        except Exception as e:
            self.logger.error(f"Merge failed: {str(e)}")
            raise ValidationError("MERGE_ERROR", str(e))