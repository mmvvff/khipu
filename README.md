# Khipu - Dairy Farm Data Extraction System

## Overview
Khipu is a Python-based system that automates the extraction of data from handwritten dairy farm records. It uses Claude AI's image processing capabilities to convert handwritten tables into structured data, processes the extracted information, and generates standardized Excel reports for dairy farm management.

## Features
- Image-to-text conversion using Claude AI API
- Automated data extraction and processing
- Batch processing capabilities
- Data validation and error handling
- Configurable processing parameters
- Logging system
- Standardized output generation

## Prerequisites
- Python 3.x
- Anthropic API key for Claude
- Required Python packages (see `requirements.txt`)
- Spanish locale support for date processing

## Installation

1. Clone the repository:
```bash
git clone [repository-url]
cd khipu
```

2. Install required packages:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
Create a `.env` file in the project root with:
```
CLAUDE_API_KEY=your_api_key_here
```

## Project Structure
```
khipu/
├── src/
│   ├── config.py           # Configuration settings
│   ├── custom_logging.py   # Logging functionality
│   ├── postprocessing.py   # Data post-processing utilities
│   ├── processing.py       # Core processing functions
│   ├── validation.py       # Data validation functions
│   └── khipu_v01.py       # Main program
├── _data/                  # Data directory
│   └── [batch_id]/
│       ├── 1_img/         # Input images
│       ├── 2_sg_excel/    # Source Excel files
│       └── 3_output/      # Processed outputs
├── logs/                   # Log files
└── output/                 # General output directory
```

## Usage

Run the program with a batch ID:
```bash
python khipu_v01.py <batch_id>
```

Example:
```bash
python khipu_v01.py 01_2024_4
```

### Batch Directory Structure
Each batch should follow this structure:
- `1_img/`: Contains JPEG/JPG images of dairy data
- `2_sg_excel/`: Contains Excel files with naming pattern "Fecha*Parto*.xlsx"
- `3_output/`: Destination for processed files

### Input Requirements
1. Images (.jpg/.jpeg):
   - Must contain tabular data
   - Should include column headers with "vaca" in at least one header

2. Excel Files:
   - Must follow naming convention: "Fecha*Parto*.xlsx"
   - Required columns: "Número animal", "Fecha Parto"

## Configuration

Key configuration settings can be modified in `config.py`:
- File paths and directory structure
- Allowed file patterns
- Column settings and mappings
- Data processing parameters
- Logging configuration

## Output Files

The system generates two Excel files per batch:
1. `leche_[batch_id].xlsx`: Regular output file
2. `leche_[batch_id]_final.xlsx`: Final formatted output

Each file contains processed data with:
- Animal identification numbers
- Milk production data
- Calving dates
- Data quality flags

## Logging

The system maintains detailed logs in the `logs` directory:
- Daily rotating log files
- Console output
- Processing status and errors
- Data validation results

## Error Handling

The system includes comprehensive error handling for:
- File validation
- Image processing
- Data parsing
- DataFrame operations
- API communication

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

[Specify your license here]

## Acknowledgments

- Claude AI by Anthropic for image processing
- [Add other acknowledgments]

## Support

For support or questions, please [specify contact method or raise an issue on GitHub]