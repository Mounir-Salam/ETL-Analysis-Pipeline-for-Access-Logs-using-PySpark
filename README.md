# ETL & Analysis Pipeline for Access Logs using PySpark

## Overview
This is a small project I built to refresh and test my knowledge in the setup and use of PySpark. The project is built as a CLI tool that takes in an access logs file as input and outputs the cleaned data in both CSV (default) and Parquet (optional) formats. The tool also has the option to save the cleaned data to MongoDB.

The project is still a work in progress, but it's a good example of how to use PySpark to process and analyze access logs data.

## Requirements

- The project has been tested on Python 3.10.11
- Install the required packages using `pip install -r requirements.txt` (I suggest setting up a virtual environment)
- Pyspark 3.1.3
- Hadoop/winutils 3.2.x
- MongoDB (either set up locally or use a cloud database)

## ETL Tool
0. If needed, modify the `.env` file for your specified setup. Make sure to set the correct MongoDB connection details, along with the location of your python interpreter and hadoop installation. You can also add them to your system's environemt variables if needed.
1. On your command line interface, navigate to the project directory and run the project through the main script `process_logs.py`:
```
python -m scripts.process_logs
```
2. Depending on your specified options, the cleaned data will be saved to your specified output folder in CVS and/or Parquet formats, and/or in your specified MongoDB database.

### Options

- `--path`: Path to the log file (default: `data/access_logs/sample.log`).
- `--output_path`: Path to save the cleaned log data (default: `output/`).
- `--log_format`: Log format to use for parsing the logs. Current available format options:
    - `combined` (default)
    - `common`
- `--no_csv_output`: Disable CSV output (default: enabled).
- `--parquet_output`: Enable Parquet output (default: disabled).
- `--mongo_output`: Enable MongoDB output (default: disabled).

## Analysis Tools
Stay tuned :)