# Research-Kickstarter

https://drive.google.com/drive/folders/1hgxqro_USLr5z2FEKyaJ3NE5NYEIuQW1?ths=true

# Kickstarter Data Extractor

## Introduction

This program is designed to extract data from Kickstarter projects and store it in a SQLite database. It utilizes web scraping techniques to gather information from project URLs provided in a CSV file.

## Setup

Before running the program, ensure that you have the following setup:

1. **Python Environment**: Make sure you have Python installed on your system. The program is written in Python 3.
2. **Dependencies**: Install the required dependencies by running:
3. **Chrome Driver**: Download the appropriate ChromeDriver executable and specify its path in the `DRIVER_PATH` variable. You can download ChromeDriver from https://chromedriver.chromium.org/downloads.
4. **CSV Input**: Prepare a CSV file containing the URLs of Kickstarter projects to be scraped. Set the path to this CSV file in the `DATA_PATH` variable.
5. **Json Input**: Prepare or download a JSON file containing the URLs of Kickstarter projects to be scraped. Set the path to this JSON file in the `DATA_PATH` variable.
6. **Output Directory**: Specify the path where you want the output SQLite database to be saved in the `OUTPUT_PATH` variable.

## Configuration

Before running the program, you may need to adjust some configuration variables in the code:

- `MISSING`: Define the value to be entered in case of missing data. The default value is an empty string `""`.

- `TESTING`: Set this variable to `1` if you are testing the program, and `0` otherwise.

- `chunk_size`: Specify the number of URLs to extract per session.

- `process_size`: Define the number of processes per attempt.

- `icon_num`: Set the taskbar location of the VPN changer in window.

## Usage

To run the program, execute the main script `project_data_extractor.py`. Make sure all required variables are properly configured before running the script.

```bash
python project_data_extractor.py
