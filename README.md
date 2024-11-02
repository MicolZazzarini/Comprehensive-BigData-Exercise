# 🌐 Big Data Analysis Project

## 📄 Project Description

This project is a data analysis exercise focused on processing a large weather dataset stored in **HDFS**. Using **PySpark** 🐍, we analyze various measurements across multiple years and stations to extract insights on measurement counts, frequent temperature values, and wind speed occurrences. The results are saved as CSV files for further review.

## 🗂️ Dataset Structure

- **Entry Point**: `BDAchallenge2324`
- **Data Source**: 📂 HDFS directory at `hdfs://192.168.104.45:9000/user/amircoli/BDA2324/`
- **Structure**:
  - **Folders**: One folder per year
  - **Files**: Each station's data is saved as a separate CSV file named after the station
  - **Note**: Some fields may vary across files, and the program handles these inconsistencies.

## 🎯 Objectives

This project follows three main objectives:

1. **📊 Measurement Count by Year and Station**:
   - Calculates the number of measurements recorded per station each year.
   - Results are ordered by year and station.
   - **Example Output**:
     ```plaintext
     2000,s_1,100
     2000,s_2,98
     ```

2. **🌡️ Top 10 Most Frequent Temperatures in Highlighted Area**:
   - Extracts the 10 most frequently recorded temperatures within specific geographical coordinates `[(60,-135);(30,-90)]`.
   - Results are sorted by frequency and temperature.
   - **Example Output**:
     ```plaintext
     [(60,-135);(30,-90)],22.1,40
     [(60,-135);(30,-90)],21.17,30
     ```

3. **💨 Most Frequent Wind Speed**:
   - Finds the station with the most frequently occurring wind speed value (in knots), as indicated in the "WND" column of the dataset.
   - **Example Output**:
     ```plaintext
     234234,9,40
     ```

---

## 🔍 Code Explanation

Each script focuses on one of the objectives listed above, performing data analysis and saving the results.

### 📂 Script 1: Measurement Count by Year and Station

This script:
- Reads the dataset from **HDFS**.
- Adds columns for **Anno** (Year) and **Stazione** (Station) by parsing the file paths.
- Counts measurements per station per year and saves the sorted results as a CSV file.

### 🌡️ Script 2: Top 10 Most Frequent Temperatures in Highlighted Area

This script:
- Filters temperature measurements within specified geographical coordinates.
- Identifies the top 10 most frequent temperatures, adds a column for the coordinates, and saves the sorted results.

### 💨 Script 3: Most Frequent Wind Speed

This script:
- Extracts and analyzes wind speed data from the **WND** column.
- Finds the station with the most frequently occurring wind speed value and saves the result.

## 🛠️ Technologies Used

- **PySpark** 🐍: For data processing and analysis
- **HDFS** 📂: For distributed file storage
- **Python** 🐍: Language used for scripting

## ⚙️ Setup Instructions

1. Ensure you have access to the HDFS path specified in the dataset structure.
2. Set up a **PySpark** environment with access to the HDFS files.
3. Run each script independently to perform the data processing steps and generate output CSV files in the specified results directory.

## 📁 Results

The results for each objective are saved as CSV files in the `RESULT_DIR_DATASET` path, with filenames corresponding to each task:

- **Task 1**: `r1.csv`
- **Task 2**: `r2.csv`
- **Task 3**: `r3.csv`

Each CSV file contains the relevant sorted results based on the analysis described above.

## 👤 Author

- [MicolZazzarini](https://github.com/MicolZazzarini)

## License

This project is licensed under the [MIT License](LICENSE).



