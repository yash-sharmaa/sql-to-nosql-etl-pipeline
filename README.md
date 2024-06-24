Here is a draft for the README file based on the provided Python files:

---

# Data Pipeline and Scheduler

This project consists of two main components: the `Data_pipeline.ipynb` notebook and the `insert_and_schedule.py` script. The data pipeline handles data processing and loading, while the insert and schedule script manages the scheduling and insertion of data into a MongoDB cluster.

## Data Pipeline

The `Data_pipeline.ipynb` notebook performs the following tasks:

1. **Data Loading**:
   - Loads data from CSV files into Pandas DataFrames.

2. **Data Cleaning**:
   - Cleans the DataFrames by handling missing values, correcting data types, and other preprocessing steps.

3. **Schema Extraction and Mapping**:
   - Extracts schema from SQL tables in SSMS (SQL Server Management Studio).
   - Maps DataFrame columns to the extracted schema.

4. **Loading Data into SQL Tables**:
   - Loads the mapped DataFrames into corresponding tables in SSMS.

5. **Running Aggregation Queries**:
   - Executes aggregation queries on the SQL tables remotely from the script itself.

6. **Saving Aggregation Results**:
   - Saves the results of the aggregation queries into a new DataFrame.

7. **Insight Generation**:
   - Performs insight generation from the already cleaned DataFrames.

8. **Saving DataFrames to Pickle File**:
   - Saves all the insight DataFrames and the aggregation DataFrame into a pickle file for later use.

## Insert and Schedule

The `insert_and_schedule.py` script performs the following tasks:

1. **Running the Data Pipeline Notebook**:
   - Executes the `Data_pipeline.ipynb` notebook to process and clean the data.

2. **Extracting Data from Pickle File**:
   - Loads the data from the pickle file back into DataFrames.

3. **Connecting to MongoDB**:
   - Connects to a MongoDB cluster using the provided URI.

4. **Loading Data into MongoDB**:
   - Loads the DataFrames into the corresponding collections in MongoDB.

5. **Scheduling**:
   - Runs the entire process every 3 hours using the Prefect library.

## Getting Started

### Prerequisites

- Python 3.x
- Pandas
- Pymongo
- Prefect
- nbformat
- nbconvert
- SQL Server Management Studio (SSMS)
- MongoDB Cluster

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/your-repo/data-pipeline.git
   cd data-pipeline
   ```

2. Install the required Python packages:
   ```bash
   pip install pandas pymongo prefect nbformat nbconvert
   ```

3. Set up your MongoDB cluster and update the `MONGO_URI` in the `insert_and_schedule.py` script.

### Usage

1. **Run the Data Pipeline**:
   - Open and run the `Data_pipeline.ipynb` notebook to process and clean your data.

2. **Schedule the Insert Script**:
   - Run the `insert_and_schedule.py` script to start the scheduling process:
     ```bash
     python insert_and_schedule.py
     ```

   - The script will execute the data pipeline, load the data into MongoDB, and repeat the process every 3 hours.

---