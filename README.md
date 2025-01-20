
# **Documentation**

  

This document outlines the steps required to set up and run the Spotify

Analysis Pipeline DAG using Apache Airflow.

  

## **Prerequisites**

  

Before running the DAG, ensure the following:

  

-  **Apache Airflow** is installed and configured.

  

- A **Python environment** with necessary dependencies is set up.

  

- The **Airflow DAGs folder** is accessible.

  

## **Installation**

  

### **1. Set Up a Virtual Environment (Optional but Recommended)**

  

Creating a virtual environment is recommended to isolate dependencies:

  

*python -m venv airflow_env*

  

*source airflow_env/bin/activate* 
\# On Windows: *airflow_env\\Scripts\\activate*

  

### **2. Install Apache Airflow**

  

To install Apache Airflow, run the following command:

  

*pip install apache-airflow*

  

### **3. Install Additional Dependencies**

  

Install any other dependencies required by the DAG, including those from

*requirements.txt*:

  

*pip install -r requirements.txt*

### **4. Download Kaggle Dataset**

  

Run *download_dataset.py* to download the dataset used in the pipeline.


  

## **Running the DAG**

  

### **1. Initialize Airflow Database**

  

To initialize the Airflow database, use the following command:

  

*airflow db init*

  

### **2. Start Airflow Scheduler & Web Server**

  

To start the scheduler and web server:

- **Start the Airflow scheduler**: *airflow scheduler*


- **Start the Airflow web server** (to view the DAGs in the UI): 
	*airflow webserver \--port 8080*

  

### **3. Verify DAG Availability**

  

- Open the Airflow UI in a browser: [http://localhost:8080](http://localhost:8080/)

  

- Check if ***spotify_analysis_pipeline*** appears in the DAGs list.

  

### **4. Trigger the DAG**

  

You can manually trigger the DAG in the following ways:

  

-  **Via the Command Line**:
*airflow dags trigger spotify_analysis_pipeline*


-  **Via the Airflow UI**:

  

	- Navigate to the DAGs list on the UI.

  

	- Find spotify_analysis_pipeline.

  

	- Click on the trigger button to start the DAG.
