# ds200-lab4-22521207

22521207

# Laptop Price Streaming Prediction with Spark Structured Streaming

## Project Description

This project uses Apache Spark Structured Streaming to build a laptop price prediction model based on features such as CPU speed, RAM size, storage capacity, screen size, and weight.

The process consists of two main parts:

* **Part 1:** Read historical data from a CSV file to train the initial prediction model.
* **Part 2:** Set up Spark Structured Streaming to read input data as a stream from a server socket, process data in batches, and apply the trained model to predict prices in real time.

---

## How to Use

### 1. Initial Model Training with Historical Data

* The historical laptop price data is in the file `Laptop_price.csv`.
* A Linear Regression model is trained on this data with preprocessing steps including:

  * Imputation of missing values with mean values
  * Data standardization
  * Pipeline creation for model training
* After training, the model is stored globally in the variable `pipeline_model_global`.

### 2. Setting up Streaming to Receive Data and Predict

* Spark reads streaming data from a TCP socket (host: `localhost`, port: `9999`).
* Incoming stream data is CSV strings containing laptop features.
* Each batch is processed by the `process_batch` function to:

  * Print schema info and preview some data rows
  * Apply the trained model to predict laptop prices
  * Display prediction results
* Streaming triggers every 15 seconds.

### 3. Python Server to Send Data

* A Python server reads the `Laptop_price.csv` file and sends rows one by one through the TCP socket to Spark Streaming.
* The server waits for Spark to connect, then sends the data sequentially.
* There is a 0.2-second delay between sending each row to simulate streaming data.

---

## File Structure

* `streaming_laptop_price.py`: Main Spark Structured Streaming script for reading streaming data and predicting prices.
* `data_streamer.py`: Python server script for sending CSV data via socket to Spark Streaming.

---

## Environment Requirements

* Apache Spark (version supporting Structured Streaming)
* Python 3.x
* PySpark
* pandas (for data sending server)

---

## How to Run (terminal)

1. Run the streaming script:

```bash
python streamer.py
```

2. Run the main Spark streaming application:

```bash
spark-submit main.py

