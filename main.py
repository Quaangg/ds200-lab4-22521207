from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, abs, log1p, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.ml import Pipeline
from pyspark.ml.feature import Imputer, VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

pipeline_model_global = None

def process_batch(batch_df, batch_id):

    global pipeline_model_global

    print(f"\n--- Processing Streaming Batch ID: {batch_id} ---")
    print(f"--- Debug Info for Batch ID: {batch_id} ---")
    print("Schema of incoming batch_df:")
    try:
        batch_df.printSchema()
    except Exception as e:
        print(f"Error printing schema: {e}")
    print("First few rows of incoming batch_df:")
    try:
        batch_df.show(5, truncate=False)
    except Exception as e:
        print(f"Error showing data: {e}")
    print("--- End Debug Info ---")

    if batch_df.isEmpty():
        print("Streaming batch is empty, skipping.")
        return

    if pipeline_model_global:
        try:
            print("Applying trained pipeline model and making predictions...")
            predictions = pipeline_model_global.transform(batch_df)
            print(f"Predictions for streaming batch {batch_id}:")
            if "Price" in predictions.columns and "prediction" in predictions.columns:
                predictions.select("Price", "prediction").show(5)
            else:
                print("Expected columns 'Price' or 'prediction' not found in predictions DataFrame.")
                predictions.printSchema()
        except Exception as e:
            print(f"Error transforming or predicting on streaming batch: {e}")
            print("Schema of streaming batch_df:")
            batch_df.printSchema()
            batch_df.show(3, truncate=False)
    else:
        print("Initial model not trained yet. Skipping prediction for this batch.")

def main():

    HOST = 'localhost'
    PORT = 9999

    # Create SparkSession
    spark = SparkSession \
        .builder \
        .appName("SparkStreamingLaptopPrice") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("SparkSession created.")

    # --- BATCH PROCESSING FOR INITIAL MODEL TRAINING ---
    historical_schema_csv = StructType([
        StructField("Brand", StringType(), True),
        StructField("Processor_Speed", StringType(), True),
        StructField("RAM_Size", StringType(), True),
        StructField("Storage_Capacity", StringType(), True),
        StructField("Screen_Size", StringType(), True),
        StructField("Weight", StringType(), True),
        StructField("Price", StringType(), True)
    ])

    HISTORICAL_CSV_PATH = r'Laptop_price.csv'
    print(f"Reading historical data from {HISTORICAL_CSV_PATH} for initial training...")
    try:
        historical_df_raw = spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("sep", ",") \
            .schema(historical_schema_csv) \
            .load(HISTORICAL_CSV_PATH)

        print(f"Successfully read {historical_df_raw.count()} rows from historical data.")
        print("Historical data raw schema:")
        historical_df_raw.printSchema()
        print("First few rows of raw historical data:")
        historical_df_raw.show(5, truncate=False)
    except Exception as e:
        print(f"Error reading historical CSV file: {e}")
        print("Cannot proceed with initial training. Please check the file path and format.")
        spark.stop()
        return

    # Cast columns to appropriate types
    try:
        historical_df_casted = historical_df_raw.select(
            col("Brand").cast(StringType()).alias("Brand"),
            col("Processor_Speed").cast(DoubleType()).alias("Processor_Speed"),
            col("RAM_Size").cast(IntegerType()).alias("RAM_Size"),
            col("Storage_Capacity").cast(IntegerType()).alias("Storage_Capacity"),
            col("Screen_Size").cast(DoubleType()).alias("Screen_Size"),
            col("Weight").cast(DoubleType()).alias("Weight"),
            col("Price").cast(DoubleType()).alias("Price")
        )
        print("Historical data schema after casting:")
        historical_df_casted.printSchema()
        print("First few rows after casting:")
        historical_df_casted.show(5, truncate=False)
    except Exception as e:
        print(f"Error during historical data casting: {e}")
        spark.stop()
        return

    try:
        target_col = "Price"
        historical_df_cleaned = historical_df_casted.na.drop(subset=[target_col])
        print(f"Historical data rows after dropping null target: {historical_df_cleaned.count()}")

        if historical_df_cleaned.isEmpty():
            print("Historical data is empty after cleaning. Cannot train initial model.")
            spark.stop()
            return

        # Split data into training and testing sets
        train_df, test_df = historical_df_cleaned.randomSplit([0.8, 0.2], seed=42)
        print(f"Historical data split: {train_df.count()} training rows, {test_df.count()} testing rows.")

        if train_df.isEmpty() or test_df.isEmpty():
            print("Train or test split resulted in empty DataFrames. Cannot train/evaluate.")
            spark.stop()
            return

        print("Train data schema:")
        train_df.printSchema()
        print("Sample training data:")
        train_df.select("Processor_Speed", "RAM_Size", "Storage_Capacity", "Screen_Size", "Weight", "Price").show(5, truncate=False)

        numerical_cols = ["Processor_Speed", "RAM_Size", "Storage_Capacity", "Screen_Size", "Weight"]
        stages = []
        num_imputer = Imputer(
            inputCols=numerical_cols,
            outputCols=[c + "_imputed" for c in numerical_cols],
            strategy="mean"
        )
        stages.append(num_imputer)
        numerical_cols_for_assembler = [c + "_imputed" for c in numerical_cols]
        assembler_inputs = numerical_cols_for_assembler
        print("Assembler inputs:", assembler_inputs)
        if not assembler_inputs:
            print("No inputs for VectorAssembler. Cannot train model.")
            spark.stop()
            return

        vector_assembler = VectorAssembler(
            inputCols=assembler_inputs,
            outputCol="features_assembled",
            handleInvalid="skip"
        )
        scaler = StandardScaler(
            inputCol="features_assembled",
            outputCol="features",
            withStd=True,
            withMean=True
        )
        stages.extend([vector_assembler, scaler])
        debug_pipeline = Pipeline(stages=stages)
        debug_model = debug_pipeline.fit(train_df)
        debug_transformed = debug_model.transform(train_df)
        print("Schema after VectorAssembler and Scaler:")
        debug_transformed.printSchema()
        print("Sample data after feature engineering:")
        debug_transformed.select("features", "Price").show(5, truncate=False)

        lr = LinearRegression(featuresCol="features", labelCol=target_col)
        stages.append(lr)
        pipeline_estimator = Pipeline(stages=stages)
        print("Training initial pipeline model on historical training data...")
        try:
            global pipeline_model_global
            pipeline_model_global = pipeline_estimator.fit(train_df)
            print("Initial pipeline model trained successfully.")


            print("Evaluating initial pipeline model on historical test data...")
            try:
                test_predictions = pipeline_model_global.transform(test_df)
                evaluator = RegressionEvaluator(labelCol=target_col, predictionCol="prediction")

                # MAE
                evaluator.setMetricName("mae")
                mae = evaluator.evaluate(test_predictions)
                
                # RMSE
                evaluator.setMetricName("rmse")
                rmse = evaluator.evaluate(test_predictions)
                
                # R²
                evaluator.setMetricName("r2")
                r2 = evaluator.evaluate(test_predictions)

                # MAPE 
                test_predictions = test_predictions.withColumn(
                    "abs_percent_error",
                    when(col(target_col) != 0, abs((col(target_col) - col("prediction")) / col(target_col)) * 100).otherwise(0)
                )
                mape = test_predictions.selectExpr("avg(abs_percent_error) as mape").collect()[0]["mape"]

                # MSLE 
                test_predictions = test_predictions.filter((col(target_col) > 0) & (col("prediction") > 0))
                test_predictions = test_predictions.withColumn(
                    "log_error",
                    (log1p(col(target_col)) - log1p(col("prediction"))) ** 2
                )
                msle = test_predictions.selectExpr("avg(log_error) as msle").collect()[0]["msle"]
                
                print("\nModel Evaluation Metrics on Historical Test Data:")
                print(f"Mean Absolute Error (MAE): {mae:.4f}")
                print(f"Root Mean Squared Error (RMSE): {rmse:.4f}")
                print(f"R-squared (R²): {r2:.4f}")
                print(f"Mean Absolute Percentage Error (MAPE): {mape:.4f}%")
                print(f"Mean Squared Logarithmic Error (MSLE): {msle:.4f}")

                print("\nSample predictions vs actual from historical test data:")
                test_predictions.select(target_col, "prediction").show(5)

            except Exception as e:
                print(f"Error evaluating initial pipeline model: {e}")
        except Exception as e:
            print(f"Error fitting initial pipeline model: {e}")
            spark.stop()
            return
    except Exception as e:
        print(f"Error during historical data cleaning or splitting: {e}")
        spark.stop()
        return

    # --- SPARK STRUCTURED STREAMING SETUP ---
    streaming_schema_csv = StructType([
        StructField("Brand", StringType(), True),
        StructField("Processor_Speed", StringType(), True),
        StructField("RAM_Size", StringType(), True),
        StructField("Storage_Capacity", StringType(), True),
        StructField("Screen_Size", StringType(), True),
        StructField("Weight", StringType(), True),
        StructField("Price", StringType(), True)
    ])

    if pipeline_model_global is not None:
        print(f"Starting Spark Streaming from socket {HOST}:{PORT}...")
        try:
            lines_df = spark \
                .readStream \
                .format("socket") \
                .option("host", HOST) \
                .option("port", PORT) \
                .load()

            split_col = split(lines_df['value'], ',')
            streaming_parsed_df = lines_df.select(
                split_col.getItem(1).alias("Processor_Speed").cast(DoubleType()),
                split_col.getItem(2).alias("RAM_Size").cast(IntegerType()),
                split_col.getItem(3).alias("Storage_Capacity").cast(IntegerType()),
                split_col.getItem(4).alias("Screen_Size").cast(DoubleType()),
                split_col.getItem(5).alias("Weight").cast(DoubleType()),
                split_col.getItem(6).alias("Price").cast(DoubleType())
            )

            query = streaming_parsed_df \
                .writeStream \
                .foreachBatch(process_batch) \
                .outputMode("update") \
                .trigger(processingTime="15 seconds") \
                .start()

            print("Spark Streaming query started. Waiting for data from streamer...")
            query.awaitTermination()
        except Exception as e:
            print(f"Error starting streaming query: {e}")
        finally:
            spark.stop()
    else:
        print("Initial model training failed. Skipping streaming.")
        spark.stop()

if __name__ == "__main__":
    main()