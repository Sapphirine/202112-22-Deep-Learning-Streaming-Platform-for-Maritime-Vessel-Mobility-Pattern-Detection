# 202112-21-Deep-Learning-Streaming-Platform-for-Maritime-Vessel-Mobility-Pattern-Detection

Welcome to our project!

Much of this project was implemented within the AWS environment leveraging serverless offerings, such as S3, Glue, Managed Airflow (MWAA) and QuickSight.

As such, please note the following:

1) The ETL folder contains the PySpark ETL job code developed within the AWS Glue ETL environment.  This job is responsible for importing 
   raw AIS vessel positional data CSV files into Parquet files within an AWS S3 hosted datalake.
2) The DataPipelineCode folder contains the Airflow DAG and supporting classes (ImageFactory and Athena facade) responsible for querying the datalake
   established by item #1 and generating vessel track images.
3) The Juptyter Notebooks folder contains the notebooks for classifying the images from item #2 for deep  learning model training, validation and testing
   purposes.
