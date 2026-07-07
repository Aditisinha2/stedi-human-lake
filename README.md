# STEDI Human Balance Analytics Data Lakehouse

## Project Overview

This project builds a data lakehouse pipeline for STEDI Human Balance Analytics using AWS services. The pipeline ingests JSON data from Amazon S3, processes it using AWS Glue Studio, stores trusted and curated outputs back in S3, and validates the results using Amazon Athena.

The project follows three data zones:

- Landing Zone
- Trusted Zone
- Curated Zone

## AWS Services Used

- Amazon S3
- AWS Glue Studio
- AWS Glue Data Catalog
- Amazon Athena

## S3 Bucket

Project bucket used:

```text
s3://stedihumanlake/
```

## Data Zones and S3 Paths

### Landing Zone

text
s3://stedihumanlake/customer/customer-landing/
s3://stedihumanlake/accelerometer/accelerometer-landing/
s3://stedihumanlake/step_trainer/step_trainer-landing/


### Trusted Zone


s3://stedihumanlake/customer/customer-trusted/
s3://stedihumanlake/accelerometer/accelerometer-trusted/
s3://stedihumanlake/step_trainer/step_trainer-trusted/


### Curated Zone


s3://stedihumanlake/customer/customer-curated/
s3://stedihumanlake/machine-learning-curated/


## Glue Jobs

The following AWS Glue Studio jobs were created:

customer_landing_to_trusted.py: Filters customer landing data to include only customers who agreed to share research data |
accelerometer_landing_to_trusted.py: Joins accelerometer landing data with customer trusted data using email/user |
step_trainer_trusted.py:Joins step trainer landing data with customer curated data using serial number |
customer_trusted_to_curated.py: Joins customer trusted data with accelerometer trusted data using email/user |
machine_learning_curated.py: Joins step trainer trusted data with accelerometer trusted data using sensor reading time/timestamp |

## Athena SQL Files

The athena_sql/ folder contains SQL DDL scripts and verification queries:
ustomer_landing.sql: Creates the customer landing Athena table |
accelerometer_landing.sql: Creates the accelerometer landing Athena table |
step_trainer_landing.sql: Creates the step trainer landing Athena table |
machine_learning_curated.sql:Creates the machine learning curated Athena table |
verification_queries.sql:Contains count checks and validation queries |

## Expected Row Counts

Landing
Customer: 956
Accelerometer: 81273
Step Trainer: 28680
Trusted
Customer: 482
Accelerometer: 40981
Step Trainer: 14460
Curated
Customer: 482
Machine Learning: 43681

## Project Output Screenshots

The screenshots folder contains Athena and Glue Studio screenshots showing:

- Glue Studio visual ETL jobs
- Landing zone table counts
- Trusted zone table counts
- Curated zone table counts
- Customer privacy filtering validation
- Final machine learning curated output count

SHOW TABLES;
 Final Result

The completed data lakehouse pipeline successfully produces the required final curated machine learning dataset with:


machine_learning_curated = 43,681 rows

