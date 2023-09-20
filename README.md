# Project: STEDI Human Balance Analytics

## Introduction

Spark and AWS Glue are powerful tools that enable you to process data from various sources, categorize it, and prepare it for future querying for diverse purposes. As a data engineer on the STEDI Step Trainer team, your role involves extracting data generated by STEDI Step Trainer sensors and the mobile app. Subsequently, you'll organize this data into a data lakehouse solution on AWS. The objective is to make this data readily available for Data Scientists to use in training machine learning models.

This task goes beyond mere data collection; it also involves the organization and curation of the data to create a robust foundation for data scientists to leverage in their machine learning endeavors. 

## Project Details

The STEDI team has been diligently working on the development of a hardware device known as the STEDI Step Trainer. This innovative device serves a dual purpose:

- It trains users in performing a specific STEDI balance exercise.
- It is equipped with sensors that gather data for training a machine learning algorithm designed to detect steps.
Furthermore, there is a complementary mobile application that not only collects customer data but also interacts with the sensors on the device.

STEDI has garnered significant interest from millions of early adopters who are eager to purchase and utilize the STEDI Step Trainer.

A number of customers have already received their Step Trainers, installed the mobile app, and initiated their balance testing routines. The Step Trainer functions as a motion sensor, recording the distance of detected objects, while the app utilizes the mobile phone's accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team's objective is to leverage the motion sensor data to train a real-time machine learning model for accurate step detection. However, it is crucial to prioritize privacy considerations when determining which data can be used.

Some of the early adopters have graciously agreed to share their data for research purposes. As a result, only the data from these specific customers' Step Trainers and accelerometers should be incorporated into the training dataset for the machine learning model.

## Implementation

### Landing Zone

**Glue Tables**:

- [customer_landing.sql](script/customer_landing.sql)
- [accelerometer_landing.sql](script/accelerometer_landing.sql)

**Athena**:
Landing Zone data query results

*Customer Landing*:

<figure>
  <img src="images/customer_landing.png" alt="Customer Landing data" width=60% height=60%>
</figure>

*Accelerometer Landing*:

<figure>
  <img src="images/accelerometer_landing.png" alt="Accelerometer Landing data" width=60% height=60%>
</figure>

### Trusted Zone

**Glue job scripts**:

- [customer_landing_to_trusted.py](scripts/customer_landing_to_trusted.py)
- [accelerometer_landing_to_trusted_zone.py](scripts/accelerometer_landing_to_trusted.py)

**Athena**:
Trusted Zone Query results:

<figure>
  <img src="images/customer_trusted.png" alt="Customer Truested data" width=60% height=60%>
</figure>

### Curated Zone

**Glue job scripts**:

- [customer_trusted_to_curated.py](scripts/customer_trusted_to_curated.py)
- [trainer_trusted_to_curated.py](scripts/trainer_trusted_to_curated.py)
