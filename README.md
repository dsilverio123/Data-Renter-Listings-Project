# Data-Renter-Listings-Project
A cloud data pipeline to front-end application.

This project pulls from RapidAPIs Rent Application. Overall, the process is below:

<img src="https://github.com/dsilverio123/Data-Renter-Listings-Project/blob/main/Data%20Pipeline%20to%20React%20App.png?raw=true" alt="some_text">

The objective was to have users able to rental listings via web application. To do so, I used a remote AWS S3 instance to process the airflow applications.

## Project Components

1. Airflow (S3, T3.Medium Instance)
   - DAG - this DAG initiates the API Pull, Text to CSV, CSV to PostGreSQL server.

2. PostGRESQL Database
   - Schema - the schema is here if replication of the database is needed.

3. Node Application
   - Node - the code for the node server is here. It makes the /Get and /delete requests available for the front-end. It's connected to PostGreSQL.

4. React Application
   - React - this is also available in the code. Here users can view and delete listings.

<img src="https://github.com/dsilverio123/Data-Renter-Listings-Project/blob/main/0_TDN2SnAd_1mppT6J.png?raw=true" alt="some_text">
