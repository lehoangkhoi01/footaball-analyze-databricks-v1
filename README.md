# Football Analyze Databricks
This project is a data engineering pipeline solution to a made-up business problem, created to aid in my learning and understanding of data pipelining.

## Project Overview
This project aims to solve the business need by building a comprehensive data pipeline on Databricks. The goal is to extract the footabll-related data such as: teams, results and match statistics from 3rd party API (https://www.api-football.com/), bring and transform it in the cloud environment to bring to the analyst team the ready-to-analyze data. The cleaned data will highlight the team performance, rankings, and match statistics through the their played matchs in season 2023-2024

## Business Requirements
The business has identified a gap in understanding the overview results of the football teams in 2023-2024 season, how they perform and which factors has the most affection to their results. The key requirements include:
- **Results and Ranking**: The overview results for each teams, how they perform in the home and away fields
- **Statistic**: Identify the statistics for top tier teams, how they possess the ball and shooting affection
- **Success Rate**: Show the effectiveness of converting chances into goals


## Solution Overview
To meet these requirements, the solution is broken down into the following components:

**1. Data Ingestion:**

- Develop the API call service to fetch data from 3rd party API.
- Save the API results as JSON format and store in the Volume in Databricks

**2. Data Transformation**

- Use Databricks to clean and transform the data.
- Organize the data into Bronze, Silver, and Gold layers for raw, cleansed, and aggregated data respectively.

**3. Automation:**

Schedule the pipeline to run daily, ensuring that the data and reports are always up-to-date.


## Projcet Setup and Structure
### Prerequisites
- Databricks Community account
- API Football credit (register free here: https://www.api-football.com/documentation-v3)

### Setup Databricks Environment
- Use Community edition and create DLT pipeline.
- Set pipeline variable for 'api-host' and volume directory
- Set the Databricks secret for API key

### Structure
- 'src' directory include the utility functions for football logic and spark dataframe action, schema definition and api service setup
- 'pipelines' directory include the python files for load ing, cleaning and transforming data and organize them into bronze, silver and gold layer
- 'explorations' directory include the notebooks for testing data purpose and also API fetching service