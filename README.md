# Top_Steam_Games_EDA
## Background
Since its launch in 2003, Steam has evolved into one of the largest and most popular platforms for distributing PC games, attracting millions of active users daily. The platform offers more than just game purchases—it also includes features like online communities, user reviews, and gameplay statistics. In the highly competitive gaming industry, a deep understanding of the factors influencing sales, reviews, and player engagement is crucial for developers and publishers to make strategic decisions.

Data for this project can be accessed __[here](https://www.kaggle.com/datasets/alicemtopcu/top-1500-games-on-steam-by-revenue-09-09-2024)__

## Objectives
As a Data Analyst, my role is to provide insights into the performance of games on the Steam platform. The analysis aims to address several key questions, including:

1. Identifying variables that have the most significant impact on sales and revenue.
2. Understanding average playtime trends and their relationship to review scores and sales.
3. Analyzing game performance based on publisher classifications (AAA, AA, or indie) and developer teams to recommend effective publishing strategies.
4. Evaluating pricing strategies to optimize revenue by considering sales and review scores.

**Target Audience**
The insights from this analysis will be valuable for:

- **Publishing and development teams**: Offering recommendations on game development and publishing strategies to stay competitive in the Steam marketplace.
- **Marketing teams**: Providing insights into optimal pricing and promotional strategies to boost sales and revenue.
- **Management**: Delivering data-driven insights to support investment decisions and future product development.

## Workflow
Tools Used:
- **Airflow**: Orchestrates workflows.
- **PostgreSQL**: Relational database for structured data.
- **Elasticsearch**: Indexes and searches data.
- **Kibana**: Visualizes the indexed data.
- **Docker**: Containers for isolated and portable environments.

## Data Analysis
Here's the dashboard made on Kibana


Pipeline Flow in Dockerized Environment:
1. Data Source → Ingestion into PostgreSQL.
2. Data transformation in PostgreSQL via Airflow.
3. Data validation using Expectations
4. Processed data exported to Elasticsearch.
5. Data visualized in Kibana.

![plot   insight 01](https://github.com/user-attachments/assets/aa24466b-91ad-4a6e-9f32-9c725f048fda)

![plot   insight 02](https://github.com/user-attachments/assets/220d99f5-c835-4417-918e-d3570d45de66)

![plot   insight 03](https://github.com/user-attachments/assets/bf805263-ae28-43c2-ac21-9b205aa642da)

![plot   insight 04](https://github.com/user-attachments/assets/608ee36a-1a73-48b6-bfe1-1bf0092fac5e)

![plot   insight 05](https://github.com/user-attachments/assets/40e5c5c6-b3a5-494b-b065-db42a09786be)

![plot   insight 06](https://github.com/user-attachments/assets/36d354f1-21f3-41ff-9b56-9c4e6081dd76)





