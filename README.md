# Rental Data Scraper and Visualization Pipeline

This project aims to develop a robust and efficient rental data scraper and visualization pipeline specifically designed for the Athens region. The pipeline incorporates a range of containerized services, including MongoDB for data storage, Airflow for task scheduling and notifications, Metabase for data visualization, and Selenium Remote Chromedriver for web scraping.

## Process Overview

The rental data scraper and visualization pipeline follows the following steps:

### Web Scraping:

- The web scraper retrieves rental property data from multiple sources across the Athens region.
- Utilizing Selenium Remote Chromedriver, the scraper automates web interactions to extract relevant property details.
- Key information, including property characteristics, web IDs, and creation dates, is collected for comprehensive analysis.
- Both properties with images and those without are included in the data collection process.

### Data Storage and Management:

- The scraped rental data is stored in MongoDB, a powerful NoSQL database designed for scalability and flexibility.
- MongoDB facilitates efficient organization and retrieval of data through collections and documents.
- Its robust storage capabilities ensure reliable management of large volumes of rental data.

### Airflow Task Scheduling and Notifications:

- Airflow, a reliable workflow management platform, schedules and orchestrates tasks within the pipeline.
- Task execution is efficiently managed to ensure timely and accurate data scraping and transfer processes.
- Airflow also provides notifications and alerts, enabling real-time monitoring of data transfer status.

### Data Transfer and Transformation:

- The scraped rental data is transferred from the web scraper to the MongoDB database.
- To facilitate seamless integration, the data undergoes appropriate transformations to match the database schema.
- The transfer process ensures the integrity and consistency of the rental data during insertion.

### Data Visualization with Metabase:

- Metabase, a powerful business intelligence and data visualization tool, empowers users to explore and analyze rental data.
- Interactive dashboards and visualizations are generated, providing a clear and intuitive representation of rental market trends.
- Metabase enables users to generate insightful reports, make data-driven decisions, and monitor key rental market indicators.

## DAG Flows

### DAG Flow 1: Main
![Screenshot_2](https://github.com/ClementineM12/Scraper_of_rentals_/assets/106354411/c84f89ab-2fb2-4d5b-a051-7aee4449c12f)

### DAG Flow 2: Dynamic
![Screenshot_1](https://github.com/ClementineM12/Scraper_of_rentals_/assets/106354411/fdd54450-48a5-40ee-86a5-518ca6e90c85)

## Slack Notifications

Integration with Slack allows real-time notifications and alerts about the pipeline's status. 
![Screenshot_3](https://github.com/ClementineM12/Scraper_of_rentals_/assets/106354411/44794d68-7e25-480b-8765-9a2c5da40060)

### Example of a BSON (Binary JSON) document saved in MongoDB

![Screenshot_4](https://github.com/ClementineM12/Scraper_of_rentals_/assets/106354411/f40187eb-bb2e-4db6-bdbe-d2115e320181)
