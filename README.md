# Project Title: Data Warehouse & ELT Pipeline for Olist
## Objective
This project involves building a data warehouse for Olist using Slowly Changing Dimensions (SCD), implementing an ELT pipeline with Python and SQL, and orchestrating the process with Luigi. The steps below outline the requirements gathering, SCD implementation, ELT pipeline, orchestration with Luigi, and report creation.

## Key Steps
### Step 1: Requirements Gathering
We conducted a hypothetical meeting with stakeholders to determine their needs for handling historical data and implementing SCD strategies.

#### Sample Questions & Answers:

1. Q: What kind of changes should we save in the data warehouse?<br>
A: Save important changes, like customer information and product information.

2. Q: How should we handle changes to customer information?<br>
A: For customer information, we want to keep a record of important changes of address attributes, because we need to analyze how location impacts customer behavior over time. So, we should save both the old and new information.

3. Q: How should we handle changes to product information? ?<br>
A: For product information, we want to keep a record of important changes of product attributes like product_weight_g, product_length_cm, product_height_cm and product_width_cm. However, for details like product_description_length, we only need to update the information as necessary.

### Step 2: Slowly Changing Dimensions (SCD)
We implemented the following SCD types based on stakeholder input:

- SCD Type 1: Overwrite data without preserving history for non-critical attributes (e.g., product_name_lenght).
- SCD Type 2: Preserve historical data by adding new records for important changes (e.g., product_category_name, product_height_cm ).

### Step 3: ELT with Python & SQL
We designed a pipeline process of SCD using ELT process:
- Extract, Load, and Transform data daily, applying both SCD Type 1 and Type 2.

### Step 4: Orchestrating ELT with Luigi
Luigi is used to manage and orchestrate the ETL pipeline, ensuring smooth execution of extract, load, and transform tasks.
- Daily process at 03.00 AM using cronjob linux

### Technologies Used:
- Python
- Luigi
- PostgreSQL
- Sentry (for logging and error tracking)