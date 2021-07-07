# Table of contents

Table Of Content for this readme.

- [Flaconi Data Engineering Challenge](#flaconi-data-engineering-challenge)
- [Task Solution](#task-solution)
- [Installation](#installation)
- [Project Structure and Usage](#project-structure-and-usage)

# Flaconi Data Engineering Challenge
This challenge involves mainly the following aspects:
- Multidimensional modeling for Data Warehouse
- ETL by using Python

# Task Solution
**Multidimensional  Modeling**:
- Created Python scripts (* also inserted code directly into the Jupyter notebook*) that transforms the given Excel file into tables. Following structures has been constructed following the star-schema:
  - fact_SALES
  - dim_PRODUCT
  - dim_CUSTOMER
  - dim_ORDER

**ETL**:
- Get rid of spaces in column names + make column names uppercase without manually adjusting the column names (i.e. do it programatically, don't manually rename columns)
- **calculate the following KPIs:**
    1. Sum of revenues per month (for the year 2017 - this dataset only includes the year 2017)
    2. The total number of orders per month (the total count)
    3. Average order volume per month
    4. Sum of revenues per customer segment
    5. Top 10 customers, incl. their Lifetime Order Volume (i.e. the lifetime revenue the company made with those customers)

# Installation

  **Requrements**: You need conda 4.9.2 or later to run this project. To use this project, first clone the repo on your device using the command below:
  ```
  git clone https://github.com/rashikcs/Flaconi_Data_Engineering_Challenge.git
  ```
 
  **Installation**: To install required packages and create virtual environment simply run following commands
  ```
  conda env create -f environment.yml
  conda activate flaconi_data_challenge
  ```
  
# Project Structure and Usage
  - **Project Structure**:

        .
        ├── Scripts                                      # Containss all the necessray python scripts
        │   ├── calculate_kpi.py                         # Contains functions to calculate different KPI
        │   ├── DataSchemas.py                           # Class used to define a common API for a set of schema subclasses
        │   ├── ETL.py                                   # Class used to transform and save table according to the provided schema
        │   ├── StarSchema.py                            # Class used to transform and save table according to star-schema
        │   └──utils.py                                  # Containss all the global helper functions
        ├── output                                       # Default Folder to store output
        │   └── Sales                                    # Folder containing output of different schemas 
        │       └── StarSchema                           # Folder containing transformed tables
        ├── Unittest                                     # Folder containing unit test scripts
        ├── Flaconi_Data_Engineering_Challenge.ipynb     # Provided notebook file
        ├── .travis                                      # Automated unit tests using TravisCI
        ├── environments.yml                             
        ├── requirements.txt
        └── README.md

  - **Usage**:
      - #### ETL Class: This class acts as a facade to interact with the other schema classes to transform and load tables. To convert a table to multiple dimensions of star schema one need to provide two variables as shown below.
          ```
          # Usage
          >>> star = ETL('starschema')
          >>> dimension_features_without_dimension_name_substring = {'PRODUCT': ['UNITPRICE'],
                                                                     'CUSTOMER': [],
                                                                     'ORDER':[]}

          >>> fact_table_columns_containing_dimension_name = ['ORDERPRIORITY', 'ORDERQUANTITY']
          
          >>> star.init_params(dataframe_xlsx_path = 'sales.xlsx',
                            xlsx_sheet_name = 'Sales',
                            dimension_features_without_dimension_name_substring = dimension_features_without_dimension_name_substring,
                            fact_table_columns_containing_dimension_name = fact_table_columns_containing_dimension_name)
        
          >>> star.transform_table()
          >>> fact_table, dimension_tables_dict = star.get_transformed_tables()
          >>> merged_df = star.get_merged_table()
                    
      - #### Calculate KPIs: All the methods are in the calculate_kpi.py scripts 
          ```
          # Sample usage
            >>> result = get_revenues_sum(merged_df, variable = 'month')
            >>> result = orders_per_month(merged_df, aggregate_function='sum')
            >>> result = orders_per_month(merged_df, aggregate_function='average order volume')
            >>> result = get_revenues_sum(merged_df, variable = 'customer segment')
            >>> result = get_top_n_customers(merged_df, sort_column = 'life time order volume')
            >>> result = get_top_n_customers(merged_df, sort_column = 'life time revenue')
            
**All the answers to the queries have been implemented in the Flaconi_Data_Engineering_Challenge.ipynb file. Please check the file for more details**