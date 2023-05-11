# phonepe_user_dashboard

#README for Demo_etl.py file:
This code is a Python script that extracts data from JSON files and combines them into pandas dataframes. It has six main functions:

#clone_repo_once(repo_url, dest_folder)

This function clones a git repository from the given repo_url to the given dest_folder if the folder does not exist yet. If it does exist, it prints a message saying it is skipping the clone.

#extract_data_from_json_agg_bar(path, year, quater)

This function extracts data from a JSON file containing transaction data and returns a pandas dataframe with columns 'T_type', 'Amount', 'T_Count', 'Year', 'Quater', and 'chart_type'. It takes the path to the JSON file, the year, and the quarter as arguments.

#extract_data_from_json_agg_pie(path, year, quater)

This function extracts data from a JSON file containing user data and returns a pandas dataframe with columns 'Reg_user', 'Brand', 'User_Count', 'Percentage', 'Year', 'Quater', and 'chart_type'. It takes the path to the JSON file, the year, and the quarter as arguments.

#extract_data_from_json_map(json_file, year, quater)

This function extracts data from a JSON file containing map data and returns a pandas dataframe with columns 'State', 'Amount', 'Amt_color', 'Year', and 'Quater'. It takes the path to the JSON file, the year, and the quarter as arguments.

#combine_dataframes(df1, df2, df3, df4)

This function takes four dataframes as arguments and combines them into a single dataframe using pandas' concat function.


#load_df_to_mysql(df, db_name, table_name, host, user, password)

This function takes a Pandas data frame, the name of the database, table name, database host, username, and password. It creates the database if it doesn't exist, drops the table if it exists, creates a table with the same table name and columns as the data frame, and inserts the data into the table.

#load_df_to_mysql_map(df, db_name, table_name, host, user, password)

This function is similar to load_df_to_mysql but creates a specific schema for a chloropleth map data frame. It takes the same parameters as load_df_to_mysql.

#Driver code part

This code is a Python script that calls the above functions that  extracts data from JSON files and combines them into pandas dataframes and, loads the combined data into MYSQL. 

#-----------------------------------------------------------------------------------------------------------------------#

# README FOR app_query_finil.py fie (SQL Dashboard using Streamlit)

This is a simple SQL dashboard that visualizes data fetched from a MySQL database using Streamlit and Plotly Express.

#Dependencies

•	Streamlit
•	Plotly
•	Pandas
•	SQLAlchemy
•	mysql-connector-python

#Usage

The Data  extracted using demo_etl.py file and stored in MySQL database , the data required for the dashboard is queried from the database and used to plot.
The app consists of three pages, each with a different visualization of the data.

#Aggregated View

This page displays a bar plot of transaction type vs transaction amount for a selected year and quarter. The user can select a filter to display a subset of the data.

#Overall Pie View

This page displays a pie chart of the distribution of PhonePe users by brand for a selected year and quarter.

#India Map View

This page displays a choropleth map of India using geojson file with the transaction amount for each state for a selected year and quarter.








