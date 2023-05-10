import streamlit as st
import plotly.express as px
import pandas as pd
import json

from sqlalchemy import create_engine, text
import pandas as pd
import textwrap

def query_sql_records(query, db_name, host, user, password):
    # Create the engine object
    e = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{db_name}")
    
    # Connect to the database and execute the query
    conn = e.connect()
    result = conn.execute(text(textwrap.dedent(query)))
    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    
    # Close the connection
    conn.close()
    
    # Return the DataFrame
    return df




# Define the first page with a plot
def Aggrigatedpage():

    st.title("Aggrigated View")

    yr = [2018, 2019, 2020, 2021, 2022]
    y = st.selectbox('Select a Year:', yr, key='year_dropdown')

    qt = ['Q1', 'Q2', 'Q3', 'Q4']
    s = st.selectbox('Select a quarter:', qt, key='quarter_dropdown') 

    # Disable the Apply button by default
    button_disabled = True

    # Enable the Apply button only when both dropdowns have been selected
    if y and s:
        button_disabled = False

    # Apply button that runs the query when clicked
    if not button_disabled and st.button('Apply'):       

        query = f"""       

            SELECT T_type,T_count,Amount, Quater, Year
            FROM agg_data
            WHERE Year = {y} and Quater = '{s}'
            
            ORDER BY Amount DESC;
        """ 

        query1 = "SELECT  * FROM aggdata"

        #def query_sql_records(query, db_name, host, user, password):

        df = query_sql_records(query, "phone_pe_db", "localhost", "root", "password")
        st.write(df)
        #print(df)

        # Add a dropdown for filtering the data to plot  
        option = st.multiselect("Select Your Filter for bar graph", df["T_type"].unique().tolist())
        
        fltdf = df[df["T_type"].isin(option)] if option else df
        #-------------------------------------------------- 

        st.title(f"Bar plot of T_type vs Amount from {s} Add filer sample")

        fig = px.bar(fltdf, x='T_type', y='Amount', color='T_type')
        st.plotly_chart(fig)



# Define the second page with a plot
def Overallpieviewpage():
      
    st.title("Pie View")
    yr = [2018, 2019, 2020]
    y = st.selectbox('Select a Year:', yr, key='year_dropdown')

    qt = ['Q1', 'Q2', 'Q3', 'Q4']
    s = st.selectbox('Select a quarter:', qt, key='quarter_dropdown') 

    # Disable the Apply button by default
    button_disabled = True

    # Enable the Apply button only when both dropdowns have been selected
    if y and s:
        button_disabled = False

    # Apply button that runs the query when clicked
    if not button_disabled and st.button('Apply'):
    
    
        query = f"""       

            SELECT Brand,Reg_user,User_Count, Quater, Year
            FROM pie_data
            WHERE Year = {y} and Quater = '{s}'
            
        """ 


        query1 = "SELECT  * FROM aggdata"

        #def query_sql_records(query, db_name, host, user, password):

        df = query_sql_records(query, "phone_pe_db", "localhost", "root", "password")
        st.write(df)
        st.title(f"Overall User data for {y} and {s}")
    
        # group the data by Brand and Reg_user
        grouped_data = df.groupby(['Brand', 'Reg_user'])['User_Count'].sum().reset_index()

        # create the pie chart
        fig = px.pie(grouped_data, values='User_Count', names='Brand', 
                    title='Brand distribution of PhonePe Users', hole=0.5)

        # display the chart
        st.plotly_chart(fig)
        
            

# Define the third page with a plot
def IndiaMapview():
    st.title("Map View")
    yr = [2018, 2019, 2020, 2021, 2022]
    y = st.selectbox('Select a Year:', yr, key='year_dropdown')

    qt = ['Q1', 'Q2', 'Q3', 'Q4']
    s = st.selectbox('Select a quarter:', qt, key='quarter_dropdown') 

    # Disable the Apply button by default
    button_disabled = True

    # Enable the Apply button only when both dropdowns have been selected
    if y and s:
        button_disabled = False

    # Apply button that runs the query when clicked
    if not button_disabled and st.button('Apply'):
        
        query =  f"""
            SELECT State, Amount, Amt_color, Quater, Year
            FROM map_data
            WHERE Year = {y} and Quater = '{s}'
            """
    
        dfmap = query_sql_records(query, "phone_pe_db", "localhost", "root", "password")
        
        st.write(dfmap.head())
        

        geojson_file_path= r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\geojson\states_india.geojson'

        plot_choropleth(dfmap, geojson_file_path,y,s)


        

    


def plot_choropleth(merged_df, geojson_file_path,y,s):

    india_states = json.load(open(geojson_file_path, "r"))

    state_id_map = {}
    for feature in india_states["features"]:
        feature["id"] = feature["properties"]["state_code"]
        state_id_map[feature["properties"]["st_nm"]] = feature["id"]
    
    merged_df["ID_x"] = merged_df["State"].map(state_id_map)
    import numpy as np
    #merged_df["Amt_color"]  = np.log10(merged_df["Amount"])

    fig = px.choropleth_mapbox(
        
        merged_df,
        locations="ID_x",
        geojson=india_states,
        color="Amount", 
        title=f"Map View for {y} and {s}",       
        hover_name="State",
        hover_data=["Amount"],
        
        mapbox_style="carto-positron",
        center={"lat": 24, "lon": 78},
        zoom=3,
        opacity=0.5,
        
    )

    fig.update_geos(fitbounds="locations", visible=False)
    fig.show()



# Define the Streamlit app
def main():

    import streamlit as st

    st.markdown("<h1 style='text-align: center; font-size: 55px; font-weight: bold;'>Phonepe Pluse repo With MYSQL Dashboard</h1>", unsafe_allow_html=True)


    # Define the navigation dropdown menu
    pages = {
        "Aggrigated view": Aggrigatedpage,        
        "Overall pie view": Overallpieviewpage,
        "India Map view": IndiaMapview
    }

    option = st.sidebar.selectbox("Select a page view", list(pages.keys()))
    pages[option]()

if __name__ == '__main__':
    main()
