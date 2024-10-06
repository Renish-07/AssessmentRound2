import requests
import streamlit as st
import pandas as pd

def fetch_solr_data(solr_url, query):
    params = {
        'q': query,  
        'wt': 'json',  
        'rows': 1262   
    }
    response = requests.get(solr_url, params=params)
    if response.status_code == 200:
        data = response.json()
        return data['response']['docs']
    else:
        st.error(f"Error: Unable to fetch data from Solr (Status Code: {response.status_code})")
        return []


st.title("Apache Solr Search Results")


solr_url = "http://localhost:8983/solr/EmployeeCSV/select"
st.markdown("<h1 style='text-align: left;'>Enter Your Search</h1>", unsafe_allow_html=True)

categories = ["Employee_ID","Department", "Country","City","Gender","Search Dynamically"]
category = st.selectbox("Choose a category:", categories)


if category=="Employee_ID":
    item= st.text_input("Enter the Employee ID")
elif category == "Department":
    items = ['Accounting','Engineering','Finance','Human Resources','IT','Marketing','Sales']
elif category == "Country":
    items = ["United States", "Brazil", "China"]
elif category == "Gender":
    items=["Male","Female"]
elif category == "City":
    items=["Austin","Chicago","Columbus","Miami","Phoenix","Seattle","Beijing","Chengdu","Chongqing","Shanghai","Manaus","Rio de Janeiro ","Sao Paulo"]
elif category=="Search Dynamically":
    category=st.text_input("Enter the category")
    item=st.text_input("Enter the value")


if(category=="Department" or category=="Country" or category=="Gender" or category=="City"):
    item = st.selectbox("Choose an item:", items)
query=category+":"+item

if st.button("Search"):
    results = fetch_solr_data(solr_url,query)
    
    if results:
       
        df = pd.DataFrame(results)
        st.write("Search Results:")
        st.dataframe(df)  
    else:
        st.write("No results found")


