import streamlit as st
import pandas as pd
import math
from utils import find_and_generate_keys, extract_nested
from database import db
import query
import inspect

limit = 25
skip = 1

# Collects functinos in query.py and writes their paramaters as input fields on the web app

query_functions = [name for name, obj in inspect.getmembers(query) if inspect.isfunction(obj) and obj.__module__ == query.__name__]
query_type = st.selectbox("Query Type:", options=query_functions, index=1)

    
selected_function = getattr(query, query_type)


params = inspect.signature(selected_function).parameters
input_values = {}
input_menu = st.columns(len(params.items()))
for i, param in enumerate(params.items()):
    if param[0] == 'self':
        continue
    default_value = param[1].default if param[1].default is not param[1].empty else ""

    with input_menu[i]:
        if param[1].annotation == int:
            input_values[param[0]] = st.number_input(f"Enter {param[0]}:", value=0, step=1)
        elif param[1].annotation == list:
            all_keys = find_and_generate_keys(db[input_values["collection_name"]], runtime=3) # Currently hard-coded to generate a multibox selection for all collections
            input_values[param[0]] = st.multiselect(f"Select {param[0]}:", all_keys)
        else:
            input_values[param[0]] = st.text_input(f"Enter {param[0]}:",value=str(default_value))
        
# Runs the chosen query functions with the input values 

try:
    data = selected_function(**input_values)
    st.write("Result:")

    # Check whether output is a dictionary (multiple dataframe outputs)
    if isinstance(data, dict):
        for item in data:
            st.write(item)
            st.dataframe(data[item], hide_index=True)
            if len(data[item]) > 0:
                with st.expander("View Nested Objects"): # Currently hard-coded and assumes output has single row
                    extract_nested(data[item].iloc[0])

    # Check whether output is a single dataframe
    elif isinstance(data, pd.DataFrame):
        selected_row = st.dataframe(
            data,
            hide_index=True,
            key="data",
            on_select="rerun",
            selection_mode='single-row',
        )
        with st.expander("View Nested Objects"):
            try:
                extract_nested(data.iloc[selected_row["selection"]["rows"][0]])
            except:
                st.write("Select a row to see its nested objects")
except Exception as e:
    st.write("Please fill all the fields correctly.")
    

    