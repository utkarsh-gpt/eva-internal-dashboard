import pymongo
import streamlit as st

@st.cache_resource
def init_connection():
    return pymongo.MongoClient(st.secrets["mongo"]["evabot_string"])

client = init_connection()
db = client.RapportDashboard
