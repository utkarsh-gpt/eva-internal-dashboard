import pymongo
import streamlit as st

@st.cache_resource
def init_connection():
    # connection_string = "mongodb+srv://evaUtkarsh:WiNE8sOH5slpkmAS@cluster0.fyr3c.mongodb.net/RxmDatabase?retryWrites=true&w=majority"
    return pymongo.MongoClient(st.secrets["mongo"]["evabot_string"])

client = init_connection()
db = client.RapportDashboard