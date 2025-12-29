import os
import streamlit as st

from llama_index.llms.openai import OpenAI
from llama_index.core import StorageContext, load_index_from_storage, SimpleDirectoryReader, TreeIndex
from llama_index.core.settings import Settings  # NEW

PERSIST_DIR = "./kb"
DOCS_DIR = "./spool-empty"

def getQueryEngine():
    # 1) LLM
    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    Settings.llm = client  # replaces ServiceContext

    # 2) Build index only if not already persisted
    if not os.path.exists(PERSIST_DIR) or not os.listdir(PERSIST_DIR):
        reader = SimpleDirectoryReader(DOCS_DIR)
        documents = reader.load_data()

        index = TreeIndex.from_documents(documents)  # uses Settings.llm
        index.storage_context.persist(persist_dir=PERSIST_DIR)

    # 3) Load index + return query engine
    storage_context = StorageContext.from_defaults(persist_dir=PERSIST_DIR)
    index = load_index_from_storage(storage_context)
    return index.as_query_engine()


st.title("ChatGPT Agent for Custom Content")

if "query_engine" not in st.session_state:
    st.session_state.query_engine = getQueryEngine()

if "messages" not in st.session_state:
    st.session_state.messages = [{
        "role": "system",
        "content": (
            "Your purpose is to answer questions about specific documents only. "
            "Please answer the user's questions based on what you know about the document. "
            "If the question is outside the scope of the document, please politely decline. "
            "If you don't know the answer, say `I don't know`."
        )
    }]

for message in st.session_state.messages:
    if message["role"] in ["user", "assistant"]:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

if prompt := st.chat_input():
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        response = st.session_state.query_engine.query(prompt)

        # response can be a Response object; render as string safely
        response_text = str(response)
        st.markdown(response_text)

        st.session_state.messages.append({"role": "assistant", "content": response_text})
