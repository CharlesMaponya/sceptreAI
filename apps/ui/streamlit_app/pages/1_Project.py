from __future__ import annotations

import streamlit as st

st.set_page_config(page_title="Project Workspace", layout="wide")

from streamlit_app.workspace import render_project_detail, render_session_sidebar

render_session_sidebar()

if "access_token" not in st.session_state:
    st.switch_page("app.py")
    st.stop()

project_id = st.session_state.get("selected_project_id")
if not project_id:
    st.switch_page("app.py")
    st.stop()

if st.button("Back to projects"):
    st.switch_page("app.py")

render_project_detail(str(project_id), st.session_state["access_token"])
