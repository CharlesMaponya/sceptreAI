from __future__ import annotations

import streamlit as st

st.set_page_config(page_title="SMME Tabular AutoML", layout="wide")

from streamlit_app.workspace import api_request, render_session_sidebar, set_auth

st.title("SMME Tabular AutoML")
st.caption("Project workspaces, dataset profiling, and model-ready preparation.")
render_session_sidebar()

if "access_token" not in st.session_state:
    sign_in_tab, register_tab = st.tabs(["Sign in", "Register"])

    with sign_in_tab:
        with st.form("login_form"):
            email = st.text_input("Email")
            password = st.text_input("Password", type="password")
            submitted = st.form_submit_button("Sign in", use_container_width=True)
        if submitted:
            status_code, response = api_request(
                "POST",
                "/auth/login",
                payload={"email": email, "password": password},
            )
            if status_code == 200 and isinstance(response, dict):
                set_auth(response)
                st.rerun()
            else:
                st.error(response)

    with register_tab:
        with st.form("register_form"):
            full_name = st.text_input("Full name")
            email = st.text_input("Work email")
            password = st.text_input("Password", type="password")
            submitted = st.form_submit_button("Create account", use_container_width=True)
        if submitted:
            status_code, response = api_request(
                "POST",
                "/auth/register",
                payload={"full_name": full_name, "email": email, "password": password},
            )
            if status_code == 201 and isinstance(response, dict):
                set_auth(response)
                st.rerun()
            else:
                st.error(response)

    st.stop()

access_token = st.session_state["access_token"]
overview_tab, create_tab, invite_tab = st.tabs(
    ["Projects", "Create Project", "Accept Invite"]
)

with overview_tab:
    status_code, response = api_request("GET", "/projects", token=access_token)
    if status_code == 200 and isinstance(response, list):
        if response:
            for project in response:
                name_column, status_column, created_column = st.columns([3, 1, 2])
                with name_column:
                    if st.button(
                        project["name"],
                        key=f"open_project:{project['id']}",
                        use_container_width=True,
                    ):
                        st.session_state["selected_project_id"] = str(project["id"])
                        st.switch_page("pages/1_Project.py")
                status_column.write(project["status"].replace("_", " ").title())
                created_column.caption(project["created_at"])
                if project.get("description"):
                    st.caption(project["description"])
                st.divider()
        else:
            st.info("No projects yet.")
    else:
        st.error(response)

with create_tab:
    with st.form("create_project_form"):
        name = st.text_input("Project name")
        description = st.text_area("Description")
        submitted = st.form_submit_button("Create project", use_container_width=True)
    if submitted:
        status_code, response = api_request(
            "POST",
            "/projects",
            payload={"name": name, "description": description, "settings": {}},
            token=access_token,
        )
        if status_code == 201:
            st.success("Project created.")
            st.rerun()
        else:
            st.error(response)

with invite_tab:
    with st.form("accept_invite_form"):
        invite_token = st.text_input("Invite token")
        submitted = st.form_submit_button("Join project", use_container_width=True)
    if submitted:
        status_code, response = api_request(
            "POST",
            "/projects/share-links/accept",
            payload={"invite_token": invite_token},
            token=access_token,
        )
        if status_code == 200:
            st.success("Project invite accepted.")
            st.rerun()
        else:
            st.error(response)
