"""Public landing content: what the platform does, shown to visitors before they sign in."""
import streamlit as st

PIPELINE = """
digraph {
  rankdir=LR; bgcolor="transparent";
  node [shape=box, style="rounded,filled", fillcolor="#16233A", color="#2DD4BF", fontcolor="#E5ECF6",
        fontname="Helvetica", fontsize=11, penwidth=1.4];
  edge [color="#5EEAD4", penwidth=1.4];
  app [label="Streamlit\\nevents"]; kafka [label="Kafka\\n3 topics"]; airflow [label="Airflow\\n2 DAGs"];
  raw [label="HDFS\\nraw"]; spark [label="Spark\\n8 jobs"]; curated [label="HDFS refined\\n& curated"]; dash [label="Risk\\ndashboards"];
  app -> kafka -> airflow -> raw -> spark -> curated -> dash;
}
"""

TECH = ["Kafka", "HDFS", "Spark (PySpark)", "Airflow", "Docker", "AWS EC2", "Flask", "Streamlit"]

ROLES = """
<table class="ts-roles">
  <thead><tr><th>Role</th><th>Password</th><th>Access</th></tr></thead>
  <tbody>
    <tr><td>Trust &amp; Safety Analyst</td><td><code>ts_admin</code></td><td>Full</td></tr>
    <tr class="ts-does"><td colspan="3">Investigates risky sellers, checks data quality, uses the demo tools.</td></tr>
    <tr><td>Business Analyst</td><td><code>biz_user</code></td><td>Read-only</td></tr>
    <tr class="ts-does"><td colspan="3">Sees seller names, regions and risk levels only.</td></tr>
  </tbody>
</table>
"""


def about_section():
    st.subheader("About this project")
    st.markdown("**A marketplace has thousands of sellers. Which ones should the Trust & Safety team review first?**")
    st.write("This platform answers that question with a real big data pipeline. It streams orders, returns and "
             "complaints, cleans them, scores every seller for fraud risk from 0 to 100, and ranks the riskiest "
             "sellers first, without exposing customers' personal data.")

    st.markdown("##### How the data flows")
    st.graphviz_chart(PIPELINE)

    c1, c2, c3 = st.columns(3)
    c1.metric("Spark jobs per run", 8)
    c2.metric("Data-quality checks", 20)
    c3.metric("HDFS data layers", 3)

    st.markdown("##### Demo tools")
    st.markdown(
        "- **Event Producer** plays the marketplace. In production, events come from the marketplace itself.\n"
        "- **Pipeline Monitor** and **Run Pipeline** are the engineer's view. In production, Airflow runs it daily."
    )

    st.markdown("##### Built with")
    st.html('<div class="ts-chips">' + "".join(f"<span>{name}</span>" for name in TECH) + "</div>")


def demo_logins():
    """One table under the sign-in form: role, password, access, and what each role does."""
    with st.container(border=True):
        st.markdown("**Demo logins**")
        st.html(ROLES)
