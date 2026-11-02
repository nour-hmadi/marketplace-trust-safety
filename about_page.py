"""Public landing content: what the platform does, shown to visitors before they sign in."""
import streamlit as st

PIPELINE = """
digraph {
  rankdir=LR; bgcolor="transparent";
  node [shape=box, style="rounded,filled", fillcolor="#E8EEF7", color="#5B6C8F", fontname="Helvetica", fontsize=11];
  edge [color="#8A94A6"];
  app [label="Streamlit\\nevents"]; kafka [label="Kafka\\n3 topics"]; airflow [label="Airflow\\n2 DAGs"];
  raw [label="HDFS\\nraw"]; spark [label="Spark\\n8 jobs"]; curated [label="HDFS refined\\n& curated"]; dash [label="Risk\\ndashboards"];
  app -> kafka -> airflow -> raw -> spark -> curated -> dash;
}
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

    st.markdown("##### Try it")
    st.markdown(
        "| Role | Password | What you can do |\n"
        "|---|---|---|\n"
        "| Trust & Safety Analyst | `ts_admin` | Send events, run the pipeline, investigate sellers, view governance |\n"
        "| Business Analyst | `biz_user` | Read-only view: seller names, regions and risk levels |"
    )

    st.markdown("##### Built with")
    st.write("Kafka · HDFS · Spark (PySpark) · Airflow · Docker · AWS EC2 · Flask · Streamlit")
    st.caption("Built by **Nour Hmadi** · [GitHub repo](https://github.com/nour-hmadi/marketplace-trust-safety)"
               " · [LinkedIn](https://www.linkedin.com/in/nour-hmadi-b86a29438)")
