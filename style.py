"""The app's look ("Midnight Teal"): page header with an animated light, cards and labels.

Colors and the font are set in .streamlit/config.toml; this file adds what theme options can't do.
"""
import streamlit as st

CSS = """
/* header bar: transparent, so the page starts near the top */
[data-testid='stHeader'] { background: transparent; }
[data-testid='stAppDeployButton'] { display: none; }
[data-testid='stMainBlockContainer'] { padding-top: 1.5rem; padding-bottom: 1.5rem; }
[data-testid='stMain'] [data-testid='stVerticalBlock'] { gap: 1.2rem; }

/* page header: gradient banner, and a sharp slanted light that strikes across once (like lightning),
   then rests on the right and glows softly */
.ts-hero { position: relative; overflow: hidden; isolation: isolate; display: flex; align-items: center; gap: 20px;
           background: linear-gradient(120deg, #115E59 0%, #13324A 55%, #111B2E 100%);
           border: 1px solid #1F2C44; border-radius: 18px; padding: 22px 30px; }
.ts-hero::before { content: ""; position: absolute; top: -60%; height: 220%; left: 78%; width: 3px; z-index: -1;
                   pointer-events: none; transform: rotate(20deg);
                   background: linear-gradient(to bottom, transparent, #E6FFFB 30%, #5EEAD4 70%, transparent);
                   box-shadow: 0 0 10px 2px rgba(94,234,212,.8), 0 0 46px 14px rgba(45,212,191,.28);
                   animation: ts-strike .4s cubic-bezier(.3,.6,.2,1) both,
                              ts-breathe 4s ease-in-out .4s infinite alternate; }
.ts-hero::after { content: ""; position: absolute; inset: 0; z-index: -1; pointer-events: none; opacity: 0;
                  background: linear-gradient(110deg, transparent 20%, rgba(230,255,251,.22) 50%, transparent 80%);
                  animation: ts-flash .45s ease-out both; }
@keyframes ts-strike { 0% { left: -6%; opacity: 0; } 6% { opacity: 1; } 100% { left: 78%; opacity: 1; } }
@keyframes ts-flash { 0% { opacity: 0; } 8% { opacity: 1; } 22% { opacity: .1; } 32% { opacity: .6; } 100% { opacity: 0; } }
@keyframes ts-breathe { from { opacity: 1; } to { opacity: .5; } }
@media (prefers-reduced-motion: reduce) { .ts-hero::before, .ts-hero::after { animation: none; } }
.ts-badge { flex: none; width: 56px; height: 56px; border-radius: 16px; display: grid; place-items: center;
            background: url("data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAyNCAyNCIgZmlsbD0ibm9uZSIgc3Ryb2tlPSIjNUVFQUQ0IiBzdHJva2Utd2lkdGg9IjEuNiIgc3Ryb2tlLWxpbmVjYXA9InJvdW5kIiBzdHJva2UtbGluZWpvaW49InJvdW5kIj48cGF0aCBkPSJNMTIgMi44IDQuNSA1LjZ2NS42YzAgNC42IDMuMSA4LjYgNy41IDEwIDQuNC0xLjQgNy41LTUuNCA3LjUtMTBWNS42eiIvPjxwYXRoIGQ9Ik03IDEyLjRoMi40bDEuNS0zLjIgMi4yIDYuMiAxLjYtM2gyLjMiLz48L3N2Zz4=") center / 32px no-repeat, rgba(255,255,255,.08);
            border: 1px solid rgba(255,255,255,.18); }
.ts-kicker { color: #5EEAD4; text-transform: uppercase; letter-spacing: .16em; font-size: .72rem; font-weight: 700; }
.ts-title { color: #FFFFFF; font-size: 2.4rem; font-weight: 700; line-height: 1.1; letter-spacing: -0.02em;
            margin: 4px 0 6px; }
.ts-title span { color: #2DD4BF; }
.ts-sub { color: rgba(255,255,255,.78); font-size: .98rem; }
.ts-author { margin-left: auto; text-align: right; color: rgba(255,255,255,.78); font-size: .85rem; }
.ts-links { display: flex; gap: 8px; justify-content: flex-end; margin-top: 8px; }
.ts-links a { color: #E5ECF6 !important; text-decoration: none; padding: 4px 12px; border-radius: 999px;
              border: 1px solid rgba(255,255,255,.22); background: rgba(255,255,255,.06); font-size: .8rem; }
.ts-links a:hover { border-color: #5EEAD4; color: #5EEAD4 !important; }
@media (max-width: 900px) { .ts-author { display: none; } }

/* section labels (#####): small capitals in teal */
h5 { color: #5EEAD4 !important; text-transform: uppercase; letter-spacing: .08em; font-size: .78rem !important;
     font-weight: 600 !important; padding: 1.2rem 0 .4rem !important; }
h3 { font-weight: 600 !important; letter-spacing: -0.01em; }

/* cards: forms and metric tiles */
[data-testid='stForm'] { background: #111B2E; border: 1px solid #1F2C44 !important; border-radius: 14px; }
[data-testid='stMetric'] { background: #111B2E; border: 1px solid #1F2C44; border-left: 3px solid #5EEAD4;
                           border-radius: 12px; padding: 8px 16px; }
[data-testid='stMetricValue'] { font-size: 1.9rem; }
[data-testid='stMetricLabel'] { color: #94A3B8; }
[data-testid='stBaseButton-primary'] { color: #04201C !important; font-weight: 600; }

/* demo logins table: role, password and access, with a short line under each role */
.ts-roles { width: 100%; border-collapse: separate; border-spacing: 0; border-radius: 10px; overflow: hidden;
            font-size: .92rem; }
.ts-roles th { background: #1F2C44; color: #E5ECF6; font-weight: 700; text-align: left; padding: 10px 14px; }
.ts-roles td { padding: 12px 14px 2px; border-top: 1px solid #1F2C44; color: #E5ECF6; white-space: nowrap; }
.ts-roles tr.ts-does td { border-top: none; padding: 0 14px 12px; color: #94A3B8; font-size: .85rem;
                          white-space: normal; }
.ts-roles tbody tr:first-child td { border-top: none; }
.ts-roles code { color: #5EEAD4; background: rgba(45,212,191,.12); padding: 2px 7px; border-radius: 6px; }

/* medium-height screens: a little tighter */
@media (max-height: 820px) {
  [data-testid='stMainBlockContainer'] { padding-top: 1rem; }
  [data-testid='stMain'] [data-testid='stVerticalBlock'] { gap: 1.05rem; }
  h5 { padding-top: 1rem !important; }
  .ts-hero { padding: 16px 26px; }
  .ts-badge { width: 48px; height: 48px; background-size: 27px; border-radius: 14px; }
  .ts-title { font-size: 2.15rem; }
  [data-testid='stMetricValue'] { font-size: 1.7rem; }
}
/* short screens: compact, so the first page still fits without scrolling */
@media (max-height: 760px) {
  [data-testid='stMain'] [data-testid='stVerticalBlock'] { gap: .85rem; }
  h5 { padding-top: .9rem !important; }
  .ts-hero { padding: 12px 24px; }
  .ts-badge { width: 46px; height: 46px; background-size: 26px; border-radius: 13px; }
  .ts-title { font-size: 2rem; }
  [data-testid='stMetricValue'] { font-size: 1.6rem; }
}

/* "built with" chips */
.ts-chips { display: flex; flex-wrap: wrap; gap: 6px; }
.ts-chips span { border: 1px solid #1F2C44; background: #111B2E; color: #CBD5E1; padding: 3px 11px;
                 border-radius: 999px; font-size: .8rem; }
"""

HERO = """
<div class="ts-hero">
  <div class="ts-badge"></div>
  <div>
    <div class="ts-kicker">Fraud risk analytics · live demo</div>
    <div class="ts-title"><span>Trust &amp; Safety</span> Analytics Platform</div>
    <div class="ts-sub">Big data pipeline on Kafka, Spark and Airflow · deployed on AWS</div>
  </div>
  <div class="ts-author">
    <div>Built by <b>Nour Hmadi</b></div>
    <div class="ts-links">
      <a href="https://github.com/nour-hmadi/marketplace-trust-safety" target="_blank">GitHub repo</a>
      <a href="https://www.linkedin.com/in/nour-hmadi-b86a29438" target="_blank">LinkedIn</a>
    </div>
  </div>
</div>
"""


def apply_style():
    st.html(f"<style>{CSS}</style>")


def hero():
    st.html(HERO)
