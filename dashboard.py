"""
dashboard.py
============
Plotly Dash live monitoring dashboard for Reactor R1.

Reads prediction results from S3 Gold and displays:
    - 5 metric cards (temperature, pH, viscosity, error, rows)
    - 10-minute viscosity prediction banner with alert
    - Temperature over time chart
    - pH level over time chart
    - Viscosity actual vs predicted chart
    - Prediction error distribution histogram
    - Feature importance horizontal bar chart

Auto-refreshes every 60 seconds.

Usage:
    # Run in foreground
    python3 dashboard.py

    # Run in background (recommended)
    nohup python3 ~/dashboard.py > ~/dashboard.log 2>&1 &

Access:
    http://[EC2-IP]:8050

Author: James Daramola
Date:   May 2026
"""

import boto3
import io
import pandas as pd
import plotly.graph_objects as go
import dash
from dash import dcc, html, Input, Output

# ── Configuration ─────────────────────────────────────────────────
BUCKET    = "reactor-project-emperor1"
PRED_PATH = "gold/predictions/"

# Acceptable viscosity range — adjust to match plant specification
VISC_MIN  = 800
VISC_MAX  = 900


def load_data():
    """
    Load predictions from S3 Gold.
    Returns the latest 5,000 rows sorted by timestamp.
    """
    s3  = boto3.client("s3")
    obj = s3.list_objects_v2(Bucket=BUCKET, Prefix=PRED_PATH)
    dfs = []
    for item in obj.get("Contents", []):
        if item["Key"].endswith(".parquet"):
            body = s3.get_object(
                Bucket=BUCKET, Key=item["Key"])["Body"].read()
            dfs.append(pd.read_parquet(io.BytesIO(body)))

    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs).sort_values("timestamp").tail(5000)
    df["error"] = abs(df["prediction"] - df["viscosity_target_10min"])
    return df


# ── Feature importance from trained model ─────────────────────────
FEATURES = ["ph_level", "ph_rolling_avg", "temperature_c", "batch_time",
            "temp_rate_change", "ph_rate_change", "batch_progress",
            "visc_lag_1"]
IMPORTANCES = [0.2347, 0.1963, 0.1911, 0.1456,
               0.0976, 0.0732, 0.0512, 0.0103]


# ── App layout ────────────────────────────────────────────────────
app = dash.Dash(__name__)

app.layout = html.Div([

    # Header
    html.Div([
        html.H1("Reactor R1 — Monitoring Dashboard",
                style={"margin": "0", "color": "#1B3A5C"}),
        html.P("Real-time viscosity prediction | Resin Manufacturing Plant",
               style={"margin": "0", "color": "#7F8C8D", "fontSize": "13px"}),
        html.Span("Pipeline Active",
                  style={"float": "right", "marginTop": "-40px",
                         "border": "1px solid #1E8449", "padding": "4px 12px",
                         "borderRadius": "20px", "color": "#1E8449",
                         "fontSize": "12px"})
    ], style={"padding": "16px 24px", "borderBottom": "1px solid #EEEEEE"}),

    # Metric cards
    html.Div(id="metric-cards",
             style={"display": "flex", "gap": "16px",
                    "padding": "16px 24px"}),

    # Prediction banner
    html.Div(id="prediction-banner",
             style={"margin": "0 24px 16px", "padding": "16px 24px",
                    "border": "1px solid #DDDDDD", "borderRadius": "8px"}),

    # Charts row 1
    html.Div([
        dcc.Graph(id="temp-chart",  style={"flex": "1"}),
        dcc.Graph(id="ph-chart",    style={"flex": "1"}),
    ], style={"display": "flex", "gap": "16px", "padding": "0 24px 16px"}),

    # Charts row 2
    html.Div([
        dcc.Graph(id="visc-chart",  style={"flex": "2"}),
        dcc.Graph(id="error-chart", style={"flex": "1"}),
    ], style={"display": "flex", "gap": "16px", "padding": "0 24px 16px"}),

    # Feature importance
    dcc.Graph(id="importance-chart",
              style={"margin": "0 24px 24px"}),

    # Auto-refresh every 60 seconds
    dcc.Interval(id="interval", interval=60_000, n_intervals=0),

], style={"fontFamily": "Arial, sans-serif", "backgroundColor": "#FAFAFA"})


# ── Callback — updates all charts every 60 seconds ────────────────
@app.callback(
    [Output("metric-cards",      "children"),
     Output("prediction-banner", "children"),
     Output("temp-chart",        "figure"),
     Output("ph-chart",          "figure"),
     Output("visc-chart",        "figure"),
     Output("error-chart",       "figure"),
     Output("importance-chart",  "figure")],
    Input("interval", "n_intervals")
)
def update(n):
    df = load_data()
    if df.empty:
        empty = html.P("No data yet — run the pipeline first.")
        empty_fig = go.Figure()
        return [empty], empty, empty_fig, empty_fig, empty_fig, empty_fig, empty_fig

    latest     = df.iloc[-1]
    prediction = round(float(latest["prediction"]), 1)
    avg_error  = round(float(df["error"].mean()), 2)
    n_batches  = df["batch_id"].nunique() if "batch_id" in df.columns else "—"

    # ── Metric cards ─────────────────────────────────────────────
    card_style = {
        "flex": "1", "padding": "16px", "backgroundColor": "white",
        "borderRadius": "8px", "border": "1px solid #EEEEEE",
        "boxShadow": "0 1px 3px rgba(0,0,0,0.06)"
    }
    cards = html.Div([
        html.Div([html.P("Temperature",    style={"color": "#7F8C8D", "fontSize":"12px", "margin":"0"}),
                  html.H3(f"{latest['temperature_c']:.1f} °C", style={"color":"#E67E22","margin":"4px 0 0"}),
                  html.P("current reading",style={"color":"#AAAAAA","fontSize":"11px","margin":"0"})], style=card_style),
        html.Div([html.P("pH Level",       style={"color": "#7F8C8D", "fontSize":"12px", "margin":"0"}),
                  html.H3(f"{latest['ph_level']:.2f}",         style={"color":"#2471A3","margin":"4px 0 0"}),
                  html.P("current reading",style={"color":"#AAAAAA","fontSize":"11px","margin":"0"})], style=card_style),
        html.Div([html.P("Viscosity Now",  style={"color": "#7F8C8D", "fontSize":"12px", "margin":"0"}),
                  html.H3(f"{latest['viscosity_target_10min']:.1f} cP", style={"color":"#1B3A5C","margin":"4px 0 0"}),
                  html.P("current reading",style={"color":"#AAAAAA","fontSize":"11px","margin":"0"})], style=card_style),
        html.Div([html.P("Avg Error",      style={"color": "#7F8C8D", "fontSize":"12px", "margin":"0"}),
                  html.H3(f"{avg_error} cP", style={"color": "#1E8449" if avg_error < 50 else "#E74C3C","margin":"4px 0 0"}),
                  html.P("prediction error",style={"color":"#AAAAAA","fontSize":"11px","margin":"0"})], style=card_style),
        html.Div([html.P("Rows Processed", style={"color": "#7F8C8D", "fontSize":"12px", "margin":"0"}),
                  html.H3(f"{len(df):,}",  style={"color":"#1B3A5C","margin":"4px 0 0"}),
                  html.P("in dataset",     style={"color":"#AAAAAA","fontSize":"11px","margin":"0"})], style=card_style),
    ], style={"display":"flex","gap":"16px"})

    # ── Prediction banner ─────────────────────────────────────────
    in_range = VISC_MIN <= prediction <= VISC_MAX
    banner = html.Div([
        html.P("10-minute viscosity prediction",
               style={"margin":"0","fontSize":"13px","color":"#555555"}),
        html.H2(f"{prediction} cP",
                style={"margin":"4px 0","fontSize":"36px","color":"#1B3A5C"}),
        html.P("Within acceptable range" if in_range else "WARNING: Outside range!",
               style={"margin":"0","fontWeight":"bold",
                      "color":"#1E8449" if in_range else "#E74C3C"}),
        html.Div([
            html.Span(f"R²  {0.9978}", style={"marginRight":"24px","fontWeight":"bold","color":"#1E8449"}),
            html.Span(f"RMSE  23.02 cP", style={"marginRight":"24px","color":"#555555"}),
            html.Span(f"Batches  {n_batches}", style={"color":"#555555"}),
        ], style={"marginTop":"8px","float":"right","fontSize":"13px"}),
    ])

    # ── Temperature chart ─────────────────────────────────────────
    temp_fig = go.Figure(go.Scatter(
        x=df["timestamp"], y=df["temperature_c"],
        fill="tozeroy", fillcolor="rgba(230,126,34,0.15)",
        line=dict(color="#E67E22", width=1.5), name="Temperature"
    ))
    temp_fig.update_layout(
        title="Temperature (°C)", margin=dict(l=40,r=20,t=40,b=40),
        plot_bgcolor="white", paper_bgcolor="white",
        xaxis=dict(showgrid=True, gridcolor="#F0F0F0"),
        yaxis=dict(showgrid=True, gridcolor="#F0F0F0")
    )

    # ── pH chart ──────────────────────────────────────────────────
    ph_fig = go.Figure(go.Scatter(
        x=df["timestamp"], y=df["ph_level"],
        fill="tozeroy", fillcolor="rgba(36,113,163,0.1)",
        line=dict(color="#2471A3", width=1.5), name="pH"
    ))
    ph_fig.update_layout(
        title="pH Level", margin=dict(l=40,r=20,t=40,b=40),
        plot_bgcolor="white", paper_bgcolor="white",
        xaxis=dict(showgrid=True, gridcolor="#F0F0F0"),
        yaxis=dict(showgrid=True, gridcolor="#F0F0F0")
    )

    # ── Viscosity actual vs predicted ─────────────────────────────
    visc_fig = go.Figure([
        go.Scatter(x=df["timestamp"], y=df["viscosity_target_10min"],
                   name="Actual", line=dict(color="#1B3A5C", width=1.5)),
        go.Scatter(x=df["timestamp"], y=df["prediction"],
                   name="Predicted",
                   line=dict(color="#E67E22", width=1.5, dash="dash")),
    ])
    visc_fig.update_layout(
        title="Viscosity — actual vs predicted (cP)",
        margin=dict(l=40,r=20,t=40,b=40),
        plot_bgcolor="white", paper_bgcolor="white",
        legend=dict(orientation="h", y=1.1),
        xaxis=dict(showgrid=True, gridcolor="#F0F0F0"),
        yaxis=dict(showgrid=True, gridcolor="#F0F0F0")
    )

    # ── Error histogram ───────────────────────────────────────────
    err_fig = go.Figure(go.Histogram(
        x=df["error"], nbinsx=20,
        marker_color="#2471A3", name="Error"
    ))
    err_fig.update_layout(
        title="Prediction error distribution (cP)",
        xaxis_title="Error (cP)", yaxis_title="Count",
        margin=dict(l=40,r=20,t=40,b=40),
        plot_bgcolor="white", paper_bgcolor="white"
    )

    # ── Feature importance ────────────────────────────────────────
    colours = ["#2471A3" if i < 3 else "#E67E22" if i < 6 else "#7F8C8D"
               for i in range(len(FEATURES))]
    imp_fig = go.Figure(go.Bar(
        x=IMPORTANCES, y=FEATURES,
        orientation="h", marker_color=colours
    ))
    imp_fig.update_layout(
        title="Feature importance — what drives viscosity",
        xaxis_tickformat=".0%",
        margin=dict(l=160,r=40,t=40,b=40),
        plot_bgcolor="white", paper_bgcolor="white",
        xaxis=dict(showgrid=True, gridcolor="#F0F0F0")
    )

    return (cards, banner, temp_fig, ph_fig,
            visc_fig, err_fig, imp_fig)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
