"""
Created on Fri Jan  2 16:24:42 2026
@author: tpriyank
"""

import streamlit as st
import pandas as pd
from io import BytesIO

# ---------------------- Merge BBH + Daily ----------------------
def merge_bbh_daily(bbh_file, daily_file):
    bbh = pd.read_excel(bbh_file)
    daily = pd.read_excel(daily_file)

    bbh.columns = bbh.columns.str.strip()
    daily.columns = daily.columns.str.strip()

    # Time handling
    bbh["Period start time"] = pd.to_datetime(bbh.get("Period start time", pd.NaT))
    bbh["Date"] = bbh["Period start time"].dt.date
    bbh["Hour"] = bbh["Period start time"].dt.hour

    daily["Period start time"] = pd.to_datetime(daily.get("Period start time", pd.NaT))
    daily["Date"] = daily["Period start time"].dt.date

    # Clean numeric KPIs in BBH
    id_cols = ["Period start time", "Date", "Hour", "MRBTS name", "LNBTS name", "LNCEL name"]
    kpi_cols = [c for c in bbh.columns if c not in id_cols]
    for col in kpi_cols:
        bbh[col] = pd.to_numeric(bbh[col].astype(str).str.replace(",", "", regex=False), errors="coerce")

    # Rename daily payload
    if "Total LTE data volume, DL + UL" in daily.columns:
        daily = daily.rename(columns={"Total LTE data volume, DL + UL": "Daily LTE Payload"})

    # Merge BBH + Daily safely
    merge_cols = ["Date", "LNBTS name", "LNCEL name"]
    for col in ["Daily LTE Payload", "VoLTE total traffic"]:
        if col not in daily.columns:
            daily[col] = None

    merged = pd.merge(
        bbh,
        daily[["Date", "LNBTS name", "LNCEL name", "Daily LTE Payload", "VoLTE total traffic"]],
        on=merge_cols,
        how="left",
        suffixes=("_BBH", "_DAILY")
    )
    return merged

# ---------------------- Acceptance Sheet with Remarks ----------------------
def build_acceptance_with_remarks(bbh_file, daily_file, lnbts_list):
    df = merge_bbh_daily(bbh_file, daily_file)

    if lnbts_list != "ALL":
        df = df[df["LNBTS name"].isin(lnbts_list if isinstance(lnbts_list, list) else [lnbts_list])]

    kpi_list = [
        "Average CQI", "Avg RRC conn UE", "Avg UE distance", "Cell Avail excl BLU",
        "E-RAB DR RAN", "E-UTRAN Avg PRB usage per TTI DL", "E-UTRAN E-RAB stp SR",
        "Init Contx stp SR for CSFB", "Intra eNB HO SR", "Avg IP thp DL QCI9",
        "Total E-UTRAN RRC conn stp SR", "Total LTE Traffic (24 Hr)", "VoLTE total traffic",
        "E-UTRAN Intra-Freq HO SR", "E-UTRAN Inter-Freq HO SR", "inter eNB E-UTRAN HO SR X2"
    ]
        
    rows = []
    for _, row in df.iterrows():
        for kpi in kpi_list:
            # safely get value
            if kpi == "Total LTE Traffic (24 Hr)":
                value = row.get("Daily LTE Payload", None)
            elif kpi == "VoLTE total traffic":
                value = row.get("VoLTE total traffic", None)
            else:
                value = row.get(kpi, None)

            if value is not None:
                rows.append({
                    "LNBTS name": row.get("LNBTS name", ""),
                    "LNCEL name": row.get("LNCEL name", ""),
                    "KPI NAME": kpi,
                    "Date": row.get("Date", ""),
                    "Value": value
                })

    df_acc = pd.DataFrame(rows)
    if df_acc.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Pivot safely
    df_acc = df_acc.pivot_table(
        index=["LNBTS name", "LNCEL name", "KPI NAME"],
        columns="Date",
        values="Value",
        aggfunc="first"
    ).reset_index()
    df_acc.columns.name = None

    # -------- Add Remarks --------
    thresholds = {
        "Total E-UTRAN RRC conn stp SR": ">99",
        "Avg IP thp DL QCI9": ">5000",
        "Intra eNB HO SR": ">98",
        "inter eNB E-UTRAN HO SR X2": ">98",
        "Init Contx stp SR for CSFB": ">98",
        "E-UTRAN Intra-Freq HO SR": ">98",
        "E-UTRAN Inter-Freq HO SR": ">98",
        "E-UTRAN E-RAB stp SR": ">99",
        "E-RAB DR RAN": "<1",
        "Cell Avail excl BLU": ">99.5",
        "Average CQI": ">7"
    }

    # Get last date column
    last_date_col = next((c for c in reversed(df_acc.columns) if c not in ["LNBTS name", "LNCEL name", "KPI NAME"]), None)
    remarks = []
    for idx, row in df_acc.iterrows():
        kpi = row["KPI NAME"]
        value = row.get(last_date_col, None)
        threshold = thresholds.get(kpi)
        if threshold and pd.notna(value):
            try:
                if threshold.startswith(">"):
                    remarks.append("KPI Stable / Meeting Threshold" if value >= float(threshold[1:]) else "Fail")
                elif threshold.startswith("<"):
                    remarks.append("KPI Stable / Meeting Threshold" if value <= float(threshold[1:]) else "Fail")
                else:
                    remarks.append("")
            except:
                remarks.append("Fail")
        else:
            remarks.append("")
    df_acc["Remarks"] = remarks

    # -------- Acceptance Status Summary --------
    summary_rows = []
    grouped = df_acc.groupby(["LNBTS name", "LNCEL name"])
    for (lnbts, lncel), group in grouped:
        failed_kpis = group[group["Remarks"] == "Fail"]["KPI NAME"].tolist()
        summary_rows.append({
            "LNBTS": lnbts,
            "LNCEL": lncel,
            "Acceptance Status": "Pass" if len(failed_kpis)==0 else "Fail",
            "Failed KPIs": ", ".join(failed_kpis)
        })
    summary_df = pd.DataFrame(summary_rows)

    return df_acc, summary_df

# ---------------------- BBH Tracker ----------------------
def build_bbh_tracker(bbh_file, daily_file, lnbts_list, existing_tracker=None):
    df = merge_bbh_daily(bbh_file, daily_file)

    if lnbts_list != "ALL":
        df = df[df["LNBTS name"].isin(lnbts_list if isinstance(lnbts_list, list) else [lnbts_list])]

    tracker_kpis = [
        # list all required KPIs safely
        "Cell Avail excl BLU", "E-UTRAN Avg PRB usage per TTI DL", "% MIMO RI 2", "% MIMO RI 1",
        "Init Contx stp SR for CSFB", "RACH Stp Completion SR", "SINR_PUSCH_AVG (M8005C95)",
        "SINR_PUCCH_AVG (M8005C92)", "Avg RSSI for PUSCH", "RSSI_PUCCH_AVG (M8005C2)",
        "Avg PDCP cell thp DL", "Avg IP thp DL QCI9", "Total LTE data volume, DL + UL",
        "Avg UE distance", "Average CQI", "Avg RRC conn UE2", "inter eNB E-UTRAN HO SR X2",
        "Intra eNB HO SR", "E-RAB DR RAN", "E-UTRAN E-RAB stp SR",
        "Total E-UTRAN RRC conn stp SR", "Avg IP thp DL QCI6", "Avg IP thp DL QCI8", "Avg IP thp DL QCI7",
        "Avg DL nonGBR IP thp UEs w/out CA", "Avg DL nonGBR IP thp CA active UEs 2 CCS",
        "E-UTRAN RLC PDU Volume DL via Scell", "RLC PDU vol DL via Pcell",
        "Avg DL User throughput", "Avg UL User throughput",
        "Total LTE Traffic (24 Hr)", "VoLTE total traffic","E-UTRAN Intra-Freq HO SR", "E-UTRAN Inter-Freq HO SR",
        "inter eNB E-UTRAN HO SR X2"
    ]

    rows = []
    for _, row in df.iterrows():
        for kpi in tracker_kpis:
            value = row.get(kpi, None)
            if kpi == "Total LTE Traffic (24 Hr)":
                value = row.get("Daily LTE Payload", None)
            elif kpi == "VoLTE total traffic":
                value = row.get("VoLTE total traffic", None)
            if value is not None:
                rows.append({
                    "LNBTS name": row.get("LNBTS name", ""),
                    "LNCEL name": row.get("LNCEL name", ""),
                    "KPI NAME": kpi,
                    "Date": row.get("Date", ""),
                    "Value": value
                })

    df_tracker = pd.DataFrame(rows)
    if not df_tracker.empty:
        df_tracker = df_tracker.pivot_table(
            index=["LNBTS name", "LNCEL name", "KPI NAME"],
            columns='Date',
            values='Value',
            aggfunc="first"
        ).reset_index()
        df_tracker.columns.name = None

    if existing_tracker is not None:
        df_tracker = pd.concat([existing_tracker, df_tracker], ignore_index=True).drop_duplicates(
            subset=["LNBTS name", "LNCEL name", "KPI NAME"], keep="last"
        )
    return df_tracker

# ---------------------- Streamlit App ----------------------
st.set_page_config(page_title="LTE Ops Tool", layout="wide")
st.title("📡 LTE Daily Acceptance & BBH Tracker")

tab1, tab2 = st.tabs(["✅ Acceptance Sheet", "📊 BBH Tracker"])

# --------- Acceptance Tab ---------
with tab1:
    st.subheader("Acceptance Sheet Generator")
    bbh_file = st.file_uploader("Upload BBH RAW File", type="xlsx")
    daily_file = st.file_uploader("Upload Daily LTE File", type="xlsx")
    lnbts_input = st.text_input("LNBTS Name (comma separated or ALL)", value="ALL")

    if st.button("Generate Acceptance Sheet"):
        if bbh_file and daily_file:
            lnbts_list = [x.strip() for x in lnbts_input.split(",")] if lnbts_input != "ALL" else "ALL"
            df_acc, df_summary = build_acceptance_with_remarks(bbh_file, daily_file, lnbts_list)
            st.dataframe(df_acc)
            st.subheader("Acceptance Summary")
            st.dataframe(df_summary)

            # Download
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
                df_acc.to_excel(writer, index=False, sheet_name="Acceptance")
                df_summary.to_excel(writer, index=False, sheet_name="Summary")
            st.download_button("⬇ Download Acceptance + Summary", buffer.getvalue(), "Acceptance.xlsx")
        else:
            st.warning("Upload both BBH and Daily files.")

# --------- BBH Tracker Tab ---------
with tab2:
    st.subheader("BBH Tracker")
    bbh_file2 = st.file_uploader("Upload BBH RAW File", type="xlsx", key="bbh2")
    daily_file2 = st.file_uploader("Upload Daily LTE File", type="xlsx", key="daily2")
    mode = st.radio("Tracker Mode", ["New BBH Tracker", "Update Existing Tracker"])
    existing_tracker = None
    if mode == "Update Existing Tracker":
        existing_file = st.file_uploader("Upload Existing BBH Tracker", type="xlsx")
        if existing_file:
            existing_tracker = pd.read_excel(existing_file)

    lnbts_input2 = st.text_input("LNBTS Name (comma separated or ALL)", value="ALL", key="lnbts2")

    if st.button("Generate BBH Tracker"):
        if bbh_file2 and daily_file2:
            lnbts_list2 = [x.strip() for x in lnbts_input2.split(",")] if lnbts_input2 != "ALL" else "ALL"
            df_tracker = build_bbh_tracker(bbh_file2, daily_file2, lnbts_list2, existing_tracker)
            st.dataframe(df_tracker)

            buffer2 = BytesIO()
            df_tracker.to_excel(buffer2, index=False, engine="xlsxwriter")
            st.download_button("⬇ Download BBH Tracker", buffer2.getvalue(), "BBH_Tracker.xlsx")
        else:
            st.warning("Upload both BBH and Daily files.")







