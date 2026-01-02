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
    bbh["Period start time"] = pd.to_datetime(bbh["Period start time"])
    bbh["Date"] = bbh["Period start time"].dt.date
    bbh["Hour"] = bbh["Period start time"].dt.hour

    daily["Period start time"] = pd.to_datetime(daily["Period start time"])
    daily["Date"] = daily["Period start time"].dt.date

    # Clean numeric KPIs in BBH
    id_cols = ["Period start time", "Date", "Hour", "MRBTS name", "LNBTS name", "LNCEL name"]
    kpi_cols = [c for c in bbh.columns if c not in id_cols]

    for col in kpi_cols:
        bbh[col] = pd.to_numeric(bbh[col].astype(str).str.replace(",", "", regex=False), errors="coerce")

    # Rename daily payload
    if "Total LTE data volume, DL + UL" in daily.columns:
        daily = daily.rename(columns={"Total LTE data volume, DL + UL": "Daily LTE Payload"})

    # Merge BBH + Daily
    merge_cols = ["Date", "LNBTS name", "LNCEL name"]
    if "Daily LTE Payload" in daily.columns:
        merge_cols.append("Daily LTE Payload")
    if "VoLTE total traffic" in daily.columns:
        merge_cols.append("VoLTE total traffic")

    merged = pd.merge(
        bbh,
        daily[merge_cols],
        on=["Date", "LNBTS name", "LNCEL name"],
        how="left",
        suffixes=("_BBH", "_DAILY")
    )

    return merged

# ---------------------- Thresholds ----------------------
thresholds = {
    "Total E-UTRAN RRC conn stp SR": (">", 99),
    "Avg IP thp DL QCI9": (">", 5000),
    "Intra eNB HO SR": (">", 98),
    "inter eNB E-UTRAN HO SR X2": (">", 98),
    "Init Contx stp SR for CSFB": (">", 98),
    "E-UTRAN Intra-Freq HO SR": (">", 98),
    "E-UTRAN Inter-Freq HO SR": (">", 98),
    "E-UTRAN E-RAB stp SR": (">", 99),
    "E-RAB DR RAN": ("<", 1),
    "Cell Avail excl BLU": (">", 99.5),
    "Average CQI": (">", 7)
}

# ---------------------- Acceptance Sheet ----------------------
def build_acceptance(bbh_file, daily_file, lnbts_list):
    df = merge_bbh_daily(bbh_file, daily_file)

    if lnbts_list != "ALL":
        df = df[df["LNBTS name"].isin(lnbts_list if isinstance(lnbts_list, list) else [lnbts_list])]

    kpi_list = [
        "Average CQI", "Avg RRC conn UE", "Avg UE distance", "Cell Avail excl BLU",
        "E-RAB DR RAN", "E-UTRAN Avg PRB usage per TTI DL", "E-UTRAN E-RAB stp SR",
        "Init Contx stp SR for CSFB", "Intra eNB HO SR", "Avg IP thp DL QCI9",
        "Total E-UTRAN RRC conn stp SR", "Total LTE data volume, DL + UL", "VoLTE total traffic",
        "E-UTRAN Intra-Freq HO SR", "E-UTRAN Inter-Freq HO SR"
    ]

    rows = []
    missing_kpis = set()
    for _, row in df.iterrows():
        for kpi in kpi_list:
            if kpi not in row:
                missing_kpis.add(kpi)
                continue

            if kpi == "Total LTE Traffic (24 Hr)":
                if "Daily LTE Payload" in row:
                    value = row["Daily LTE Payload"]  # always from daily file
                elif "Total LTE data volume, DL + UL" in row:
                    value = row["Total LTE data volume, DL + UL"]  # fallback
                else:
                    continue  # skip if neither column exists
            elif kpi == "VoLTE total traffic":
                if "VoLTE total traffic" in row:
                    value = row["VoLTE total traffic"]
                else:
                    continue
            else:
                if kpi in row:
                    value = row[kpi]
                else:
                    continue

            rows.append({
                "LNBTS name": row["LNBTS name"],
                "LNCEL name": row["LNCEL name"],
                "KPI NAME": kpi,
                "Date": row["Date"],
                "Value": value
            })

    df_acc = pd.DataFrame(rows)
    if not df_acc.empty:
        df_acc = df_acc.pivot_table(
            index=["LNBTS name", "LNCEL name", "KPI NAME"],
            columns='Date',
            values='Value',
            aggfunc="first"
        ).reset_index()
        df_acc.columns.name = None

    # ---------- Add Remarks based on last day ----------
    if not df_acc.empty:
        date_cols = [c for c in df_acc.columns if str(c).count("-") == 2]
        if date_cols:
            last_date = max(date_cols)
            remarks = []
            for _, row in df_acc.iterrows():
                kpi = row["KPI NAME"]
                last_val = row[last_date]
                if kpi in thresholds:
                    comp, thr = thresholds[kpi]
                    if pd.isna(last_val):
                        remark = "No Data"
                    elif comp == ">" and last_val >= thr:
                        remark = "KPI Stable / Meeting Threshold"
                    elif comp == "<" and last_val <= thr:
                        remark = "KPI Stable / Meeting Threshold"
                    else:
                        remark = "Fail Threshold"
                else:
                    remark = "N/A"
                remarks.append(remark)
            df_acc["Remarks"] = remarks

    # ---------- Acceptance Status sheet ----------
    status_rows = []
    for (lnbts, lncel), group in df_acc.groupby(["LNBTS name", "LNCEL name"]):
        fail_kpis = group.loc[group["Remarks"] == "Fail Threshold", "KPI NAME"].tolist()
        status_rows.append({
            "LNBTS name": lnbts,
            "LNCEL name": lncel,
            "Acceptance Status": "Pass" if not fail_kpis else "Fail",
            "Failing KPIs": ", ".join(fail_kpis) if fail_kpis else ""
        })
    df_status = pd.DataFrame(status_rows)

    return df_acc, df_status, missing_kpis

# ---------------------- BBH Tracker ----------------------
def build_bbh_tracker(bbh_file, daily_file, lnbts_list, existing_tracker=None):
    df = merge_bbh_daily(bbh_file, daily_file)

    if lnbts_list != "ALL":
        df = df[df["LNBTS name"].isin(lnbts_list if isinstance(lnbts_list, list) else [lnbts_list])]

    tracker_kpis = [
        "Cell Avail excl BLU", "E-UTRAN Avg PRB usage per TTI DL", "% MIMO RI 2", "% MIMO RI 1",
        "Init Contx stp SR for CSFB", "RACH Stp Completion SR", "SINR_PUSCH_AVG (M8005C95)",
        "SINR_PUCCH_AVG (M8005C92)", "Avg RSSI for PUSCH", "RSSI_PUCCH_AVG (M8005C2)",
        "Avg PDCP cell thp DL", "Avg IP thp DL QCI9",
        "Avg UE distance", "Average CQI", "Avg RRC conn UE", "inter eNB E-UTRAN HO SR X2",
        "Intra eNB HO SR", "E-RAB DR RAN", "E-UTRAN E-RAB stp SR",
        "Total E-UTRAN RRC conn stp SR", "Avg IP thp DL QCI6", "Avg IP thp DL QCI8", "Avg IP thp DL QCI7",
        "Avg DL nonGBR IP thp UEs w/out CA", "Avg DL nonGBR IP thp CA active UEs 2 CCS",
        "E-UTRAN RLC PDU Volume DL via Scell", "RLC PDU vol DL via Pcell",
        "Avg DL User throughput", "Avg UL User throughput",
        "Total LTE data volume, DL + UL", "VoLTE total traffic"
    ]

    rows = []
    missing_kpis = set()
    for _, row in df.iterrows():
        for kpi in tracker_kpis:
            if kpi not in row:
                missing_kpis.add(kpi)
                continue

            if kpi == "Total LTE Traffic (24 Hr)":
                if "Daily LTE Payload" in row:
                    value = row["Daily LTE Payload"]
                elif "Total LTE data volume, DL + UL" in row:
                    value = row["Total LTE data volume, DL + UL"]
                else:
                    continue
            elif kpi == "VoLTE total traffic":
                if "VoLTE total traffic" in row:
                    value = row["VoLTE total traffic"]
                else:
                    continue
            else:
                if kpi in row:
                    value = row[kpi]
                else:
                    continue

            rows.append({
                "LNBTS name": row["LNBTS name"],
                "LNCEL name": row["LNCEL name"],
                "KPI NAME": kpi,
                "Date": row["Date"],
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

    return df_tracker, missing_kpis

# ---------------------- Streamlit App ----------------------
st.set_page_config(page_title="LTE Ops Tool", layout="wide")
st.title("📡 LTE Daily Acceptance & BBH Tracker")

tab1, tab2 = st.tabs(["✅ Acceptance Sheet", "📊 BBH Tracker"])

# --------- ACCEPTANCE TAB ---------
with tab1:
    st.subheader("Acceptance Sheet Generator")
    bbh_file = st.file_uploader("Upload BBH RAW File", type="xlsx")
    daily_file = st.file_uploader("Upload Daily LTE File", type="xlsx")
    lnbts_input = st.text_input("LNBTS Name (comma separated or ALL)", value="ALL")

    if st.button("Generate Acceptance Sheet"):
        if bbh_file and daily_file:
            lnbts_list = [x.strip() for x in lnbts_input.split(",")] if lnbts_input != "ALL" else "ALL"
            df_acc, df_status, missing_kpis = build_acceptance(bbh_file, daily_file, lnbts_list)
            st.dataframe(df_acc)
            st.markdown("### Acceptance Status")
            st.dataframe(df_status)

            if missing_kpis:
                st.warning(f"KPIs not found in data and ignored: {', '.join(missing_kpis)}")

            # Download all sheets
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
                df_acc.to_excel(writer, index=False, sheet_name="Acceptance_Sheet")
                df_status.to_excel(writer, index=False, sheet_name="Acceptance_Status")
            st.download_button("⬇ Download Acceptance Excel", buffer.getvalue(), "Acceptance.xlsx")
        else:
            st.warning("Upload both BBH and Daily files.")

# --------- BBH TRACKER TAB ---------
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
            df_tracker, missing_kpis = build_bbh_tracker(bbh_file2, daily_file2, lnbts_list2, existing_tracker)
            st.dataframe(df_tracker)

            if missing_kpis:
                st.warning(f"KPIs not found in data and ignored: {', '.join(missing_kpis)}")

            buffer2 = BytesIO()
            df_tracker.to_excel(buffer2, index=False, engine="xlsxwriter")
            st.download_button("⬇ Download BBH Tracker", buffer2.getvalue(), "BBH_Tracker.xlsx")
        else:
            st.warning("Upload both BBH and Daily files.")



