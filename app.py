# app.py
# Streamlit app to ingest a monthly file (Arabic headers supported),
# map -> clean -> validate -> append to Master.xlsx, build pivots & charts,
# and provide downloads.

import os, glob, hashlib, shutil
from io import BytesIO
from datetime import datetime
from typing import List, Tuple

import pandas as pd
import streamlit as st

# -----------------------
# CONFIG (edit if you like)
# -----------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "MonthlyRaw")       # optional local drop folder
OUT_MASTER = os.path.join(BASE_DIR, "Master.xlsx")   # persisted on local run
LOG_PATH = os.path.join(BASE_DIR, "ingest_log.csv")  # file de-dupe by hash
HISTORY_SHEET = "History"
PIVOT_SHEET   = "Pivots"
DASH_SHEET    = "Dashboard"
EXPECTED_COLS = ["ID", "Nationality", "Cost Center", "Amount", "Month"]
DEDUP_KEYS    = ["ID", "Month"]
BACKUP_KEEP   = 10

# Arabic/English header hints to auto-map
HEADER_HINTS = {
    "ID": ["رقم الهوية", "رقم المشترك", "ID", "National ID", "Iqama", "Iqama ID"],
    "Nationality": ["الجنسية", "Nationality"],
    "Cost Center": ["وحدة الاشتراك", "مركز التكلفة", "Cost Center", "CostCenter", "Dept", "Department"],
    "Amount": ["المبلغ الإجمالي (ر.س)", "المبلغ الإجمالي", "إجمالي الاشتراكات", "Total", "Amount", "المبلغ"],
    "Month": ["الشهر", "Month", "Period", "الفترة", "تاريخ الاستحقاق", "الأجر الخاضع للاشتراك (ر.س) - تاريخ"]  # last one rarely used
}

# -----------------------
# Helpers
# -----------------------
def ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def file_hash(f) -> str:
    # works for both file-like and path
    h = hashlib.md5()
    if isinstance(f, (bytes, bytearray)):
        h.update(f)
    elif hasattr(f, "read"):
        pos = f.tell()
        h.update(f.read())
        f.seek(pos)
    else:
        with open(f, "rb") as fh:
            h.update(fh.read())
    return h.hexdigest()

def ensure_dirs():
    os.makedirs(BASE_DIR, exist_ok=True)
    os.makedirs(RAW_DIR, exist_ok=True)

def load_log() -> pd.DataFrame:
    if os.path.exists(LOG_PATH):
        try:
            return pd.read_csv(LOG_PATH)
        except Exception:
            pass
    return pd.DataFrame(columns=["filename", "hash", "ingested_at"])

def save_log(df: pd.DataFrame):
    df.to_csv(LOG_PATH, index=False)

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    return out

def auto_guess_mapping(headers: List[str]):
    """Return a default mapping dict target->source if obvious, else None"""
    mapping = {}
    for target, hints in HEADER_HINTS.items():
        found = None
        for h in headers:
            if h.strip() in hints:
                found = h
                break
        mapping[target] = found
    return mapping

def validate_and_clean(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Enforce schema & types. Amount numeric, Month to first day, ID trimming.
    Returns (good_rows_df, exceptions_df)
    """
    err_rows = []
    w = df.copy()

    # Coerce
    w["ID"] = w["ID"].astype(str).str.strip()
    w["Nationality"] = w["Nationality"].astype(str).str.strip()
    w["Cost Center"] = w["Cost Center"].astype(str).str.strip()

    # Amount numeric
    w["Amount_raw"] = w["Amount"]
    w["Amount"] = pd.to_numeric(w["Amount"], errors="coerce")

    # Month date
    w["Month_raw"] = w["Month"]
    w["Month"] = pd.to_datetime(w["Month"], errors="coerce")

    # Flags
    bad_amt = w["Amount"].isna()
    bad_month = w["Month"].isna()

    if bad_amt.any():
        bad = w.loc[bad_amt].copy()
        bad["Issue"] = "Non-numeric Amount"
        err_rows.append(bad)

    if bad_month.any():
        bad = w.loc[bad_month].copy()
        bad["Issue"] = "Invalid Month"
        err_rows.append(bad)

    good = w.loc[~(bad_amt | bad_month), EXPECTED_COLS].copy()
    if not good.empty:
        good["Month"] = good["Month"].dt.to_period("M").dt.to_timestamp()

    exceptions = (pd.concat(err_rows, ignore_index=True)
                  if err_rows else pd.DataFrame(columns=list(w.columns)+["Issue"]))
    return good, exceptions

def read_history() -> pd.DataFrame:
    if not os.path.exists(OUT_MASTER):
        return pd.DataFrame(columns=EXPECTED_COLS)
    try:
        hist = pd.read_excel(OUT_MASTER, sheet_name=HISTORY_SHEET)
    except ValueError:
        hist = pd.DataFrame(columns=EXPECTED_COLS)
    if not hist.empty:
        hist["Month"] = pd.to_datetime(hist["Month"], errors="coerce")
        hist["Month"] = hist["Month"].dt.to_period("M").dt.to_timestamp()
        hist["ID"] = hist["ID"].astype(str).str.strip()
    return hist

def build_pivots(history: pd.DataFrame):
    if history.empty:
        return pd.DataFrame(), pd.DataFrame()
    pivot_nat = (history.groupby(["Month","Nationality"], as_index=False)["Amount"]
                 .sum().pivot(index="Month", columns="Nationality", values="Amount")
                 .fillna(0.0).sort_index())
    pivot_cc = (history.groupby(["Month","Cost Center"], as_index=False)["Amount"]
                .sum().pivot(index="Month", columns="Cost Center", values="Amount")
                .fillna(0.0).sort_index())
    return pivot_nat, pivot_cc

def save_master_with_pivots(history: pd.DataFrame, pivot_nat: pd.DataFrame, pivot_cc: pd.DataFrame):
    # Rotate backups
    if os.path.exists(OUT_MASTER):
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup = OUT_MASTER.replace(".xlsx", f"_backup_{stamp}.xlsx")
        shutil.copyfile(OUT_MASTER, backup)
        siblings = sorted(glob.glob(OUT_MASTER.replace(".xlsx", "_backup_*.xlsx")))
        if len(siblings) > BACKUP_KEEP:
            for s in siblings[:-BACKUP_KEEP]:
                os.remove(s)

    with pd.ExcelWriter(OUT_MASTER, engine="xlsxwriter") as xl:
        history.to_excel(xl, sheet_name=HISTORY_SHEET, index=False)
        pivot_nat.to_excel(xl, sheet_name=PIVOT_SHEET, startrow=0, startcol=0)
        startcol2 = pivot_nat.shape[1] + 3
        pivot_cc.to_excel(xl, sheet_name=PIVOT_SHEET, startrow=0, startcol=startcol2)

        wb = xl.book
        ws = wb.add_worksheet(DASH_SHEET)
        fmt_title = wb.add_format({"bold": True, "font_size": 16})
        ws.write(0, 0, "Monthly Dashboard", fmt_title)
        ws.write(1, 0, f"Generated: {ts()}")

        # Dump pivot_nat to Dashboard sheet and chart
        r0, c0 = 3, 0
        ws.write(r0, c0, "Amount by Nationality")
        df_nat = pivot_nat.reset_index()
        # headers
        for j, col in enumerate(df_nat.columns):
            ws.write(r0+1, c0+j, str(col))
        for i in range(len(df_nat)):
            for j in range(len(df_nat.columns)):
                ws.write(r0+2+i, c0+j, df_nat.iloc[i, j])
        chart1 = wb.add_chart({'type': 'line'})
        n_series = len(df_nat.columns) - 1
        for s in range(n_series):
            chart1.add_series({
                'name':       [DASH_SHEET, r0+1, c0+1+s],
                'categories': [DASH_SHEET, r0+2, c0, r0+1+len(df_nat), c0],
                'values':     [DASH_SHEET, r0+2, c0+1+s, r0+1+len(df_nat), c0+1+s],
            })
        chart1.set_title({'name': 'Amount by Nationality'})
        chart1.set_x_axis({'name': 'Month'})
        chart1.set_y_axis({'name': 'Amount'})
        ws.insert_chart(r0+1, c0 + len(df_nat.columns) + 2, chart1)

        # Dump pivot_cc and chart
        r1 = r0 + max(16, len(df_nat) + 10)
        ws.write(r1, c0, "Amount by Cost Center")
        df_cc = pivot_cc.reset_index()
        for j, col in enumerate(df_cc.columns):
            ws.write(r1+1, c0+j, str(col))
        for i in range(len(df_cc)):
            for j in range(len(df_cc.columns)):
                ws.write(r1+2+i, c0+j, df_cc.iloc[i, j])
        chart2 = wb.add_chart({'type': 'column'})
        m_series = len(df_cc.columns) - 1
        for s in range(min(m_series, 6)):
            chart2.add_series({
                'name':       [DASH_SHEET, r1+1, c0+1+s],
                'categories': [DASH_SHEET, r1+2, c0, r1+1+len(df_cc), c0],
                'values':     [DASH_SHEET, r1+2, c0+1+s, r1+1+len(df_cc), c0+1+s],
            })
        chart2.set_title({'name': 'Top Cost Centers (first 6)'})
        chart2.set_x_axis({'name': 'Month'})
        chart2.set_y_axis({'name': 'Amount'})
        ws.insert_chart(r1+1, c0 + len(df_cc.columns) + 2, chart2)

def to_excel_download(*dfs_with_names):
    """Pack multiple dataframes into a single xlsx binary for download."""
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as xl:
        for df, name in dfs_with_names:
            df.to_excel(xl, sheet_name=name, index=False if name != "PIVOT_NAT" and name != "PIVOT_CC" else True)
    return bio.getvalue()

# -----------------------
# Streamlit UI
# -----------------------
st.set_page_config(page_title="Monthly Ingest (Arabic→Standard)", layout="wide")
st.title("📥 Monthly Data Ingest → Master.xlsx")

ensure_dirs()

left, right = st.columns([2, 1])

with left:
    st.subheader("1) Upload new month file")
    upl = st.file_uploader("Upload Excel/CSV", type=["xlsx", "csv"])

    st.caption("Tip: If your source has multiple contribution columns, choose **Amount = المبلغ الإجمالي (ر.س)** "
               "or manually build it via the 'Sum selected columns' toggle below.")

if upl:
    # Read uploaded
    try:
        if upl.name.lower().endswith(".csv"):
            try:
                df_raw = pd.read_csv(upl, engine="pyarrow")
            except Exception:
                upl.seek(0)
                df_raw = pd.read_csv(upl)
        else:
            df_raw = pd.read_excel(upl)
    except Exception as e:
        st.error(f"Failed to read file: {e}")
        st.stop()

    df_raw = standardize_columns(df_raw)
    st.write("**Raw preview**")
    st.dataframe(df_raw.head(20), use_container_width=True)

    # ---- Column Mapping ----
    st.subheader("2) Map columns to target schema")
    headers = list(df_raw.columns)
    default_map = auto_guess_mapping(headers)

    c1, c2, c3 = st.columns(3)
    with c1:
        col_id = st.selectbox("ID (رقم الهوية)", options=["-- pick --"] + headers,
                              index=(headers.index(default_map["ID"]) + 1) if default_map["ID"] in headers else 0)
        col_nat = st.selectbox("Nationality (الجنسية)", options=["-- pick --"] + headers,
                               index=(headers.index(default_map["Nationality"]) + 1) if default_map["Nationality"] in headers else 0)
    with c2:
        col_cc = st.selectbox("Cost Center (وحدة الاشتراك/مركز التكلفة)", options=["-- pick --"] + headers,
                              index=(headers.index(default_map["Cost Center"]) + 1) if default_map["Cost Center"] in headers else 0)
        col_month = st.selectbox("Month (الشهر/الفترة/تاريخ)", options=["-- pick --"] + headers,
                                 index=(headers.index(default_map["Month"]) + 1) if default_map["Month"] in headers else 0)
    with c3:
        use_sum = st.checkbox("Amount = sum of multiple columns?")
        if use_sum:
            amount_cols = st.multiselect("Pick columns to sum for Amount", options=headers,
                                         default=[c for c in headers if c in HEADER_HINTS["Amount"]][:1])
        else:
            col_amt = st.selectbox("Amount (المبلغ الإجمالي)", options=["-- pick --"] + headers,
                                   index=(headers.index(default_map["Amount"]) + 1) if default_map["Amount"] in headers else 0)

    # Build standardized DF
    if st.button("▶️ Build standardized table"):
        missing_selects = []
        if col_id == "-- pick --": missing_selects.append("ID")
        if col_nat == "-- pick --": missing_selects.append("Nationality")
        if col_cc == "-- pick --": missing_selects.append("Cost Center")
        if col_month == "-- pick --": missing_selects.append("Month")
        if use_sum and not st.session_state.get("ok_sum", False):
            # handled below
            pass
        if not use_sum and col_amt == "-- pick --":
            missing_selects.append("Amount")

        if use_sum and (not amount_cols or len(amount_cols) == 0):
            missing_selects.append("Amount (pick columns to sum)")

        if missing_selects:
            st.error(f"Please pick: {', '.join(missing_selects)}")
            st.stop()

        # Compose standardized columns
        std = pd.DataFrame()
        std["ID"] = df_raw[col_id].astype(str).str.strip()
        std["Nationality"] = df_raw[col_nat].astype(str).str.strip()
        std["Cost Center"] = df_raw[col_cc].astype(str).str.strip()
        if use_sum:
            std["Amount"] = df_raw[amount_cols].apply(pd.to_numeric, errors="coerce").sum(axis=1)
        else:
            std["Amount"] = pd.to_numeric(df_raw[col_amt], errors="coerce")
        std["Month"] = pd.to_datetime(df_raw[col_month], errors="coerce")

        # Validate & clean
        clean, exceptions = validate_and_clean(std[EXPECTED_COLS])

        st.success(f"Standardized rows: {len(std):,} | Valid rows: {len(clean):,} | Exceptions: {len(exceptions):,}")
        st.write("**Cleaned (valid) preview**")
        st.dataframe(clean.head(30), use_container_width=True)

        if not exceptions.empty:
            st.warning("Exceptions found (non-numeric Amount or invalid Month).")
            with st.expander("Show exceptions"):
                st.dataframe(exceptions, use_container_width=True)

        # ---- Append to Master & build pivots ----
        history = read_history()
        before = len(history)
        history = pd.concat([history, clean], ignore_index=True)
        history = history.drop_duplicates(subset=DEDUP_KEYS, keep="last")
        after = len(history)
        added = after - before

        pivot_nat, pivot_cc = build_pivots(history)

        # Save to OUT_MASTER on disk
        save_master_with_pivots(history, pivot_nat, pivot_cc)

        # Update de-dupe log by file hash
        log = load_log()
        # for uploads, use content hash + filename
        upl_hash = file_hash(upl)
        log = pd.concat([log, pd.DataFrame({
            "filename": [upl.name],
            "hash": [upl_hash],
            "ingested_at": [ts()]
        })], ignore_index=True)
        save_log(log)

        st.info(f"Appended {added} new unique rows into Master.xlsx (key: ID+Month). "
                f"Master location: `{OUT_MASTER}`")

        # ---- Show pivots ----
        st.subheader("3) Pivots")
        tabs = st.tabs(["Pivot: Nationality × Month", "Pivot: Cost Center × Month"])
        with tabs[0]:
            st.dataframe(pivot_nat.reset_index(), use_container_width=True)
            st.line_chart(pivot_nat)
        with tabs[1]:
            st.dataframe(pivot_cc.reset_index(), use_container_width=True)
            st.bar_chart(pivot_cc)

        # ---- Downloads ----
        st.subheader("4) Downloads")
        # fresh master as binary (read back from disk)
        with open(OUT_MASTER, "rb") as f:
            st.download_button("💾 Download Master.xlsx", f.read(), file_name="Master.xlsx")

        # pack separate cleaned/exceptions/pivots for convenience
        pkg = to_excel_download(
            (clean, "CLEANED"),
            (exceptions if not exceptions.empty else pd.DataFrame({"Info": ["No exceptions"]}), "EXCEPTIONS"),
            (pivot_nat.reset_index(), "PIVOT_NAT"),
            (pivot_cc.reset_index(), "PIVOT_CC")
        )
        st.download_button("📦 Download package (cleaned + exceptions + pivots).xlsx",
                           pkg, file_name="Ingest_Package.xlsx")

with right:
    st.subheader("Settings / Info")
    st.write(f"**Master path**: `{OUT_MASTER}`")
    st.write(f"**Raw folder (optional)**: `{RAW_DIR}`")
    st.write("Drop files into the Raw folder if you prefer a filesystem workflow; "
             "this app focuses on uploads but still writes a persistent Master on disk.")

    st.write("**Schema expected for the standardized table:**")
    st.code(", ".join(EXPECTED_COLS))

    if os.path.exists(LOG_PATH):
        st.write("Recent ingests:")
        logdf = pd.read_csv(LOG_PATH).sort_values("ingested_at", ascending=False).head(10)
        st.dataframe(logdf, use_container_width=True)
