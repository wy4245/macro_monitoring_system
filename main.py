# python -m streamlit run main.py --server.port 8801


import os
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from datetime import datetime, timedelta, date

from modules.calculator.global_treasury import TreasuryCalc


# ─── 페이지 기본 설정 ──────────────────────────────────────────────────────────
st.set_page_config(page_title="MMS", layout="wide")
st.title("MMS(Macro Monitoring System)")

COUNTRIES = ["KR", "US", "DE", "GB", "JP", "CN"]
TENORS    = [2, 3, 5, 10, 20, 30]


# ─── 데이터 로드 ──────────────────────────────────────────────────────────────

def _load_global() -> pd.DataFrame | None:
    """data/global_treasury.parquet 에서 글로벌 국채 데이터를 로드합니다."""
    path = os.path.join("data", "global_treasury.parquet")
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_parquet(path)
        df.index.name = "Date"
        return df
    except Exception as e:
        print(f"[글로벌] 파일 읽기 오류: {e}")
        return None


def _load_otc() -> pd.DataFrame | None:
    """data/otc_summary.parquet 에서 장외거래대표수익률 데이터를 로드합니다."""
    path = os.path.join("data", "otc_summary.parquet")
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_parquet(path)
        df.index.name = "Date"
        return df
    except Exception as e:
        print(f"[OTC] 파일 읽기 오류: {e}")
        return None


def _load_bond() -> pd.DataFrame | None:
    """data/bond_summary.parquet 에서 국내 채권 데이터를 로드합니다."""
    path = os.path.join("data", "bond_summary.parquet")
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_parquet(path)
        df.index.name = "Date"
        return df
    except Exception as e:
        print(f"[BondSummary] 파일 읽기 오류: {e}")
        return None


# ─── 앱 시작 시 사전 계산 ────────────────────────────────────────────────────

_global_df: pd.DataFrame | None = _load_global()
_bond_df:   pd.DataFrame | None = _load_bond()
_otc_df:    pd.DataFrame | None = _load_otc()

# bond_summary의 KTB_nY 컬럼을 KR_nY 형식으로 변환하여 글로벌 데이터와 병합
_merged_df: pd.DataFrame | None = None
if _global_df is not None and _bond_df is not None:
    _ktb_to_kr = {f"KTB_{t}Y": f"KR_{t}Y" for t in TENORS if f"KTB_{t}Y" in _bond_df.columns}
    _kr_df = _bond_df[list(_ktb_to_kr.keys())].rename(columns=_ktb_to_kr)
    _merged_df = TreasuryCalc.merge(_global_df, _kr_df)
elif _global_df is not None:
    _merged_df = _global_df


# ─── 기준일: 실제 데이터의 마지막 날짜 ──────────────────────────────────────────

_candidates = []
if _merged_df is not None and not _merged_df.empty:
    _candidates.append(_merged_df.index.max().date())
if _bond_df is not None and not _bond_df.empty:
    _candidates.append(_bond_df.index.max().date())
TARGET_DATE = max(_candidates) if _candidates else date.today() - timedelta(days=1)

TODAY     = TARGET_DATE
TODAY_STR = TODAY.strftime("%Y-%m-%d")
try:
    START_DATE = TODAY.replace(year=TODAY.year - 1)
except ValueError:  # 2월 29일인 경우
    START_DATE = TODAY - timedelta(days=365)
START_STR = START_DATE.strftime("%Y-%m-%d")


# ─── 채권 종목 한글 레이블 ────────────────────────────────────────────────────

BOND_LABELS: dict[str, str] = {
    "KTB_1Y": "국고채(1년)",    "KTB_2Y": "국고채(2년)",   "KTB_3Y": "국고채(3년)",
    "KTB_5Y": "국고채(5년)",    "KTB_10Y": "국고채(10년)", "KTB_20Y": "국고채(20년)",
    "KTB_30Y": "국고채(30년)",  "KTB_50Y": "국고채(50년)",
    "NHB_5Y":  "국민주택1종(5년)",
    "MSB_91D": "통안증권(91일)", "MSB_1Y": "통안증권(1년)", "MSB_2Y": "통안증권(2년)",
    "KEPCO_3Y": "한전채(3년)",  "KDB_1Y": "산금채(1년)",
    "CORP_AA_3Y": "회사채AA-(3년)", "CORP_BBB_3Y": "회사채BBB-(3년)",
    "CD_91D": "CD(91일)",       "CP_91D": "CP(91일)",
}

# KTB 만기 순서
KTB_TENORS = [1, 2, 3, 5, 10, 20, 30, 50]


# ─── 공통 헬퍼 ────────────────────────────────────────────────────────────────

def _color_bp(val):
    if pd.isna(val):
        return ""
    if isinstance(val, (int, float)):
        if val > 0:
            return "color: #ff4b4b"
        if val < 0:
            return "color: #0068c9"
    return ""


def _build_bond_summary(df: pd.DataFrame, target_date) -> pd.DataFrame:
    """각 채권 시리즈의 현재 금리 + 변화량(bp) 요약 테이블."""
    today = pd.Timestamp(target_date)
    ref_infos = [
        ("1D",  today - pd.Timedelta(days=1)),
        ("1W",  today - pd.Timedelta(days=7)),
        ("MTD", pd.Timestamp(today.year, today.month, 1) - pd.Timedelta(days=1)),
        ("MoM", today - pd.DateOffset(months=1)),
        ("YTD", pd.Timestamp(today.year - 1, 12, 31)),
        ("YoY", today - pd.DateOffset(years=1)),
    ]
    today_vals = TreasuryCalc.get_ref_value(df, today)
    rows: dict = {}
    for col in df.columns:
        label = BOND_LABELS.get(col, col)
        curr  = today_vals.get(col, float("nan")) if col in today_vals.index else float("nan")
        row: dict = {"금리 (%)": curr}
        for ref_label, ref_date in ref_infos:
            ref_vals = TreasuryCalc.get_ref_value(df, ref_date)
            ref      = ref_vals.get(col, float("nan")) if col in ref_vals.index else float("nan")
            row[ref_label] = (curr - ref) * 100 if pd.notna(curr) and pd.notna(ref) else float("nan")
        rows[label] = row
    result = pd.DataFrame.from_dict(rows, orient="index")
    result.index.name = "종목"
    return result


def _yield_curve_at(df: pd.DataFrame, country: str, ref_date) -> pd.Series:
    """ref_date 이하 가장 가까운 날짜의 해당 국가 금리 커브를 반환합니다."""
    cols       = [f"{country}_{t}Y" for t in TENORS]
    avail_cols = [c for c in cols if c in df.columns]
    if not avail_cols:
        return pd.Series(float("nan"), index=TENORS, dtype=float)

    avail_idx = df.index[df.index <= pd.Timestamp(ref_date)]
    if len(avail_idx) == 0:
        return pd.Series(float("nan"), index=TENORS, dtype=float)

    row    = df.loc[avail_idx[-1], avail_cols]
    result = pd.Series(index=TENORS, dtype=float)
    for t in TENORS:
        c = f"{country}_{t}Y"
        if c in row.index:
            result[t] = row[c]
    return result


# ─── 사이드바 네비게이션 ────────────────────────────────────────────────────

with st.sidebar:
    import subprocess as _subprocess

    if st.button("🔄 데이터 최신화", help="git pull로 최신 데이터를 가져온 뒤 새로고침합니다."):
        _result = _subprocess.run(
            ["git", "pull"],
            capture_output=True,
            text=True,
            cwd=os.path.dirname(os.path.abspath(__file__)),
        )
        if _result.returncode == 0:
            st.success(_result.stdout.strip() or "이미 최신 상태입니다.")
        else:
            st.error(f"git pull 실패:\n{_result.stderr.strip()}")
        st.rerun()

    asset_class = st.radio("", ["채권", "주식"], label_visibility="collapsed")


# ══════════════════════════════════════════════════════════════════════════════
# 채권
# ══════════════════════════════════════════════════════════════════════════════

if asset_class == "채권":

    tab_global, tab_domestic, tab_otc, tab_raw = st.tabs([
        "글로벌 국채 금리", "국내 채권 금리", "장외거래 대표수익률", "Raw Data",
    ])

    # ── 글로벌 국채 금리 ─────────────────────────────────────────────────────
    with tab_global:
        st.caption(
            f"Source: investing.com + KOFIA  ·  기준일: {TODAY_STR}  ·  "
            "주말·공휴일은 직전 거래일 값으로 채워짐"
        )

        if _merged_df is None:
            st.error(
                "데이터 파일이 없습니다.  \n"
                "로컬 PC에서 `python collect_data.py` 실행 후 `git push` 해주세요."
            )
        else:
            st.subheader("주요국 금리 동향")
            st.caption("2년물 / 10년물 기준  ·  bp = basis point (0.01%p)")

            summary_df = TreasuryCalc.build_change_summary(_merged_df, target_date=TARGET_DATE)

            format_dict = {}
            for col in summary_df.columns:
                col_label = col[1] if isinstance(col, tuple) else col
                if "%" in col_label:
                    format_dict[col] = "{:.2f}"   # % 단위: 소수점 2자리
                else:
                    format_dict[col] = "{:.1f}"   # bp 단위: 소수점 1자리

            styled = summary_df.style.format(format_dict, na_rep="-")
            bp_cols = [c for c in summary_df.columns if "금리" not in c[1]]
            styled = styled.map(_color_bp, subset=bp_cols)
            styled = styled.set_properties(**{"text-align": "center"})
            st.dataframe(styled, use_container_width=True)

            st.divider()
            st.subheader("국가별 Yield Curve")

            COUNTRY_MAP = {
                "US": "미국", "KR": "한국", "DE": "독일",
                "GB": "영국", "JP": "일본", "CN": "중국",
            }
            avail_countries = [
                c for c in COUNTRIES
                if any(f"{c}_{t}Y" in _merged_df.columns for t in TENORS)
            ]
            selected_code = st.selectbox(
                "국가 선택",
                options=avail_countries,
                format_func=lambda x: COUNTRY_MAP.get(x, x),
                key="curve_country",
            )
            selected_name = COUNTRY_MAP.get(selected_code, selected_code)

            today_curve = _yield_curve_at(_merged_df, selected_code, TODAY)
            week_curve  = _yield_curve_at(_merged_df, selected_code, TODAY - timedelta(days=7))
            month_curve = _yield_curve_at(_merged_df, selected_code, TODAY - timedelta(days=30))
            tenor_labels = [f"{t}Y" for t in TENORS]

            fig_curve = go.Figure()
            if not today_curve.dropna().empty:
                fig_curve.add_trace(go.Scatter(
                    x=tenor_labels, y=today_curve.values,
                    mode="lines+markers", name=f"현재 ({TODAY_STR})",
                    line=dict(width=2.5),
                ))
            if not week_curve.dropna().empty:
                fig_curve.add_trace(go.Scatter(
                    x=tenor_labels, y=week_curve.values,
                    mode="lines+markers", name="1주 전",
                    line=dict(dash="dot", width=1.5), opacity=0.8,
                ))
            if not month_curve.dropna().empty:
                fig_curve.add_trace(go.Scatter(
                    x=tenor_labels, y=month_curve.values,
                    mode="lines+markers", name="1개월 전",
                    line=dict(dash="dash", width=1.5), opacity=0.8,
                ))
            fig_curve.update_layout(
                title=f"{selected_name} 국채 금리 커브",
                xaxis_title="만기", yaxis_title="수익률 (%)",
                hovermode="x unified",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig_curve, use_container_width=True)

            curve_data = {
                "현재(%)": today_curve.values,
                "1W(bp)":  (today_curve - week_curve).values * 100,
                "1M(bp)":  (today_curve - month_curve).values * 100,
            }
            curve_table = pd.DataFrame(curve_data, index=pd.Index(tenor_labels, name="만기"))
            curve_styled = (
                curve_table.style
                .format({"현재(%)": "{:.2f}", "1W(bp)": "{:.1f}", "1M(bp)": "{:.1f}"}, na_rep="-")
                .map(_color_bp, subset=["1W(bp)", "1M(bp)"])
                .set_properties(**{"text-align": "center"})
            )
            st.dataframe(curve_styled, use_container_width=True)

    # ── 국내 채권 금리 ───────────────────────────────────────────────────────
    with tab_domestic:
        st.caption(
            f"Source: KOFIA  ·  기준일: {TODAY_STR}  ·  "
            "주말·공휴일은 직전 거래일 값으로 채워짐"
        )

        if _bond_df is None:
            st.error(
                "데이터 파일이 없습니다.  \n"
                "로컬 PC에서 `python collect_data.py` 실행 후 `git push` 해주세요."
            )
        else:
            st.subheader("국내 채권 금리 동향")
            st.caption("단위: 금리 (%), 변화 bp (0.01%p)")

            bond_summary_df = _build_bond_summary(_bond_df, TARGET_DATE)
            bond_format = {
                "금리 (%)": "{:.2f}", "1D": "{:.1f}", "1W": "{:.1f}",
                "MTD": "{:.1f}", "MoM": "{:.1f}", "YTD": "{:.1f}", "YoY": "{:.1f}",
            }
            bp_cols_bond = [c for c in bond_summary_df.columns if c != "금리 (%)"]
            bond_styled = (
                bond_summary_df.style
                .format(bond_format, na_rep="-")
                .map(_color_bp, subset=bp_cols_bond)
                .set_properties(**{"text-align": "center"})
            )
            st.dataframe(bond_styled, use_container_width=True)

            st.divider()

            ktb_avail = [t for t in KTB_TENORS if f"KTB_{t}Y" in _bond_df.columns]
            if ktb_avail:
                st.subheader("국내 채권 Yield Curve")
                ktb_tenor_labels = [f"{t}Y" for t in ktb_avail]
                ktb_cols         = [f"KTB_{t}Y" for t in ktb_avail]

                def _ktb_curve_at(ref_date) -> pd.Series:
                    avail_idx = _bond_df.index[_bond_df.index <= pd.Timestamp(ref_date)]
                    if len(avail_idx) == 0:
                        return pd.Series(float("nan"), index=ktb_avail, dtype=float)
                    row = _bond_df.loc[avail_idx[-1], ktb_cols]
                    return pd.Series(row.values, index=ktb_avail, dtype=float)

                today_ktb = _ktb_curve_at(TODAY)
                week_ktb  = _ktb_curve_at(TODAY - timedelta(days=7))
                month_ktb = _ktb_curve_at(TODAY - timedelta(days=30))

                fig_ktb = go.Figure()
                if not today_ktb.dropna().empty:
                    fig_ktb.add_trace(go.Scatter(
                        x=ktb_tenor_labels, y=today_ktb.values,
                        mode="lines+markers", name=f"현재 ({TODAY_STR})",
                        line=dict(width=2.5),
                    ))
                if not week_ktb.dropna().empty:
                    fig_ktb.add_trace(go.Scatter(
                        x=ktb_tenor_labels, y=week_ktb.values,
                        mode="lines+markers", name="1주 전",
                        line=dict(dash="dot", width=1.5), opacity=0.8,
                    ))
                if not month_ktb.dropna().empty:
                    fig_ktb.add_trace(go.Scatter(
                        x=ktb_tenor_labels, y=month_ktb.values,
                        mode="lines+markers", name="1개월 전",
                        line=dict(dash="dash", width=1.5), opacity=0.8,
                    ))
                fig_ktb.update_layout(
                    title="국고채 금리 커브",
                    xaxis_title="만기", yaxis_title="수익률 (%)",
                    hovermode="x unified",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                )
                st.plotly_chart(fig_ktb, use_container_width=True)

                ktb_curve_data = {
                    "현재(%)": today_ktb.values,
                    "1W(bp)":  (today_ktb - week_ktb).values * 100,
                    "1M(bp)":  (today_ktb - month_ktb).values * 100,
                }
                ktb_curve_table = pd.DataFrame(
                    ktb_curve_data,
                    index=pd.Index(ktb_tenor_labels, name="만기"),
                )
                ktb_styled = (
                    ktb_curve_table.style
                    .format({"현재(%)": "{:.2f}", "1W(bp)": "{:.1f}", "1M(bp)": "{:.1f}"}, na_rep="-")
                    .map(_color_bp, subset=["1W(bp)", "1M(bp)"])
                    .set_properties(**{"text-align": "center"})
                )
                st.dataframe(ktb_styled, use_container_width=True)

    # ── 장외거래 대표수익률 ──────────────────────────────────────────────────
    with tab_otc:
        st.caption(
            f"Source: KOFIA  ·  기준일: {TODAY_STR}  ·  "
            "최종호가수익률 vs. 장외거래대표수익률  ·  스프레드 = 최종호가 − 장외거래"
        )

        if _bond_df is None or _otc_df is None:
            st.error(
                "국내 채권 금리 또는 장외거래대표수익률 데이터가 없습니다.  \n"
                "로컬 PC에서 `python collect_data.py` 실행 후 `git push` 해주세요."
            )
        else:
            common_cols = [c for c in _otc_df.columns if c in _bond_df.columns]
            if not common_cols:
                st.warning("비교 가능한 공통 종목이 없습니다.")
            else:
                today_bond = TreasuryCalc.get_ref_value(_bond_df, TODAY)
                today_otc  = TreasuryCalc.get_ref_value(_otc_df,  TODAY)

                # ── 시그널 계산 (5Y 통계 기반 Z-score) ────────────────
                _bond_al, _otc_al = _bond_df[common_cols].align(
                    _otc_df[common_cols], join="inner"
                )
                spread_ts = (_bond_al - _otc_al) * 100  # bp 시계열
                mean_5y   = spread_ts.mean()
                std_5y    = spread_ts.std()

                _avail_idx = spread_ts.index[spread_ts.index <= pd.Timestamp(TODAY)]
                if len(_avail_idx) > 0:
                    today_spread_row = spread_ts.loc[_avail_idx[-1]]
                else:
                    today_spread_row = pd.Series(float("nan"), index=common_cols)

                z_scores = (today_spread_row - mean_5y) / std_5y

                def _get_signal(z) -> str:
                    if pd.isna(z):        return "Normal"
                    if abs(z) >= 2.0:     return "Warning"
                    if abs(z) >= 1.5:     return "Caution"
                    return "Normal"

                signal_map = {col: _get_signal(z_scores[col]) for col in common_cols}

                n_warning = sum(1 for s in signal_map.values() if s == "Warning")
                n_caution = sum(1 for s in signal_map.values() if s == "Caution")
                n_normal  = len(common_cols) - n_warning - n_caution

                # ── 요약 배너 ──────────────────────────────────────────
                st.subheader("스프레드 이상 시그널")
                st.caption("5Y 전체 기간 평균·표준편차 기준  ·  |Z| ≥ 1.5σ: Caution  ·  |Z| ≥ 2.0σ: Warning")

                col_w, col_c, col_n = st.columns(3)
                col_w.metric("🚨 Warning", f"{n_warning}종목")
                col_c.metric("⚡ Caution", f"{n_caution}종목")
                col_n.metric("✅ 정상",    f"{n_normal}종목")

                # ── 시그널 종목 상세 테이블 ────────────────────────────
                _SIG_WARNING_BG = "background-color: rgba(255, 75, 75, 0.18)"
                _SIG_CAUTION_BG = "background-color: rgba(255, 165, 0, 0.18)"

                def _fmt_signal(val):
                    if val == "Warning": return "🚨 Warning"
                    if val == "Caution": return "⚡ Caution"
                    return val

                signal_rows: dict = {}
                for col in common_cols:
                    sig = signal_map[col]
                    if sig == "Normal":
                        continue
                    label = BOND_LABELS.get(col, col)
                    z     = z_scores[col]
                    signal_rows[label] = {
                        "현재(bp)":       today_spread_row[col] if pd.notna(today_spread_row[col]) else float("nan"),
                        "5Y평균(bp)":     mean_5y[col],
                        "5Y표준편차(bp)": std_5y[col],
                        "Z-score":        z,
                        "시그널":         sig,
                    }

                if signal_rows:
                    sig_df = pd.DataFrame.from_dict(signal_rows, orient="index")
                    sig_df.index.name = "종목"

                    def _row_signal_style(row):
                        sig = row["시그널"]
                        if sig == "Warning": bg = _SIG_WARNING_BG
                        elif sig == "Caution": bg = _SIG_CAUTION_BG
                        else: bg = ""
                        return [bg] * len(row)

                    sig_styled = (
                        sig_df.style
                        .apply(_row_signal_style, axis=1)
                        .format({
                            "현재(bp)":       "{:+.1f}",
                            "5Y평균(bp)":     "{:+.1f}",
                            "5Y표준편차(bp)": "{:.1f}",
                            "Z-score":        "{:+.2f}",
                            "시그널":         _fmt_signal,
                        }, na_rep="-")
                        .set_properties(**{"text-align": "center"})
                    )
                    st.dataframe(sig_styled, use_container_width=True)
                else:
                    st.success("현재 모든 종목의 스프레드가 정상 범위 내에 있습니다.")

                st.divider()

                # ── 전체 스프레드 비교 테이블 ──────────────────────────
                st.subheader("최종호가 vs. 장외거래 상세")
                rows: dict = {}
                for col in common_cols:
                    label    = BOND_LABELS.get(col, col)
                    bond_val = today_bond[col] if col in today_bond.index else float("nan")
                    otc_val  = today_otc[col]  if col in today_otc.index  else float("nan")
                    spread   = (bond_val - otc_val) * 100 if pd.notna(bond_val) and pd.notna(otc_val) else float("nan")
                    rows[label] = {
                        "최종호가(%)":   bond_val,
                        "장외거래(%)":   otc_val,
                        "스프레드(bp)": spread,
                        "시그널":       signal_map[col],
                    }

                otc_cmp_df = pd.DataFrame.from_dict(rows, orient="index")
                otc_cmp_df.index.name = "종목"

                def _row_signal_style_full(row):
                    sig = row["시그널"]
                    if sig == "Warning": bg = _SIG_WARNING_BG
                    elif sig == "Caution": bg = _SIG_CAUTION_BG
                    else: bg = ""
                    return [bg] * len(row)

                otc_cmp_styled = (
                    otc_cmp_df.style
                    .apply(_row_signal_style_full, axis=1)
                    .format({
                        "최종호가(%)":  "{:.2f}",
                        "장외거래(%)":  "{:.2f}",
                        "스프레드(bp)": "{:.1f}",
                        "시그널":       _fmt_signal,
                    }, na_rep="-")
                    .map(_color_bp, subset=["스프레드(bp)"])
                    .set_properties(**{"text-align": "center"})
                )
                st.dataframe(otc_cmp_styled, use_container_width=True)

    # ── Raw Data ─────────────────────────────────────────────────────────────
    with tab_raw:
        raw1, raw2, raw3 = st.tabs(["글로벌 국채 금리", "국내 채권 금리", "장외거래 대표수익률"])

        # ── 글로벌 국채 금리 raw ─────────────────────────────────────────────
        with raw1:
            st.caption(
                "글로벌 국채 금리 + KOFIA KR 금리 병합  ·  "
                "주말·공휴일은 직전 거래일 값으로 채워짐 (forward fill)"
            )
            if _merged_df is None:
                st.info("데이터를 불러오지 못했습니다.")
            else:
                all_cols = _merged_df.columns.tolist()
                selected = st.multiselect(
                    "표시할 시리즈",
                    options=all_cols,
                    default=[c for c in ["US_10Y", "DE_10Y", "JP_10Y", "KR_10Y"] if c in all_cols],
                    key="m_cols",
                )
                if selected:
                    df_melt = _merged_df[selected].reset_index().melt(
                        id_vars="Date", var_name="Series", value_name="Yield (%)"
                    )
                    fig_m = px.line(df_melt, x="Date", y="Yield (%)", color="Series",
                                    title="글로벌 + KR 국채 금리")
                    fig_m.update_layout(hovermode="x unified")
                    st.plotly_chart(fig_m, use_container_width=True)

                df_m_display = _merged_df.copy()
                df_m_display.index = df_m_display.index.strftime("%Y-%m-%d")
                st.dataframe(
                    df_m_display.style.format("{:.2f}", na_rep="-").set_properties(**{"text-align": "center"}),
                    use_container_width=True,
                )

        # ── 국내 채권 금리 raw ───────────────────────────────────────────────
        with raw2:
            st.caption(
                "KOFIA 전종목 최종호가수익률  ·  "
                "주말·공휴일은 직전 거래일 값으로 채워짐 (forward fill)"
            )
            if _bond_df is None:
                st.info("데이터를 불러오지 못했습니다.")
            else:
                bond_all_cols     = _bond_df.columns.tolist()
                bond_default_cols = [c for c in ["KTB_10Y", "KTB_3Y", "CORP_AA_3Y", "CD_91D"] if c in bond_all_cols]
                if not bond_default_cols:
                    bond_default_cols = bond_all_cols[:4]

                bond_selected = st.multiselect(
                    "표시할 시리즈",
                    options=bond_all_cols,
                    format_func=lambda x: f"{BOND_LABELS.get(x, x)} ({x})",
                    default=bond_default_cols,
                    key="bond_cols",
                )
                if bond_selected:
                    df_bond_melt = _bond_df[bond_selected].reset_index().melt(
                        id_vars="Date", var_name="Series", value_name="Yield (%)"
                    )
                    df_bond_melt["Series"] = df_bond_melt["Series"].map(
                        lambda x: f"{BOND_LABELS.get(x, x)} ({x})"
                    )
                    fig_bond = px.line(df_bond_melt, x="Date", y="Yield (%)", color="Series",
                                       title="국내 채권 금리 시계열")
                    fig_bond.update_layout(hovermode="x unified")
                    st.plotly_chart(fig_bond, use_container_width=True)

                df_bond_display = _bond_df.copy()
                df_bond_display.index   = df_bond_display.index.strftime("%Y-%m-%d")
                df_bond_display.columns = [f"{BOND_LABELS.get(c, c)} ({c})" for c in df_bond_display.columns]
                st.dataframe(
                    df_bond_display.style.format("{:.2f}", na_rep="-").set_properties(**{"text-align": "center"}),
                    use_container_width=True,
                )

        # ── 장외 거래 대표수익률 raw ─────────────────────────────────────────
        with raw3:
            st.caption(
                "KOFIA 장외거래대표수익률  ·  "
                "주말·공휴일은 직전 거래일 값으로 채워짐 (forward fill)"
            )
            if _otc_df is None:
                st.info("데이터를 불러오지 못했습니다.")
            else:
                otc_all_cols     = _otc_df.columns.tolist()
                otc_default_cols = [c for c in ["KTB_10Y", "KTB_3Y", "KEPCO_3Y", "CORP_AA_3Y"] if c in otc_all_cols]
                if not otc_default_cols:
                    otc_default_cols = otc_all_cols[:4]

                otc_selected = st.multiselect(
                    "표시할 시리즈",
                    options=otc_all_cols,
                    format_func=lambda x: f"{BOND_LABELS.get(x, x)} ({x})",
                    default=otc_default_cols,
                    key="otc_cols",
                )
                if otc_selected:
                    df_otc_melt = _otc_df[otc_selected].reset_index().melt(
                        id_vars="Date", var_name="Series", value_name="Yield (%)"
                    )
                    df_otc_melt["Series"] = df_otc_melt["Series"].map(
                        lambda x: f"{BOND_LABELS.get(x, x)} ({x})"
                    )
                    fig_otc = px.line(df_otc_melt, x="Date", y="Yield (%)", color="Series",
                                      title="장외거래대표수익률 시계열")
                    fig_otc.update_layout(hovermode="x unified")
                    st.plotly_chart(fig_otc, use_container_width=True)

                df_otc_display = _otc_df.copy()
                df_otc_display.index   = df_otc_display.index.strftime("%Y-%m-%d")
                df_otc_display.columns = [f"{BOND_LABELS.get(c, c)} ({c})" for c in df_otc_display.columns]
                st.dataframe(
                    df_otc_display.style.format("{:.2f}", na_rep="-").set_properties(**{"text-align": "center"}),
                    use_container_width=True,
                )


# ══════════════════════════════════════════════════════════════════════════════
# 주식 (준비 중)
# ══════════════════════════════════════════════════════════════════════════════

elif asset_class == "주식":
    st.info("주식 데이터는 준비 중입니다.")
