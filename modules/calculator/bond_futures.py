import re
import pandas as pd
from pathlib import Path


def parse_bond_code(col: str) -> dict | None:
    """선물/현물 컬럼명에서 채권 코드와 메타데이터를 파싱합니다."""
    m = re.search(r'(\d{5})-(\d{4})\((\d{2})-(\d+)\)', col)
    if not m:
        return None
    mat_yymm  = m.group(2)
    mat_year  = 2000 + int(mat_yymm[:2])
    iss_year  = 2000 + int(m.group(3))
    year_diff = mat_year - iss_year
    return {
        "code":      m.group(0),
        "coupon":    int(m.group(1)) / 10000,
        "mat_year":  mat_year,
        "mat_month": int(mat_yymm[2:]),
        "iss_year":  iss_year,
        "series":    m.group(4),
        "year_diff": year_diff,
        "tenor":     classify_tenor(year_diff),
    }


def classify_tenor(year_diff: int) -> str:
    if year_diff <= 4:   return "3Y"
    if year_diff <= 7:   return "5Y"
    if year_diff <= 15:  return "10Y"
    if year_diff <= 25:  return "20Y"
    if year_diff <= 40:  return "30Y"
    return "50Y"


def load_individual_bonds_ktb(data_dir: str, bf_codes: set) -> pd.DataFrame:
    """individual_bonds parquet에서 bond_futures와 매칭되는 국고채 행만 추출합니다.

    반환: Date, code, yield_pct 컬럼 DataFrame
    """
    ib_dir = Path(data_dir) / "individual_bonds"
    if not ib_dir.exists():
        return pd.DataFrame(columns=["Date", "code", "yield_pct"])

    frames = []
    for f in sorted(ib_dir.glob("*.parquet")):
        df = pd.read_parquet(f)
        if df.empty:
            continue
        date_col  = df.columns[0]   # Date
        name_col  = df.columns[3]   # 종목명
        yield_col = df.columns[6]   # 평균 수익률

        codes = df[name_col].apply(lambda n: _extract_code(str(n)))
        mask  = codes.isin(bf_codes)
        if not mask.any():
            continue

        sub = df.loc[mask, [date_col, yield_col]].copy()
        sub.columns = ["Date", "yield_pct"]
        sub["code"] = codes[mask].values
        frames.append(sub)

    if not frames:
        return pd.DataFrame(columns=["Date", "code", "yield_pct"])

    result = pd.concat(frames, ignore_index=True)
    result["Date"]      = pd.to_datetime(result["Date"])
    result["yield_pct"] = pd.to_numeric(result["yield_pct"], errors="coerce")
    return result.dropna(subset=["yield_pct"])


def _extract_code(name: str) -> str | None:
    m = re.search(r'(\d{5}-\d{4}\(\d{2}-\d+\))', name)
    return m.group(1) if m else None


def build_spread_df(bf_df: pd.DataFrame, ib_df: pd.DataFrame) -> pd.DataFrame:
    """선물·현물 수익률로 종목별 일별 스프레드 DataFrame을 구성합니다.

    반환: Date, code, tenor, futures_yield, spot_yield, spread_bp 컬럼 DataFrame
    """
    if ib_df.empty:
        return pd.DataFrame(columns=["Date", "code", "tenor", "futures_yield", "spot_yield", "spread_bp"])

    col_info = {}
    for col in bf_df.columns:
        info = parse_bond_code(col)
        if info:
            col_info[col] = info

    # 선물 wide → long
    bf_reset = bf_df.reset_index()
    bf_long  = bf_reset.melt(id_vars="Date", var_name="col", value_name="futures_yield")
    bf_long  = bf_long.dropna(subset=["futures_yield"])
    bf_long  = bf_long[bf_long["col"].isin(col_info)].copy()
    bf_long["code"]  = bf_long["col"].map(lambda c: col_info[c]["code"])
    bf_long["tenor"] = bf_long["col"].map(lambda c: col_info[c]["tenor"])
    bf_long = bf_long[["Date", "code", "tenor", "futures_yield"]]
    bf_long["Date"] = pd.to_datetime(bf_long["Date"])

    # 현물: Date+code별 평균 수익률
    ib_agg = (
        ib_df.groupby(["Date", "code"])["yield_pct"]
        .mean()
        .reset_index()
        .rename(columns={"yield_pct": "spot_yield"})
    )

    merged = pd.merge(bf_long, ib_agg, on=["Date", "code"], how="inner")
    merged["spread_bp"] = (merged["spot_yield"] - merged["futures_yield"]) * 100
    return merged.sort_values(["code", "Date"]).reset_index(drop=True)


def calc_zscore(spread_df: pd.DataFrame, active_codes: list, target_date) -> pd.DataFrame:
    """테너별 pooling Z-score와 현재 스프레드를 계산합니다.

    반환: code, tenor, futures_yield, spot_yield, spread_bp,
          spread_mean, spread_std, z_score, data_date 컬럼 DataFrame
    """
    target_date = pd.Timestamp(target_date)
    results = []

    for code in active_codes:
        code_rows = spread_df[spread_df["code"] == code]
        if code_rows.empty:
            continue
        tenor = code_rows["tenor"].iloc[0]

        # 같은 테너 전체 스프레드 pooling (5Y 역사)
        hist = spread_df[spread_df["tenor"] == tenor]["spread_bp"].dropna()
        mean = hist.mean()
        std  = hist.std()

        # target_date 이하 최신 행
        past = code_rows[code_rows["Date"] <= target_date].sort_values("Date")
        if past.empty:
            continue
        row = past.iloc[-1]

        zscore = (
            (row["spread_bp"] - mean) / std
            if (std > 0 and len(hist) >= 200)
            else float("nan")
        )

        results.append({
            "code":          code,
            "tenor":         tenor,
            "futures_yield": row["futures_yield"],
            "spot_yield":    row["spot_yield"],
            "spread_bp":     row["spread_bp"],
            "spread_mean":   mean,
            "spread_std":    std,
            "z_score":       zscore,
            "data_date":     row["Date"].date(),
        })

    return pd.DataFrame(results)
