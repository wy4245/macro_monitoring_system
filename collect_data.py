"""
데이터 수집 스크립트 — 로컬에서 실행 후 git push

저장 구조:
  D:\MMS_cache\mms.db     — DuckDB 전체 이력 (외장 SSD, git 미추적)
  data/*.parquet          — 최근 5년 슬라이스 (git push → Streamlit 서버)

경로 오버라이드:
  set MMS_CACHE_DIR=E:\other_cache
  python collect_data.py

사용법:
    python collect_data.py

완료 후 git push:
    git add data/
    git commit -m "데이터 업데이트 YYYYMMDD"
    git push
"""

import os
import sys
import duckdb
import pandas as pd
from datetime import date, timedelta
from pathlib import Path

_root = Path(__file__).resolve().parent
sys.path.insert(0, str(_root))

from modules.collector.kofia import BondSummary, BondSummary_OTC, BondFutures, individual_bond
from modules.collector.investing import GlobalTreasury
from modules.calculator.kofia import KofiaCalc

# ─── 경로 설정 ─────────────────────────────────────────────────────────────────

CACHE_DIR = Path(os.environ.get("MMS_CACHE_DIR", "D:/MMS_cache"))
GIT_DIR   = _root / "data"

CACHE_DIR.mkdir(parents=True, exist_ok=True)
GIT_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH      = CACHE_DIR / "mms.db"
EXPORT_YEARS = 5  # Streamlit 서버용 parquet 슬라이스 기간

# ─── 기준일 ────────────────────────────────────────────────────────────────────

end_date   = date.today() - timedelta(days=1)
end_str    = end_date.strftime("%Y-%m-%d")
target_str = end_date.strftime("%Y%m%d")

print(f"[기준일] {end_str}")
print(f"[캐시 DB] {DB_PATH}")
print()


# ─── DuckDB 헬퍼 ───────────────────────────────────────────────────────────────

def _open_db() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH))


def _table_exists(conn, table: str) -> bool:
    result = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [table]
    ).fetchone()
    return result[0] > 0


def _last_date(conn, table: str) -> date | None:
    """테이블의 가장 최근 Date를 반환합니다."""
    if not _table_exists(conn, table):
        return None
    try:
        result = conn.execute(f"SELECT MAX(Date) FROM {table}").fetchone()
        if result and result[0]:
            return pd.Timestamp(result[0]).date()
    except Exception as e:
        print(f"  [날짜 조회 오류] {table}: {e}")
    return None


def _upsert(conn, table: str, df: pd.DataFrame) -> None:
    """
    DatetimeIndex DataFrame을 DuckDB 테이블에 upsert합니다.
    새 데이터가 우선하되, 새 데이터의 NaN은 기존 값으로 보존합니다.
    (부분 수집으로 일부 컬럼이 없을 때 기존 데이터를 덮어쓰지 않음)
    """
    new_data = df.reset_index()
    new_data["Date"] = pd.to_datetime(new_data["Date"])

    if not _table_exists(conn, table):
        conn.register("_staging", new_data)
        conn.execute(f"CREATE TABLE {table} AS SELECT * FROM _staging ORDER BY Date")
        conn.unregister("_staging")
    else:
        existing = conn.execute(f"SELECT * FROM {table}").df()
        existing["Date"] = pd.to_datetime(existing["Date"])
        existing = existing.set_index("Date")
        # 새 데이터 우선, NaN 또는 누락 컬럼은 기존 값 보존
        merged = df.combine_first(existing).sort_index()
        merged_reset = merged.reset_index()
        merged_reset["Date"] = pd.to_datetime(merged_reset["Date"])
        conn.register("_merged", merged_reset)
        conn.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM _merged ORDER BY Date")
        conn.unregister("_merged")


def _gt_gap_countries(conn, ref_date: date, max_lag_days: int = 5) -> list[str]:
    """
    global_treasury 테이블에서 마지막 유효 데이터가 ref_date 기준 max_lag_days일 이상
    오래된 국가 목록을 반환합니다. 주말·공휴일을 감안해 기본 5일로 설정합니다.
    """
    if not _table_exists(conn, "global_treasury"):
        return []

    check_from = (ref_date - timedelta(days=60)).strftime("%Y-%m-%d")
    df = conn.execute(
        f"SELECT * FROM global_treasury WHERE Date >= '{check_from}' ORDER BY Date"
    ).df()
    if df.empty:
        return list(GlobalTreasury.BOND_SLUGS.keys())

    df = df.set_index("Date")
    df.index = pd.to_datetime(df.index)

    gap_countries = []
    for country in GlobalTreasury.BOND_SLUGS:
        cols = [c for c in df.columns if c.startswith(f"{country}_")]
        if not cols:
            print(f"  [갭] {country}: 컬럼 없음")
            gap_countries.append(country)
            continue
        valid = df[cols].dropna(how="all")
        if valid.empty:
            print(f"  [갭] {country}: 데이터 없음")
            gap_countries.append(country)
            continue
        last_valid = valid.index.max().date()
        lag = (ref_date - last_valid).days
        if lag > max_lag_days:
            print(f"  [갭] {country}: 마지막 유효 데이터 {last_valid} (lag {lag}일) → 재수집 필요")
            gap_countries.append(country)

    return gap_countries


def _export_parquet(conn, table: str) -> None:
    """DuckDB 테이블에서 최근 EXPORT_YEARS년치를 data/{table}.parquet으로 내보냅니다."""
    try:
        cutoff = end_date.replace(year=end_date.year - EXPORT_YEARS)
    except ValueError:
        cutoff = end_date - timedelta(days=365 * EXPORT_YEARS)

    df = conn.execute(
        f"SELECT * FROM {table} WHERE Date >= '{cutoff}' ORDER BY Date"
    ).df()
    df = df.set_index("Date")
    df.index = pd.to_datetime(df.index)
    df.index.name = "Date"

    out = GIT_DIR / f"{table}.parquet"
    df.to_parquet(out)
    print(f"  [export] → {out.name}  ({len(df)}행, {out.stat().st_size // 1024} KB)")


# ─── 마이그레이션: 기존 CSV/parquet → DuckDB (최초 1회) ───────────────────────

def _migrate_legacy(conn) -> None:
    """기존 data/*.csv와 data/individual_bonds/*.parquet을 DuckDB로 이전합니다."""
    print("─── 레거시 데이터 마이그레이션 ─────────────────────────────")

    csv_tables = {
        "bond_summary":    GIT_DIR / "bond_summary.csv",
        "global_treasury": GIT_DIR / "global_treasury.csv",
        "otc_summary":     GIT_DIR / "otc_summary.csv",
        "bond_futures":    GIT_DIR / "bond_futures.csv",
    }
    for table, csv_path in csv_tables.items():
        if csv_path.exists() and not _table_exists(conn, table):
            try:
                df = pd.read_csv(
                    csv_path, index_col="Date", parse_dates=True, encoding="utf-8-sig"
                )
                _upsert(conn, table, df)
                rows = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                print(f"  [마이그레이션] {csv_path.name} → {table}  ({rows}행)")
            except Exception as e:
                print(f"  [마이그레이션 오류] {csv_path.name}: {e}")

    # individual_bonds: 연도별 parquet → DuckDB
    legacy_ib_dir = GIT_DIR / "individual_bonds"
    if legacy_ib_dir.exists() and not _table_exists(conn, "individual_bonds"):
        parquets = sorted(legacy_ib_dir.glob("*.parquet"))
        if parquets:
            dfs = []
            for f in parquets:
                try:
                    dfs.append(pd.read_parquet(f))
                except Exception as e:
                    print(f"  [마이그레이션 오류] {f.name}: {e}")
            if dfs:
                combined = pd.concat(dfs, ignore_index=True)
                combined["Date"] = pd.to_datetime(combined["Date"])
                conn.register("_ib_legacy", combined)
                conn.execute(
                    "CREATE TABLE individual_bonds AS SELECT * FROM _ib_legacy ORDER BY Date"
                )
                conn.unregister("_ib_legacy")
                rows = conn.execute("SELECT COUNT(*) FROM individual_bonds").fetchone()[0]
                print(f"  [마이그레이션] individual_bonds  ({rows}행, {len(parquets)}개 파일)")

    print()


# ══════════════════════════════════════════════════════════════════════════════
# 메인
# ══════════════════════════════════════════════════════════════════════════════

conn = _open_db()

# 최초 실행 시 레거시 CSV/parquet → DuckDB 마이그레이션
existing_tables = conn.execute("SHOW TABLES").df()
if existing_tables.empty:
    _migrate_legacy(conn)


# ── 1. KOFIA BondSummary ──────────────────────────────────────────────────────

print("=" * 60)
print("1. KOFIA BondSummary")
print("=" * 60)

bs_last = _last_date(conn, "bond_summary")
if bs_last:
    bs_start_dt = bs_last + timedelta(days=1)
else:
    try:
        bs_start_dt = end_date.replace(year=end_date.year - 5)
    except ValueError:
        bs_start_dt = end_date - timedelta(days=365 * 5)

bs_start_str = bs_start_dt.strftime("%Y-%m-%d")
print(f"  기간: {bs_start_str} ~ {end_str}")

if bs_start_dt > end_date:
    print("  [완료] 이미 최신 데이터")
    _export_parquet(conn, "bond_summary")
else:
    df_bond = BondSummary().collect(start_date=bs_start_str, end_date=end_str)
    if df_bond is not None:
        try:
            df_std = KofiaCalc.standardize_bond(df_bond)
            _upsert(conn, "bond_summary", df_std)
            rows = conn.execute("SELECT COUNT(*) FROM bond_summary").fetchone()[0]
            print(f"  [저장] DuckDB bond_summary  (총 {rows}행)")
            _export_parquet(conn, "bond_summary")
        except Exception as e:
            print(f"  [오류] {e}")
    else:
        print("  [실패] 기존 데이터 유지")

print()


# ── 2. investing.com GlobalTreasury ──────────────────────────────────────────

print("=" * 60)
print("2. investing.com GlobalTreasury")
print("=" * 60)

gt_last = _last_date(conn, "global_treasury")
if gt_last:
    gt_start_dt = gt_last + timedelta(days=1)
else:
    try:
        gt_start_dt = end_date.replace(year=end_date.year - 5)
    except ValueError:
        gt_start_dt = end_date - timedelta(days=365 * 5)

gt_start_str = gt_start_dt.strftime("%Y-%m-%d")
print(f"  기간: {gt_start_str} ~ {end_str}")

if gt_start_dt > end_date:
    print("  [완료] 이미 최신 데이터")
else:
    df_g = GlobalTreasury().collect(start_date=gt_start_str, end_date=end_str)
    if df_g is not None:
        _upsert(conn, "global_treasury", df_g)
        rows = conn.execute("SELECT COUNT(*) FROM global_treasury").fetchone()[0]
        print(f"  [저장] DuckDB global_treasury  (총 {rows}행)")
    else:
        print("  [실패] 기존 데이터 유지")

# 국가별 갭 체크 (이미 최신이거나 신규 수집 후 모두 실행)
gap_countries = _gt_gap_countries(conn, end_date)
if gap_countries:
    gap_start = (end_date - timedelta(days=30)).strftime("%Y-%m-%d")
    print(f"  [재수집] {gap_countries} | {gap_start} ~ {end_str}")
    df_refill = GlobalTreasury().collect(
        start_date=gap_start,
        end_date=end_str,
        countries=gap_countries,
    )
    if df_refill is not None:
        _upsert(conn, "global_treasury", df_refill)
        rows = conn.execute("SELECT COUNT(*) FROM global_treasury").fetchone()[0]
        print(f"  [저장] DuckDB global_treasury  (총 {rows}행)")
    else:
        print(f"  [경고] 재수집 실패 — 기존 데이터 유지")
else:
    print("  [갭 없음] 모든 국가 데이터 정상")

_export_parquet(conn, "global_treasury")

print()


# ── 3. KOFIA 장외거래대표수익률 ───────────────────────────────────────────────

print("=" * 60)
print("3. KOFIA 장외거래대표수익률")
print("=" * 60)

otc_last = _last_date(conn, "otc_summary")
if otc_last:
    otc_start_dt = otc_last + timedelta(days=1)
else:
    try:
        otc_start_dt = end_date.replace(year=end_date.year - 5)
    except ValueError:
        otc_start_dt = end_date - timedelta(days=365 * 5)

otc_start_str = otc_start_dt.strftime("%Y-%m-%d")
print(f"  기간: {otc_start_str} ~ {end_str}")

if otc_start_dt > end_date:
    print("  [완료] 이미 최신 데이터")
    _export_parquet(conn, "otc_summary")
else:
    df_otc = BondSummary_OTC().collect(start_date=otc_start_str, end_date=end_str)
    if df_otc is not None:
        try:
            df_std = KofiaCalc.standardize_otc(df_otc)
            _upsert(conn, "otc_summary", df_std)
            rows = conn.execute("SELECT COUNT(*) FROM otc_summary").fetchone()[0]
            print(f"  [저장] DuckDB otc_summary  (총 {rows}행)")
            _export_parquet(conn, "otc_summary")
        except Exception as e:
            print(f"  [오류] {e}")
    else:
        print("  [실패] 기존 데이터 유지")

print()


# ── 4. KOFIA 국채선물수익률 ───────────────────────────────────────────────────

print("=" * 60)
print("4. KOFIA 국채선물수익률")
print("=" * 60)

bf_last = _last_date(conn, "bond_futures")
bf_start_dt = (bf_last + timedelta(days=1)) if bf_last else date(end_date.year - 5, 1, 1)
bf_start_str = bf_start_dt.strftime("%Y-%m-%d")
print(f"  기간: {bf_start_str} ~ {end_str}")

if bf_start_dt > end_date:
    print("  [완료] 이미 최신 데이터")
    _export_parquet(conn, "bond_futures")
else:
    df_futures = BondFutures().collect(start_date=bf_start_str, end_date=end_str)
    if df_futures is not None:
        try:
            df_futures = df_futures.set_index("Date")
            df_futures.index = pd.to_datetime(df_futures.index)
            df_futures.index.name = "Date"
            _upsert(conn, "bond_futures", df_futures)
            rows = conn.execute("SELECT COUNT(*) FROM bond_futures").fetchone()[0]
            print(f"  [저장] DuckDB bond_futures  (총 {rows}행)")
            _export_parquet(conn, "bond_futures")
        except Exception as e:
            print(f"  [오류] {e}")
    else:
        print("  [실패] 기존 데이터 유지")

print()


# ── 5. KOFIA 실시간 체결정보 일자별 거래현황 ──────────────────────────────────

print("=" * 60)
print("5. KOFIA 실시간 체결정보 일자별 거래현황")
print("=" * 60)

ib_last = _last_date(conn, "individual_bonds")
ib_start_dt  = (ib_last + timedelta(days=1)) if ib_last else date(end_date.year - 5, 1, 1)
ib_start_str = ib_start_dt.strftime("%Y-%m-%d")
print(f"  기간: {ib_start_str} ~ {end_str}")

if ib_start_dt > end_date:
    print("  [완료] 이미 최신 데이터")
else:
    df_ib = individual_bond().collect(start_date=ib_start_str, end_date=end_str)
    if df_ib is not None:
        df_ib = df_ib.copy()
        df_ib["Date"] = pd.to_datetime(df_ib["Date"])
        # object 컬럼 수치 변환 (parquet 타입 오류 방지)
        for col in df_ib.select_dtypes(include="object").columns:
            if col == "Date":
                continue
            converted = pd.to_numeric(df_ib[col], errors="coerce")
            if converted.notna().sum() > 0:
                df_ib[col] = converted

        conn.register("_ib_new", df_ib)
        if not _table_exists(conn, "individual_bonds"):
            conn.execute(
                "CREATE TABLE individual_bonds AS SELECT * FROM _ib_new ORDER BY Date"
            )
        else:
            # 날짜 단위 교체: 해당 날짜 기존 데이터 삭제 후 삽입
            conn.execute(
                "DELETE FROM individual_bonds WHERE Date IN (SELECT DISTINCT Date FROM _ib_new)"
            )
            conn.execute("INSERT INTO individual_bonds SELECT * FROM _ib_new")
        conn.unregister("_ib_new")

        rows = conn.execute("SELECT COUNT(*) FROM individual_bonds").fetchone()[0]
        print(f"  [저장] DuckDB individual_bonds  (총 {rows}행)")
    else:
        print("  [실패] 기존 데이터 유지")

print()

conn.close()

# ─── 완료 안내 ─────────────────────────────────────────────────────────────────

print("=" * 60)
print("수집 완료. 아래 명령어로 GitHub에 push하세요:")
print()
print("  git add data/")
print(f'  git commit -m "데이터 업데이트 {target_str}"')
print("  git push")
print("=" * 60)
