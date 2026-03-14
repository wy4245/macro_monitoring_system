# MMS 개발 계획

> 목표: 데이터 소스를 하나씩 추가하면서 Streamlit 탭을 순차적으로 확장한다.
> 기준일: 2026-03-04 (최종 수정: 2026-03-14)

---

## 프로젝트 구조 (현황)

```
MMS/
├── collect_data.py          # 로컬 실행 → DuckDB upsert → parquet export → git push
├── main.py                  # Streamlit 대시보드 (표시 전용, 수집 기능 없음)
├── auto_collect.bat         # [작성 완료] 자동 수집 스케줄러 (작업 스케줄러 등록 필요)
├── requirements.txt
│
├── modules/
│   ├── collector/
│   │   ├── kofia.py         # TreasurySummary, BondSummary, BondSummary_OTC,
│   │   │                    #   BondFutures, individual_bond
│   │   └── investing.py     # GlobalTreasury
│   ├── calculator/
│   │   ├── kofia.py         # KofiaCalc (standardize, standardize_bond, standardize_otc, fill_calendar)
│   │   └── global_treasury.py  # TreasuryCalc (merge, get_ref_value, build_change_summary, fill_calendar)
│   └── debug_frames.py
│
├── data/                    # git 추적 (Streamlit 서버용)
│   ├── global_treasury.parquet  # investing.com 글로벌 국채 (최근 5년)
│   ├── bond_summary.parquet     # KOFIA 최종호가수익률 (최근 5년)
│   ├── otc_summary.parquet      # KOFIA 장외거래대표수익률 (최근 5년)
│   ├── bond_futures.parquet     # KOFIA 국채선물수익률 (최근 5년) [탭 미구현]
│   ├── global_treasury.csv      # 레거시 (DuckDB 마이그레이션 완료 후 제거 예정)
│   ├── bond_summary.csv         # 레거시
│   ├── otc_summary.csv          # 레거시
│   ├── bond_futures.csv         # 레거시
│   ├── collect_log.txt          # .gitignore 권장
│   └── tmp/                     # Selenium 임시 다운로드 (.gitignore)
│
└── D:\MMS_cache\mms.db      # DuckDB 전체 이력 (외장 SSD, git 미추적)
    # tables: bond_summary, global_treasury, otc_summary, bond_futures, individual_bonds
```

**⚠️ TreasurySummary 클래스**: `modules/collector/kofia.py`에 존재하지만 `collect_data.py`에서 사용하지 않음. BondSummary가 그 역할을 대체. 제거 대상.

**⚠️ individual_bonds 탭**: 데이터 수집 완료(DuckDB), Streamlit 탭은 미구현.

---

---

## 표시 포맷 기준 (전 탭 공통)

| 단위 | 소수점 | 포맷 문자열 | 적용 열 예시 |
|------|--------|-------------|--------------|
| **%** (수익률) | **1자리** | `{:.1f}` | 금리 (%), 현재(%), 최종호가(%), 장외거래(%) |
| **bp** (변화량) | **3자리** | `{:.3f}` | 1D, 1W, MTD, MoM, YTD, YoY, 스프레드(bp) |

> 이 기준을 벗어난 포맷이 발견되면 즉시 수정할 것.

---

## 데이터 흐름

```
[로컬 PC]
collect_data.py 실행
  ├─ 1. BondSummary        → data/bond_summary.csv
  ├─ 2. GlobalTreasury     → data/global_treasury.csv
  ├─ 3. BondSummary_OTC    → data/otc_summary.csv
  ├─ 4. BondFutures        → data/bond_futures.csv
  └─ 5. individual_bond    → data/individual_bonds/{year}.parquet
        ↓ git push
[Streamlit 서버]
main.py 실행 (파일 읽기 전용)
  └─ 탭별 시각화
```

증분 업데이트 원칙:
- 기존 CSV 로드 → 마지막 날짜 확인 → `last_date + 1` 부터 수집
- 수집 실패 시 기존 데이터 보존 (덮어쓰기 없음)
- `pd.concat` + `drop_duplicates(keep='last')` 로 병합

---

## 탭 추가 패턴

새 데이터 소스를 추가할 때 반드시 따르는 순서:

### Step 1 — Collector 구현 (`modules/collector/<name>.py`)
```python
class NewCollector:
    def collect(self, start_date: str, end_date: str) -> pd.DataFrame | None:
        ...  # 반환: Date 컬럼 포함 DataFrame (저장 없음)
```

### Step 2 — Calculator 구현 (필요 시, `modules/calculator/<name>.py`)
```python
class NewCalc:
    @staticmethod
    def standardize(df) -> pd.DataFrame:
        ...  # DatetimeIndex + 컬럼 표준화
```

### Step 3 — `collect_data.py` 증분 수집 섹션 추가
```python
# ── N. 새 데이터 소스 ───────────────────────
NEW_CSV = RAW_DIR / "new_data.csv"
existing = _load_csv(NEW_CSV)
last     = _last_date(existing)
start_dt = last + timedelta(days=1) if last else <최초 시작일>
if start_dt <= end_date:
    df_new = NewCollector().collect(str(start_dt), end_str)
    if df_new is not None:
        df_std = NewCalc.standardize(df_new)
        _merge_save(existing, df_std, NEW_CSV)
```

### Step 4 — `main.py` 데이터 로드 함수 추가
```python
def _load_new() -> pd.DataFrame | None:
    path = os.path.join("data", "new_data.csv")
    if not os.path.exists(path): return None
    return pd.read_csv(path, index_col=0, parse_dates=True)

_new_df = _load_new()
```

### Step 5 — `main.py` 탭 목록에 추가
```python
# 기존
tab_a, tab_b, ..., tab_raw = st.tabs(["탭A", "탭B", ..., "Raw Data"])

# 변경 (Raw Data는 항상 마지막)
tab_a, tab_b, ..., tab_new, tab_raw = st.tabs(["탭A", "탭B", ..., "새탭", "Raw Data"])
```

### Step 6 — `with tab_new:` 블록 구현
- 데이터 없을 때 `st.error()` 안내
- 요약 테이블 (`st.dataframe`)
- 차트 (`st.plotly_chart`)
- Raw Data 탭에 서브탭 추가

---

## 현재 탭 구성 (main.py 기준)

```
사이드바: [🔄 데이터 최신화 버튼] [채권] [주식]

채권 탭:
  ├─ [글로벌 국채 금리]        ✅ 구현 완료
  │     - 주요국 금리 요약 테이블 (1D/1W/MTD/MoM/YTD/YoY bp, 소수점 3자리)
  │     - 국가별 Yield Curve 차트 (현재/1W/1M)
  │     - KR 열: bond_summary에서 KTB_nY → KR_nY 변환 후 병합
  │
  ├─ [국내 채권 금리]          ✅ 구현 완료
  │     - 전종목 금리 요약 테이블 (금리 % 소수점 1자리, 변화 bp 소수점 3자리)
  │     - 국고채 Yield Curve 차트
  │     ⚠️ KTB 단기물 역사 데이터 공백 → MTD/MoM/YTD/YoY가 '-'로 표시됨
  │
  ├─ [장외거래 대표수익률]     ✅ 구현 완료
  │     - OTC 스프레드 시그널 (Z-score, Warning/Caution/정상)
  │     - 최종호가 vs. 장외거래 상세 비교 테이블
  │
  ├─ [국채선물수익률]          ⬜ 미구현 (데이터: bond_futures.parquet ✅)
  ├─ [개별 채권 거래]          ⬜ 미구현 (데이터: DuckDB individual_bonds ✅)
  └─ [Raw Data]                ✅ 구현 완료 (글로벌/국내/OTC 서브탭)
```

---

## 작업 목록

### 즉시 처리 (버그·품질)

- [ ] **BondSummary 역사 데이터 공백 보완** (2026-03-14 확인)
  - 문제: `bond_summary.parquet` (및 원본 CSV)에서 KTB 단기물(1/2/3/5년)의 역사 데이터가 거의 없음
    - KTB_2Y: 1825행 중 단 7행만 유효 (2026-03-05 ~ 2026-03-13)
    - KTB_10Y·20Y: 2021-03-02 단 2행 + 2026-03-03 이후만 존재
    - CORP_AA_3Y, CD_91D 등 Batch C: 전 기간 정상
  - 원인: Batch A(국고채 단기물) 체크박스 토글 로직이 KOFIA 페이지 초기 상태와 불일치하여 오랫동안 무음 실패
  - 현황: 현재(2026-03-05~)는 정상 수집 중 — 역사 데이터만 공백
  - 해결: 역사 데이터 재수집 필요. `collect_data.py`의 `bs_start_dt`를 수동 조기화해 과거 구간 수집
    - KOFIA는 1회 조회 최대 기간 제한 있으므로 분기별 청크로 나눠 수집
  - 영향: `main.py`의 국내 채권 탭에서 KTB 단기물의 MTD/MoM/YTD/YoY가 `-`(NaN)으로 표시됨

- [ ] **git pull 새로고침 버튼** ✅ 2026-03-14 구현 완료
  - 사이드바 최상단에 "🔄 데이터 최신화" 버튼 추가
  - 클릭 시 `git pull` 실행 → `st.rerun()`으로 데이터 재로드
  - 이전: Streamlit 서버 reboot 후 직접 접속해야 반영됐음

- [ ] **fill_calendar 경계 gap 수정** (`modules/calculator/kofia.py`)
  - 문제: `standardize_bond`가 신규 배치에만 fill_calendar 적용 → 증분 경계 주말 gap 발생
  - 수정: `standardize_bond`에서 fill_calendar 제거 + `main.py`의 `_load_bond()`, `_load_otc()`에서 로드 후 fill_calendar 적용
  - 영향 파일: `modules/calculator/kofia.py`, `main.py`

- [ ] **자동 수집 설정** (`auto_collect.bat`)
  - 내용: venv 활성화 → `python collect_data.py` → git add/commit/push
  - 실행: Windows 작업 스케줄러, 평일 17:00
  - `.gitignore`에 `data/collect_log.txt`, `data/tmp/` 추가
  - 참고: Cloudflare/KOFIA 제약으로 GitHub Actions 사용 불가, 로컬 실행 필수

---

### 탭 추가 계획 (우선순위 순)

#### ⬜ TAB-1: 국채선물수익률
> 데이터: `data/bond_futures.csv` (수집 완료)
> Collector: `BondFutures` (`modules/collector/kofia.py`)
> 저장: DatetimeIndex + 채권명 컬럼 (NaN 있음, 과거 종목 단종)

구현 내용:
- `_load_futures()` 함수 추가 (`main.py`)
- 요약 테이블: 현재 수익률 + 1D/1W 변화 (bp)
- 차트: 주요 종목 시계열 (현물 국채 10년물과 비교)
- Raw Data 탭에 "국채선물수익률" 서브탭 추가

---

#### ⬜ TAB-2: 개별 채권 거래현황
> 데이터: `data/individual_bonds/{year}.parquet` (수집 완료)
> Collector: `individual_bond` (`modules/collector/kofia.py`)
> 저장: `Date` + 종목코드/수익률/거래량 등, 일별 ~800행

구현 내용:
- `_load_individual_bonds(years)` 함수 추가 (연도 필터로 선택적 로드)
- 날짜 선택 → 해당일 거래 종목 목록 + 수익률/거래량 테이블
- 종목 코드별 수익률 시계열 차트
- 데이터 규모가 크므로 `@st.cache_data(ttl=3600)` 필수

---

### 장기 계획

- [ ] **주식 섹션 구현** (사이드바에 이미 `[주식]` 라디오 버튼 있음, 미구현)
  - 추가할 데이터: KOSPI/S&P500/VIX 등 (별도 Collector 필요)

- [ ] **git 대용량 히스토리 정리**
  - `git filter-repo` 로 이전 xlsx 파일 히스토리 제거 (선택)

---

## 캐시 경로 & 포맷 개편 (2026-03-14) ✅ 확정

### 목표

1. 외장 SSD(`D:\MMS_cache\`)에 전체 이력을 영구 누적
2. CSV → DuckDB (로컬 레이크) + Parquet 슬라이스 (git/서버) 전환
3. git 저장소 경량화 + "언제든 꺼내 쓰기" 가능

---

### 현황 및 문제점

| 파일 | 크기 | git 추적 | 문제 |
|------|------|----------|------|
| `bond_summary.csv` | 105 KB | ✅ | 타입 없음, 압축 없음 |
| `global_treasury.csv` | 340 KB | ✅ | 타입 없음, 압축 없음 |
| `otc_summary.csv` | 142 KB | ✅ | 타입 없음, 압축 없음 |
| `bond_futures.csv` | 121 KB | ✅ | 타입 없음, 압축 없음 |
| `individual_bonds/*.parquet` | 17 MB | ✅ | 매일 이진 파일 커밋, git 히스토리 비대 |

---

### 확정 아키텍처

```
D:\MMS_cache\
  mms.db                  ← DuckDB (전체 이력, 외장 SSD, git 미추적)
    tables: bond_summary
            global_treasury
            otc_summary
            bond_futures
            individual_bonds

MMS/data/                 ← git 추적, Streamlit 서버용
  bond_summary.parquet    ← 최근 5년 슬라이스
  global_treasury.parquet ← 최근 5년 슬라이스
  otc_summary.parquet     ← 최근 5년 슬라이스
  bond_futures.parquet    ← 최근 5년 슬라이스
  [individual_bonds — 탭 구현 시 전략 별도 결정]
```

**collect_data.py 흐름:**
```
크롤링 → DuckDB upsert (D:\MMS_cache\mms.db)
       → 최근 5년 SELECT → parquet export (data/)
       → git push
```

---

### 포맷 선택 근거

| 포맷 | 판단 | 이유 |
|------|------|------|
| **DuckDB** (로컬 레이크) | ✅ 채택 | 단일 파일, SQL 쿼리, 증분 upsert, 분석 편의 |
| **Parquet** (서버 슬라이스) | ✅ 채택 | 압축 (~83%), 타입 보존, pyarrow 기설치 |
| CSV | ❌ 제거 | 타입 없음, 압축 없음 |
| SQLite | ❌ | 행 지향, 분석 쿼리 느림 |

**DuckDB를 데이터 레이크로 쓰는 이유:**
- `D:\MMS_cache`에 `mms.db` 파일 1개로 5개 테이블 관리
- `duckdb.connect("D:/MMS_cache/mms.db").execute("SELECT * FROM bond_summary WHERE Date > '2024-01-01'").df()` — 날짜 필터 쿼리 즉시 가능
- individual_bonds (~800K행, 계속 증가) 쿼리에 특히 유리
- `pip install duckdb` 한 번으로 끝

---

### 구현 (완료)

#### collect_data.py 핵심 구조

```python
CACHE_DIR = Path(os.environ.get("MMS_CACHE_DIR", "D:/MMS_cache"))
DB_PATH   = CACHE_DIR / "mms.db"
GIT_DIR   = Path(__file__).parent / "data"
EXPORT_YEARS = 5

def _upsert(conn, table, df):
    """DatetimeIndex DataFrame을 DuckDB에 upsert (pandas dedup)."""
    new = df.reset_index(); new["Date"] = pd.to_datetime(new["Date"])
    conn.register("_staging", new)
    if not _table_exists(conn, table):
        conn.execute(f"CREATE TABLE {table} AS SELECT * FROM _staging ORDER BY Date")
    else:
        existing = conn.execute(f"SELECT * FROM {table}").df()
        existing["Date"] = pd.to_datetime(existing["Date"])
        merged = pd.concat([existing.set_index("Date"), df])
        merged = merged[~merged.index.duplicated(keep="last")].sort_index()
        conn.register("_merged", merged.reset_index())
        conn.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM _merged ORDER BY Date")

def _export_parquet(conn, table):
    """최근 5년치를 data/{table}.parquet으로 내보냄."""
    cutoff = end_date.replace(year=end_date.year - EXPORT_YEARS)
    df = conn.execute(f"SELECT * FROM {table} WHERE Date >= '{cutoff}' ORDER BY Date").df()
    df.set_index("Date").to_parquet(GIT_DIR / f"{table}.parquet")
```

#### 최초 실행 시 마이그레이션

`mms.db`에 테이블이 없을 때 자동 실행:
- `data/*.csv` → 각 DuckDB 테이블
- `data/individual_bonds/*.parquet` → `individual_bonds` 테이블

#### git 정리 (수동, 1회)

```bash
git rm --cached data/*.csv
git rm -r --cached data/individual_bonds/
# .gitignore에 이미 추가됨
```

---

### 변경 후 git 추적 파일 크기

| 파일 | 현재 CSV | Parquet 5Y 슬라이스 | 절감 |
|------|----------|---------------------|------|
| bond_summary | 105 KB | ~18 KB | ~83% |
| global_treasury | 340 KB | ~55 KB | ~84% |
| otc_summary | 142 KB | ~22 KB | ~85% |
| bond_futures | 121 KB | ~25 KB | ~79% |
| individual_bonds | 17 MB | **git 제거** | 100% |
| **합계** | **~17.7 MB** | **~120 KB** | **99%** |

---

### 주의 사항

- `D:\MMS_cache`는 외장 SSD 의존 → 미연결 시 수집 불가 (정상 동작)
- 환경변수 `MMS_CACHE_DIR`로 경로 오버라이드 가능
- `auto_collect.bat` 경로 변경 불필요 (환경변수 없으면 자동으로 `D:/MMS_cache` 사용)
- Streamlit 서버는 `pyarrow` 필요 (requirements.txt에 이미 포함)

---

## 구조 비판적 검토 (2026-03-04)

### 현재 모듈 구조의 문제점

#### ① `fill_calendar()` 완전 중복 — DRY 위반
- `KofiaCalc.fill_calendar()` (`modules/calculator/kofia.py:20`)
- `TreasuryCalc.fill_calendar()` (`modules/calculator/global_treasury.py:18`)
- 두 메서드가 한 글자도 다르지 않음

#### ② `modules/collector/kofia.py` 비대화
- 현재 5개 클래스(`TreasurySummary`, `BondSummary`, `BondSummary_OTC`, `BondFutures`, `individual_bond`)가 단일 파일에 존재
- CLAUDE.md는 `modules/collector/<source_name>/<data_name>.py` 하위 패키지 구조를 지시하지만 실제 구현은 단일 파일 → 불일치

#### ③ `collect_data.py` 반복 패턴 5회 복붙
각 데이터셋마다 동일한 증분 수집 패턴이 반복됨:
```python
existing = _load_csv(PATH)
last     = _last_date(existing)
start_dt = last + timedelta(days=1) if last else <기본값>
if start_dt <= end_date:
    df_new = Collector().collect(...)
    df_std = Calc.standardize(df_new)
    _merge_save(existing, df_std, PATH)
```

#### ④ 기타 리스크
- `bond_futures.csv` 단종 종목 NaN 컬럼이 계속 누적 → 장기적으로 컬럼 정리 기준 필요
- `individual_bonds` parquet이 연도별 분리 구조여서, `_load_individual_last_date`가 마지막 parquet만 읽음 — 연도 경계에서 수집 범위 계산 취약
- `auto_collect.bat`의 날짜 추출 `%date:~0,4%` 방식은 Windows 로케일에 따라 오동작 가능

---

### 개선 방향

#### 단기 — 즉시 개선 (최소 변경)

1. **`fill_calendar()` 중복 제거**
   - `modules/calculator/_base.py` 생성 → 공통 함수 추출
   - `KofiaCalc`, `TreasuryCalc` 양쪽에서 import하여 사용

2. **`collect_data.py` 설정 기반 리팩터링**
   ```python
   DATASETS = [
       {
           "name": "BondSummary",
           "csv": BOND_SUMMARY_CSV,
           "collect": lambda s, e: BondSummary().collect(s, e),
           "standardize": KofiaCalc.standardize_bond,
           "default_years": 5,
       },
       ...
   ]
   for ds in DATASETS:
       _run_incremental(ds)  # 공통 증분 수집 함수
   ```
   새 데이터소스 추가 시 dict 한 줄만 추가하면 됨

#### 중기 — 구조 개선

3. **`modules/collector/kofia.py` → 패키지로 분할**
   ```
   modules/collector/kofia/
     __init__.py     # 클래스 re-export
     bond.py         # BondSummary, BondSummary_OTC
     futures.py      # BondFutures
     individual.py   # individual_bond
     treasury.py     # TreasurySummary
   ```

#### 장기 — 데이터소스 패키지 구조 (확장성 최대)

현재 기능별(collector/calculator) 구조 대신 **데이터소스별 패키지** 구조로 전환:
```
modules/
  shared/
    utils.py          # fill_calendar, get_ref_value 등 범용 함수
  kofia/
    collector.py      # 5개 클래스
    calculator.py     # KofiaCalc
  investing/
    collector.py      # GlobalTreasury
    calculator.py     # TreasuryCalc
```
새 소스 추가 시 `modules/새소스/` 패키지만 추가. 관련 collector+calculator가 같은 패키지에 묶여 응집도 향상.

> **현실적 권장:** 현재 규모에서는 단기 개선(1~2)만 적용하고, 중기(3)는 클래스가 더 늘어날 때 적용.

---

## 자동 수집 설정 (auto_collect.bat)

```bat
@echo off
cd /d %~dp0
set YYYYMMDD=%date:~0,4%%date:~5,2%%date:~8,2%
echo [%date% %time%] 수집 시작 >> data\collect_log.txt
call .venv\Scripts\activate.bat
python collect_data.py >> data\collect_log.txt 2>&1
git add data/
git diff --cached --quiet
if errorlevel 1 (
    git commit -m "데이터 업데이트 %YYYYMMDD%"
    git push
    echo [%date% %time%] push 완료 >> data\collect_log.txt
) else (
    echo [%date% %time%] 변경 없음 push 스킵 >> data\collect_log.txt
)
```

작업 스케줄러 등록 (PowerShell, 1회 실행):
```powershell
$action  = New-ScheduledTaskAction -Execute "C:\Users\user\Desktop\KAIST_MFE\Macro_Analysis\MMS\auto_collect.bat"
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At 17:00
Register-ScheduledTask -TaskName "MMS_AutoCollect" -Action $action -Trigger $trigger -RunLevel Highest
```

---

## 데이터 소스 & 컬럼 레퍼런스

| 파일 | 컬럼 형식 | 예시 | 비고 |
|------|-----------|------|------|
| `global_treasury.parquet` | `{CC}_{n}Y` | `US_10Y`, `DE_2Y` | investing.com, 5개국 30만기 (KR 없음) |
| `bond_summary.parquet` | 영문 코드 | `KTB_10Y`, `MSB_91D`, `CD_91D` | KOFIA 최종호가, 18종목 ⚠️ KTB 단기물 역사 공백 |
| `otc_summary.parquet` | 영문 코드 | `KTB_10Y`, `KEPCO_3Y` | KOFIA 장외거래, 14종목 |
| `bond_futures.parquet` | 채권명 원문 | KOFIA 채권명 | 종목 단종 시 NaN 컬럼 |
| DuckDB `individual_bonds` | `Date` + 원문 컬럼들 | 종목코드, 수익률, 거래량 | 일별 ~800행, git 미추적 |

**KR 국채 병합**: `main.py`에서 `bond_summary`의 `KTB_{n}Y` → `KR_{n}Y` 변환 후 `global_treasury`와 outer join. 만기는 2/3/5/10/20/30년만.

채권 영문 코드 → 한글 레이블 매핑은 `main.py`의 `BOND_LABELS` dict 참조.

---

## 완료 기준 체크리스트

새 탭 추가 시 완료 기준:
- [ ] `collect_data.py`에 증분 수집 섹션 추가 및 로컬 테스트
- [ ] `main.py`에 `_load_*()` 함수 추가
- [ ] `st.tabs([...])` 목록에 탭명 추가 (Raw Data는 항상 마지막)
- [ ] `with tab_*:` 블록에 요약 테이블 + 차트 구현
- [ ] Raw Data 서브탭 추가
- [ ] 데이터 없을 때 `st.error()` 안내 메시지 확인
- [ ] `git push` 후 Streamlit 서버에서 정상 표시 확인
