"""
KOFIA 데이터 수집기

TreasurySummary : 주요 만기 국채 금리 — 기간별 탭, 5개 시리즈
BondSummary     : 전종목 최종호가수익률 — 18개 시리즈, 3배치 수집 후 병합
BondFutures     : 국채선물수익률 — 90일 청크 분할 수집, 채권별 수평 확장

공통 흐름:
  default_content
    → frame "fraAMAKMain"  (메뉴 클릭)
    → frame "maincontent"  (기간별 탭 클릭)
    → frame "tabContents1_contents_tabs2_body"  (날짜·체크박스 조작)

collect() 는 파일을 저장하지 않고 DataFrame을 반환합니다.
저장·병합은 collect_data.py 에서 처리합니다.
"""

import os
import sys
import time
import pandas as pd
from pathlib import Path
from datetime import date, datetime, timedelta

_root = Path(__file__).resolve().parents[2]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


# ─── 공통 Selenium 헬퍼 ───────────────────────────────────────────────────────

_KOFIA_URL     = "https://www.kofiabond.or.kr/index.html"
_KOFIA_DL_FILE = "최종호가 수익률.xls"


def _build_options(headless: bool, download_path: str) -> Options:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("prefs", {
        "download.default_directory": download_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    })
    return opts


def _navigate_to_period_tab(driver, wait):
    """메뉴 클릭 → 기간별 탭 → 내부 프레임까지 진입."""
    driver.get(_KOFIA_URL)
    time.sleep(5)

    driver.switch_to.frame("fraAMAKMain")
    _safe_click(driver, wait, By.ID, "genLv1_0_imgLv1")
    time.sleep(1)
    _safe_click(driver, wait, By.ID, "genLv1_0_genLv2_0_txtLv2")
    time.sleep(3)

    wait.until(EC.frame_to_be_available_and_switch_to_it((By.ID, "maincontent")))
    _safe_click(driver, wait, By.ID, "tabContents1_tab_tabs2")
    time.sleep(3)

    wait.until(EC.frame_to_be_available_and_switch_to_it((By.ID, "tabContents1_contents_tabs2_body")))


def _safe_click(driver, wait, by, value):
    el = wait.until(EC.presence_of_element_located((by, value)))
    driver.execute_script("arguments[0].click();", el)


def _force_click_checkbox(driver, cid: str):
    """is_selected() 없이 체크박스를 직접 클릭합니다.
    WebSquare 커스텀 체크박스는 is_selected()가 항상 False를 반환하므로
    상태 조회 없이 caller가 상태를 추적하여 호출해야 합니다."""
    try:
        cb = driver.find_element(By.ID, cid)
        driver.execute_script("arguments[0].click();", cb)
    except Exception:
        pass


def _set_date_range(driver, wait, start_str: str, end_str: str):
    s = wait.until(EC.presence_of_element_located((By.ID, "startDtDD_input")))
    e = wait.until(EC.presence_of_element_located((By.ID, "endDtDD_input")))
    driver.execute_script("arguments[0].value = '';", s)
    s.send_keys(start_str)
    driver.execute_script("arguments[0].value = '';", e)
    e.send_keys(end_str)
    time.sleep(1)


def _wait_for_download(save_dir: str, cwd: str, timeout: int = 30, filename: str = _KOFIA_DL_FILE) -> str | None:
    for _ in range(timeout):
        for p in [
            os.path.join(save_dir, filename),
            os.path.join(cwd, filename),
            os.path.join(cwd, "data", filename),
        ]:
            if os.path.exists(p):
                return p
        time.sleep(1)
    return None


def _parse_kofia_xls(file_path: str) -> pd.DataFrame | None:
    """KOFIA .xls(HTML 테이블) 파일 → Date 컬럼 표준화된 DataFrame.

    파일을 바이트로 읽어 EUC-KR 디코딩 후 파싱합니다.
    (pd.read_html의 encoding 파라미터는 로컬 파일에서 동작하지 않음)
    """
    from io import StringIO

    try:
        with open(file_path, "rb") as f:
            raw = f.read()
        try:
            text = raw.decode("euc-kr")
        except UnicodeDecodeError:
            text = raw.decode("utf-8", errors="replace")

        try:
            dfs = pd.read_html(StringIO(text), flavor="lxml")
            df  = dfs[0]
        except Exception:
            df = pd.read_excel(file_path)

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ["_".join(str(c) for c in col).strip() for col in df.columns]

        date_col = next((c for c in df.columns if "일자" in str(c) or "Date" in str(c)), None)
        if not date_col:
            return None

        df = df[~df[date_col].astype(str).str.contains("최고|최저|Average|Max|Min", na=False)]
        df[date_col] = pd.to_datetime(
            df[date_col].astype(str).str.replace(r"[^0-9-]", "", regex=True), errors="coerce"
        )
        df = df.dropna(subset=[date_col])
        df = df.rename(columns={date_col: "Date"})
        df["Date"] = df["Date"].dt.date
        return df
    except Exception as e:
        print(f"  [파싱 오류] {file_path}: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
# Class 1: TreasurySummary
# ══════════════════════════════════════════════════════════════════════════════

class TreasurySummary:
    """
    KOFIA 주요 만기 국채 금리 수집.

    수집 대상: 국고채 2/3/10/20/30년
    반환: Date 컬럼 + 금리 컬럼들의 DataFrame (저장은 collect_data.py 에서 처리)
    """

    _UNCHECK = ["chkAnnItm_input_10", "chkAnnItm_input_11",
                "chkAnnItm_input_14", "chkAnnItm_input_16"]
    _CHECK   = ["chkAnnItm_input_1", "chkAnnItm_input_2",
                "chkAnnItm_input_4", "chkAnnItm_input_5", "chkAnnItm_input_6"]

    def __init__(self, download_dir: str | None = None):
        if download_dir is None:
            self.download_dir = os.path.abspath(os.path.join(os.getcwd(), "data"))
        else:
            self.download_dir = os.path.abspath(download_dir)
        self._tmp_dir = os.path.join(self.download_dir, "tmp")
        os.makedirs(self._tmp_dir, exist_ok=True)

    def collect(self, start_date: str, end_date: str, headless: bool = True) -> pd.DataFrame | None:
        """
        Selenium으로 KOFIA 기간별 탭을 조작하여 국채 금리 데이터를 수집합니다.

        Args:
            start_date: "YYYY-MM-DD"
            end_date  : "YYYY-MM-DD"
            headless  : True이면 브라우저 창 없이 실행

        Returns:
            Date 컬럼을 포함한 DataFrame. 실패 시 None.
        """
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=_build_options(headless, self._tmp_dir),
        )
        wait = WebDriverWait(driver, 30)

        try:
            _navigate_to_period_tab(driver, wait)
            _set_date_range(driver, wait, start_date, end_date)

            # 기본 체크 해제: 페이지 기본값으로 체크된 항목을 직접 클릭해서 해제
            for cid in self._UNCHECK:
                _force_click_checkbox(driver, cid)
            # 수집 대상 체크: 현재 모두 해제된 상태이므로 직접 클릭해서 체크
            for cid in self._CHECK:
                _force_click_checkbox(driver, cid)
            time.sleep(1)

            _safe_click(driver, wait, By.ID, "image4")
            time.sleep(5)
            _safe_click(driver, wait, By.ID, "imgExcel")
            time.sleep(5)

            dl = _wait_for_download(self._tmp_dir, os.getcwd())
            if not dl:
                print("  [오류] 다운로드 파일 미발견")
                return None

            df = _parse_kofia_xls(dl)
            try:
                os.remove(dl)
            except Exception:
                pass

            if df is None or "Date" not in df.columns:
                print("  [경고] 날짜 컬럼 미발견")
                return None

            df = df.sort_values("Date", ascending=True).reset_index(drop=True)
            print(f"  [완료] {len(df)}행")
            return df

        except Exception as e:
            print(f"  [Selenium 오류] {e}")
            try:
                with open(os.path.join(self.download_dir, "selenium_error_treasury.html"), "w", encoding="utf-8") as f:
                    f.write(driver.page_source)
            except Exception:
                pass
            return None
        finally:
            driver.quit()


# ══════════════════════════════════════════════════════════════════════════════
# Class 2: BondSummary
# ══════════════════════════════════════════════════════════════════════════════

_BOND_SUMMARY_BATCHES = [
    {
        "name": "A",
        "ids": [
            "chkAnnItm_input_0",   # 국고채권(1년)
            "chkAnnItm_input_1",   # 국고채권(2년)
            "chkAnnItm_input_2",   # 국고채권(3년)
            "chkAnnItm_input_3",   # 국고채권(5년)
            "chkAnnItm_input_4",   # 국고채권(10년)
            "chkAnnItm_input_5",   # 국고채권(20년)
        ],
    },
    {
        "name": "B",
        "ids": [
            "chkAnnItm_input_6",   # 국고채권(30년)
            "chkAnnItm_input_7",   # 국고채권(50년)
            "chkAnnItm_input_8",   # 국민주택1종(5년)
            "chkAnnItm_input_9",   # 통안증권(91일)
            "chkAnnItm_input_10",  # 통안증권(1년)
            "chkAnnItm_input_11",  # 통안증권(2년)
        ],
    },
    {
        "name": "C",
        "ids": [
            "chkAnnItm_input_12",  # 한전채(3년)
            "chkAnnItm_input_13",  # 산금채(1년)
            "chkAnnItm_input_14",  # 회사채(무보증3년)AA-
            "chkAnnItm_input_15",  # 회사채(무보증3년)BBB-
            "chkAnnItm_input_16",  # CD수익률(91일)
            "chkAnnItm_input_17",  # CP(91일)
        ],
    },
]

# 페이지 기본 체크 항목 — 신규 세션 진입 시 항상 체크되어 있는 6개 항목
_BOND_SUMMARY_INIT_UNCHECK = [
    "chkAnnItm_input_16",  # CD수익률(91일)
    "chkAnnItm_input_14",  # 회사채(무보증3년)AA-
    "chkAnnItm_input_10",  # 통안증권(1년)
    "chkAnnItm_input_11",  # 통안증권(2년)
    "chkAnnItm_input_2",   # 국고채권(3년)
    "chkAnnItm_input_3",   # 국고채권(5년)
]


class BondSummary:
    """
    KOFIA 최종호가수익률 전종목 수집.

    18개 시리즈를 6개씩 3배치로 나눠 수집 후 Date 기준 merge.
    배치: A(국고채 1~20년) / B(국고채 30·50년 + 통안증권) / C(한전채·산금채·회사채·CD·CP)
    반환: Date 컬럼 + 18개 시리즈 컬럼들의 DataFrame (저장은 collect_data.py 에서 처리)
    """

    def __init__(self, download_dir: str | None = None):
        if download_dir is None:
            self.download_dir = os.path.abspath(os.path.join(os.getcwd(), "data"))
        else:
            self.download_dir = os.path.abspath(download_dir)
        self._tmp_dir = os.path.join(self.download_dir, "tmp")
        os.makedirs(self._tmp_dir, exist_ok=True)

    def collect(self, start_date: str, end_date: str, headless: bool = True) -> pd.DataFrame | None:
        """
        배치(A/B/C)마다 독립 Chrome 세션으로 수집 후 병합하여 반환합니다.

        배치당 독립 세션을 사용하는 이유:
          - 엑셀 다운로드 후 WebSquare가 내부 상태를 리셋할 수 있어,
            단일 세션에서 배치 간 체크박스 상태가 오염될 수 있음
          - 독립 세션은 항상 알려진 초기 상태(기본 체크 항목 고정)에서 시작

        Args:
            start_date: "YYYY-MM-DD"
            end_date  : "YYYY-MM-DD"
            headless  : True이면 브라우저 창 없이 실행

        Returns:
            Date 컬럼을 포함한 병합 DataFrame. 실패 시 None.
        """
        print(f"  기간: {start_date} ~ {end_date}")

        chromedriver_path = ChromeDriverManager().install()
        batch_files: list[tuple[str, str]] = []

        for batch in _BOND_SUMMARY_BATCHES:
            bname = batch["name"]
            bids  = batch["ids"]
            print(f"  [배치 {bname}] 수집 시작...")

            driver = webdriver.Chrome(
                service=Service(chromedriver_path),
                options=_build_options(headless, self._tmp_dir),
            )
            wait = WebDriverWait(driver, 30)

            try:
                _navigate_to_period_tab(driver, wait)
                _set_date_range(driver, wait, start_date, end_date)

                # 페이지 기본 체크 항목 해제 (신규 세션이므로 항상 알려진 초기 상태)
                for cid in _BOND_SUMMARY_INIT_UNCHECK:
                    _force_click_checkbox(driver, cid)
                time.sleep(0.3)

                # 이 배치만 체크
                for cid in bids:
                    _force_click_checkbox(driver, cid)
                time.sleep(0.5)

                _safe_click(driver, wait, By.ID, "image4")
                time.sleep(5)
                _safe_click(driver, wait, By.ID, "imgExcel")
                time.sleep(5)

                dl = _wait_for_download(self._tmp_dir, os.getcwd())
                if dl:
                    dest = os.path.join(self._tmp_dir, f"bond_summary_{bname}.xls")
                    if os.path.exists(dest):
                        os.remove(dest)
                    os.rename(dl, dest)
                    batch_files.append((bname, dest))
                    print(f"  [배치 {bname}] 완료 → {os.path.basename(dest)}")
                else:
                    print(f"  [배치 {bname}] 다운로드 실패 — 건너뜀")

            except Exception as e:
                print(f"  [배치 {bname}] Selenium 오류: {e}")
                try:
                    with open(
                        os.path.join(self.download_dir, f"selenium_error_bond_{bname}.html"),
                        "w", encoding="utf-8",
                    ) as f:
                        f.write(driver.page_source)
                except Exception:
                    pass
            finally:
                driver.quit()

        if not batch_files:
            print("  [실패] 다운로드된 파일 없음")
            return None

        dfs: list[pd.DataFrame] = []
        for bname, path in batch_files:
            df = _parse_kofia_xls(path)
            if df is not None:
                dfs.append(df)
                print(f"  [배치 {bname}] {len(df)}행, {len(df.columns) - 1}열 파싱 완료")

        for _, path in batch_files:
            try:
                os.remove(path)
            except Exception:
                pass

        if not dfs:
            print("  [실패] 파싱 가능한 파일 없음")
            return None

        # Date를 인덱스로 설정 후 axis=1 방향으로 concat (outer join → 날짜 범위 통일)
        dfs_indexed = [df.set_index("Date") for df in dfs]
        merged_idx = pd.concat(dfs_indexed, axis=1)

        # 동일 컬럼명이 여러 배치에 중복된 경우(KOFIA XLS가 전종목 헤더를 포함할 때)
        # → 각 컬럼별 첫 번째 non-NaN 값으로 결합
        if merged_idx.columns.duplicated().any():
            unique_cols = list(dict.fromkeys(merged_idx.columns))
            deduped: dict[str, pd.Series] = {}
            for col in unique_cols:
                sub = merged_idx.loc[:, merged_idx.columns == col]
                if sub.shape[1] > 1:
                    combined = sub.iloc[:, 0]
                    for i in range(1, sub.shape[1]):
                        combined = combined.combine_first(sub.iloc[:, i])
                    deduped[col] = combined
                else:
                    deduped[col] = sub.iloc[:, 0]
            merged_idx = pd.DataFrame(deduped, index=merged_idx.index)

        merged = merged_idx.reset_index()
        merged = merged.sort_values("Date", ascending=True).reset_index(drop=True)
        print(f"  [완료] 병합 완료  ({len(merged)}행, {len(merged.columns)}열)")
        return merged


# ══════════════════════════════════════════════════════════════════════════════
# Class 3: BondSummary_OTC
# ══════════════════════════════════════════════════════════════════════════════

_OTC_DL_FILE = "장외거래 대표수익률(기간별).xls"

_OTC_BATCHES = [
    {
        "name": "A",
        "ids": [
            "chkAnnItm_input_0",   # 국고채권(2년)
            "chkAnnItm_input_1",   # 국고채권(3년)
            "chkAnnItm_input_2",   # 국고채권(5년)
            "chkAnnItm_input_3",   # 국고채권(10년)
            "chkAnnItm_input_4",   # 국고채권(20년)
            "chkAnnItm_input_5",   # 국고채권(30년)
        ],
    },
    {
        "name": "B",
        "ids": [
            "chkAnnItm_input_6",   # 국고채권(50년)
            "chkAnnItm_input_7",   # 국민주택1종(5년)
            "chkAnnItm_input_10",  # 한국전력(3년)
            "chkAnnItm_input_11",  # 통안증권(91일)
            "chkAnnItm_input_12",  # 통안증권(1년)
            "chkAnnItm_input_13",  # 통안증권(2년)
        ],
    },
    {
        "name": "C",
        "ids": [
            "chkAnnItm_input_15",  # 산금채(1년)
            "chkAnnItm_input_16",  # 무보증AA-(3년)
        ],
    },
]

# OTC 페이지 기본 체크 항목 (신규 세션 진입 시 항상 체크되어 있는 6개)
_OTC_INIT_UNCHECK = [
    "chkAnnItm_input_1",   # 국고채권(3년)
    "chkAnnItm_input_2",   # 국고채권(5년)
    "chkAnnItm_input_3",   # 국고채권(10년)
    "chkAnnItm_input_12",  # 통안증권(1년)
    "chkAnnItm_input_13",  # 통안증권(2년)
    "chkAnnItm_input_16",  # 무보증AA-(3년)
]


def _navigate_to_otc_page(driver, wait):
    """OTC(장외거래대표수익률) 페이지 진입."""
    driver.get(_KOFIA_URL)
    time.sleep(5)

    driver.switch_to.frame("fraAMAKMain")
    _safe_click(driver, wait, By.ID, "genLv1_0_imgLv1")
    time.sleep(1)
    _safe_click(driver, wait, By.ID, "genLv1_0_genLv2_1_txtLv2")
    time.sleep(3)

    wait.until(EC.frame_to_be_available_and_switch_to_it((By.ID, "maincontent")))
    _safe_click(driver, wait, By.ID, "tabContents1_tab_tabs2")
    time.sleep(3)

    wait.until(EC.frame_to_be_available_and_switch_to_it((By.ID, "tabContents1_contents_tabs2_body")))


class BondSummary_OTC:
    """
    KOFIA 장외거래대표수익률 수집.

    14개 시리즈를 6·6·2개씩 3배치로 나눠 수집 후 Date 기준 merge.
    배치: A(국고채 2~30년) / B(국고채50년·국민주택·한전·통안) / C(산금채·무보증AA-)
    반환: Date 컬럼 + 시리즈 컬럼들의 DataFrame (저장은 collect_data.py 에서 처리)

    ⚠️  최초 실행 전 _navigate_to_otc_page() 의 메뉴 ID를 반드시 확인.
    """

    def __init__(self, download_dir: str | None = None):
        if download_dir is None:
            self.download_dir = os.path.abspath(os.path.join(os.getcwd(), "data"))
        else:
            self.download_dir = os.path.abspath(download_dir)
        self._tmp_dir = os.path.join(self.download_dir, "tmp")
        os.makedirs(self._tmp_dir, exist_ok=True)

    def collect(self, start_date: str, end_date: str, headless: bool = True) -> pd.DataFrame | None:
        """
        배치(A/B/C)마다 독립 Chrome 세션으로 수집 후 병합하여 반환합니다.

        Args:
            start_date: "YYYY-MM-DD"
            end_date  : "YYYY-MM-DD"
            headless  : True이면 브라우저 창 없이 실행

        Returns:
            Date 컬럼을 포함한 병합 DataFrame. 실패 시 None.
        """
        print(f"  기간: {start_date} ~ {end_date}")

        chromedriver_path = ChromeDriverManager().install()
        batch_files: list[tuple[str, str]] = []

        for batch in _OTC_BATCHES:
            bname = batch["name"]
            bids  = batch["ids"]
            print(f"  [배치 {bname}] 수집 시작...")

            driver = webdriver.Chrome(
                service=Service(chromedriver_path),
                options=_build_options(headless, self._tmp_dir),
            )
            wait = WebDriverWait(driver, 30)

            try:
                _navigate_to_otc_page(driver, wait)
                _set_date_range(driver, wait, start_date, end_date)

                for cid in _OTC_INIT_UNCHECK:
                    _force_click_checkbox(driver, cid)
                time.sleep(0.3)

                for cid in bids:
                    _force_click_checkbox(driver, cid)
                time.sleep(0.5)

                _safe_click(driver, wait, By.ID, "image8")
                time.sleep(5)
                _safe_click(driver, wait, By.ID, "imgExcel")
                time.sleep(5)

                dl = _wait_for_download(self._tmp_dir, os.getcwd(), filename=_OTC_DL_FILE)
                if dl:
                    dest = os.path.join(self._tmp_dir, f"otc_summary_{bname}.xls")
                    if os.path.exists(dest):
                        os.remove(dest)
                    os.rename(dl, dest)
                    batch_files.append((bname, dest))
                    print(f"  [배치 {bname}] 완료 → {os.path.basename(dest)}")
                else:
                    print(f"  [배치 {bname}] 다운로드 실패 — 건너뜀")

            except Exception as e:
                print(f"  [배치 {bname}] Selenium 오류: {e}")
                try:
                    with open(
                        os.path.join(self.download_dir, f"selenium_error_otc_{bname}.html"),
                        "w", encoding="utf-8",
                    ) as f:
                        f.write(driver.page_source)
                except Exception:
                    pass
            finally:
                driver.quit()

        if not batch_files:
            print("  [실패] 다운로드된 파일 없음")
            return None

        dfs: list[pd.DataFrame] = []
        for bname, path in batch_files:
            df = _parse_kofia_xls(path)
            if df is not None:
                dfs.append(df)
                print(f"  [배치 {bname}] {len(df)}행, {len(df.columns) - 1}열 파싱 완료")

        for _, path in batch_files:
            try:
                os.remove(path)
            except Exception:
                pass

        if not dfs:
            print("  [실패] 파싱 가능한 파일 없음")
            return None

        dfs_indexed = [df.set_index("Date") for df in dfs]
        merged_idx = pd.concat(dfs_indexed, axis=1)

        if merged_idx.columns.duplicated().any():
            unique_cols = list(dict.fromkeys(merged_idx.columns))
            deduped: dict[str, pd.Series] = {}
            for col in unique_cols:
                sub = merged_idx.loc[:, merged_idx.columns == col]
                if sub.shape[1] > 1:
                    combined = sub.iloc[:, 0]
                    for i in range(1, sub.shape[1]):
                        combined = combined.combine_first(sub.iloc[:, i])
                    deduped[col] = combined
                else:
                    deduped[col] = sub.iloc[:, 0]
            merged_idx = pd.DataFrame(deduped, index=merged_idx.index)

        merged = merged_idx.reset_index()
        merged = merged.sort_values("Date", ascending=True).reset_index(drop=True)
        print(f"  [완료] 병합 완료  ({len(merged)}행, {len(merged.columns)}열)")
        return merged


# ══════════════════════════════════════════════════════════════════════════════
# Class 4: BondFutures
# ══════════════════════════════════════════════════════════════════════════════

_FUTURES_CHUNK_DAYS = 90  # 3개월 단위 조회 제한


def _wait_for_new_download(save_dir: str, before_files: set, timeout: int = 30) -> str | None:
    """다운로드 전/후 파일 목록 비교로 새로 생긴 파일을 반환."""
    for _ in range(timeout):
        current = set(os.listdir(save_dir))
        new_files = {
            f for f in current - before_files
            if not f.endswith(".crdownload") and not f.endswith(".tmp")
        }
        if new_files:
            return os.path.join(save_dir, sorted(new_files)[0])
        time.sleep(1)
    return None


class BondFutures:
    """
    KOFIA 국채선물수익률 수집.

    - 3개월(90일) 단위 청크로 조회 분할 수집
    - 같은 채권명 컬럼 → 수직(row) 병합
    - 신규 채권명 컬럼 → 수평(column) 확장, 과거 채권이 왼쪽
    - 반환: Date 컬럼 + 채권명 컬럼들의 DataFrame (저장은 collect_data.py 에서 처리)

    내비게이션 순서:
      fraAMAKMain → genLv1_0_imgLv1 (채권금리)
                  → leftGenLv1_3_leftTxtLv1 (국채선물수익률)
      maincontent → tabContents1_tab_tabs2 (기간별 탭)
      tabContents1_contents_tabs2_body
        → schStandardDt_input / schUstandardDt_input (날짜)
        → image3 (조회) → image4 (엑셀 다운로드)
    """

    def __init__(self, download_dir: str | None = None):
        if download_dir is None:
            self.download_dir = os.path.abspath(os.path.join(os.getcwd(), "data"))
        else:
            self.download_dir = os.path.abspath(download_dir)
        self._tmp_dir = os.path.join(self.download_dir, "tmp")
        os.makedirs(self._tmp_dir, exist_ok=True)

    @staticmethod
    def _date_chunks(start: date, end: date):
        """start ~ end 를 90일 단위 (chunk_start, chunk_end) 쌍으로 분할."""
        chunk_start = start
        while chunk_start <= end:
            chunk_end = min(chunk_start + timedelta(days=89), end)
            yield chunk_start, chunk_end
            chunk_start = chunk_end + timedelta(days=1)

    def collect(self, start_date: str, end_date: str, headless: bool = True) -> pd.DataFrame | None:
        """
        90일 단위 청크로 분할 수집 후 병합하여 반환합니다.

        Args:
            start_date: "YYYY-MM-DD"
            end_date  : "YYYY-MM-DD"
            headless  : True이면 브라우저 창 없이 실행

        Returns:
            Date 컬럼 + 채권명 컬럼들의 DataFrame. 실패 시 None.
            열 순서: 과거 채권(첫 출현)이 왼쪽, 최신 채권이 오른쪽.
        """
        start  = datetime.strptime(start_date, "%Y-%m-%d").date()
        end    = datetime.strptime(end_date,   "%Y-%m-%d").date()
        chunks = list(self._date_chunks(start, end))

        print(f"  기간: {start_date} ~ {end_date}  ({len(chunks)}개 청크)")

        chromedriver_path = ChromeDriverManager().install()
        chunk_dfs: list[pd.DataFrame] = []
        seen_cols: list[str] = []   # 첫 출현 순서 추적 (오래된 채권 → 왼쪽)

        for i, (cs, ce) in enumerate(chunks):
            cs_str = cs.strftime("%Y-%m-%d")
            ce_str = ce.strftime("%Y-%m-%d")
            print(f"  [청크 {i+1}/{len(chunks)}] {cs_str} ~ {ce_str}", end=" ... ", flush=True)

            driver = webdriver.Chrome(
                service=Service(chromedriver_path),
                options=_build_options(headless, self._tmp_dir),
            )
            wait = WebDriverWait(driver, 30)

            try:
                before = set(os.listdir(self._tmp_dir))

                # ── 페이지 진입 ──────────────────────────────────────────────
                driver.get(_KOFIA_URL)
                time.sleep(5)
                driver.switch_to.frame("fraAMAKMain")
                _safe_click(driver, wait, By.ID, "genLv1_0_imgLv1")
                time.sleep(1)
                _safe_click(driver, wait, By.ID, "leftGenLv1_3_leftTxtLv1")
                time.sleep(3)
                wait.until(EC.frame_to_be_available_and_switch_to_it((By.ID, "maincontent")))
                _safe_click(driver, wait, By.ID, "tabContents1_tab_tabs2")
                time.sleep(3)
                wait.until(EC.frame_to_be_available_and_switch_to_it(
                    (By.ID, "tabContents1_contents_tabs2_body")
                ))

                # ── 날짜 입력 (선물 페이지 전용 ID) ─────────────────────────
                s_el = wait.until(EC.presence_of_element_located((By.ID, "schStandardDt_input")))
                e_el = wait.until(EC.presence_of_element_located((By.ID, "schUstandardDt_input")))
                driver.execute_script("arguments[0].value = '';", s_el)
                s_el.send_keys(cs_str)
                driver.execute_script("arguments[0].value = '';", e_el)
                e_el.send_keys(ce_str)
                time.sleep(1)

                # ── 조회 → 엑셀 다운로드 ────────────────────────────────────
                _safe_click(driver, wait, By.ID, "image3")   # 조회
                time.sleep(5)
                _safe_click(driver, wait, By.ID, "image4")   # 엑셀 다운로드
                time.sleep(5)

                dl = _wait_for_new_download(self._tmp_dir, before)
                if not dl:
                    print("다운로드 실패")
                    continue

                df = _parse_kofia_xls(dl)
                try:
                    os.remove(dl)
                except Exception:
                    pass

                if df is None or "Date" not in df.columns:
                    print("파싱 실패")
                    continue

                # Date → DatetimeIndex
                df_idx = df.set_index("Date")
                df_idx.index = pd.to_datetime(df_idx.index)
                df_idx.index.name = "Date"
                df_idx = df_idx.apply(pd.to_numeric, errors="coerce")

                # 첫 출현 컬럼 순서 기록
                for col in df_idx.columns:
                    if col not in seen_cols:
                        seen_cols.append(col)

                chunk_dfs.append(df_idx)
                print(f"{len(df_idx)}행")

            except Exception as e:
                print(f"오류: {e}")
                try:
                    with open(
                        os.path.join(self.download_dir, f"selenium_error_futures_{i+1}.html"),
                        "w", encoding="utf-8",
                    ) as f:
                        f.write(driver.page_source)
                except Exception:
                    pass
            finally:
                driver.quit()

        if not chunk_dfs:
            print("  [실패] 수집된 데이터 없음")
            return None

        # 청크 병합: axis=0 concat → 같은 채권명은 행으로 합치고, 다른 채권명은 NaN으로 확장
        merged = pd.concat(chunk_dfs, axis=0)
        merged = merged[~merged.index.duplicated(keep="last")]
        merged.sort_index(inplace=True)

        # 열 순서: 첫 출현 순서 유지 (과거 채권이 왼쪽)
        ordered_cols = [c for c in seen_cols if c in merged.columns]
        merged = merged[ordered_cols]

        result = merged.reset_index()
        result["Date"] = pd.to_datetime(result["Date"]).dt.date
        result = result.sort_values("Date", ascending=True).reset_index(drop=True)

        # 컬럼명 정리: XLS 헤더 줄바꿈(\n) 및 앞뒤 공백 제거
        result.columns = [c.replace("\n", " ").strip() for c in result.columns]

        print(f"  [완료] 총 {len(result)}행, {len(result.columns)}열")
        return result


# ══════════════════════════════════════════════════════════════════════════════
# Class 5: individual_bond
# ══════════════════════════════════════════════════════════════════════════════

_INDIVIDUAL_SESSION_DAYS = 100   # Chrome 세션당 최대 처리 일수 (메모리 관리)


def _navigate_to_individual_page(driver, wait):
    """실시간 체결정보 일자별 거래현황 페이지 진입."""
    driver.get(_KOFIA_URL)
    time.sleep(5)
    driver.switch_to.frame("fraAMAKMain")
    _safe_click(driver, wait, By.ID, "genLv1_3_imgLv1")          # 유통시장
    time.sleep(1)
    _safe_click(driver, wait, By.ID, "leftGenLv1_6_leftTxtLv1")  # 실시간 체결정보
    time.sleep(3)
    wait.until(EC.frame_to_be_available_and_switch_to_it((By.ID, "maincontent")))
    _safe_click(driver, wait, By.ID, "tabContents1_tab_tabs2")   # 일자별 거래현황
    time.sleep(3)
    wait.until(EC.frame_to_be_available_and_switch_to_it(
        (By.ID, "tabContents1_contents_tabs2_body")
    ))


def _parse_individual_xls(file_path: str, trade_date) -> pd.DataFrame | None:
    """
    일자별 거래현황 XLS(HTML table) 파일을 파싱합니다.
    날짜 컬럼이 없으므로 trade_date를 Date 컬럼으로 자동 추가합니다.

    pd.read_html의 encoding 파라미터는 로컬 파일에서 동작하지 않으므로
    파일을 바이트로 읽어 EUC-KR 디코딩 후 파싱합니다.
    KOFIA XLS에는 빈 네비게이션 테이블이 앞에 포함될 수 있으므로
    비어있지 않은 첫 번째 테이블을 선택합니다.
    """
    from io import StringIO

    try:
        with open(file_path, "rb") as f:
            raw = f.read()

        df = None

        # 1차 시도: HTML 테이블 파싱 (KOFIA XLS는 EUC-KR HTML 테이블)
        try:
            try:
                text = raw.decode("euc-kr")
            except UnicodeDecodeError:
                text = raw.decode("utf-8", errors="replace")

            dfs = pd.read_html(StringIO(text), flavor="lxml")
            # dfs[0]이 빈 네비게이션/헤더 테이블일 수 있으므로 비어있지 않은 첫 테이블 선택
            for tbl in dfs:
                if not tbl.empty:
                    df = tbl
                    break
        except ValueError:
            pass  # HTML 테이블 없음 → pd.read_excel 시도

        # 2차 시도: 실제 Excel 바이너리 형식인 경우
        if df is None or df.empty:
            try:
                df = pd.read_excel(file_path, engine="xlrd")
            except Exception:
                try:
                    df = pd.read_excel(file_path, engine="openpyxl")
                except Exception:
                    pass

        if df is None or df.empty:
            return None

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ["_".join(str(c) for c in col).strip() for col in df.columns]

        df.columns = [str(c).replace("\n", " ").strip() for c in df.columns]

        if df.empty:
            return None

        df.insert(0, "Date", trade_date)
        return df

    except ValueError:
        # pd.read_html: No tables found → 해당일 데이터 없음
        return None
    except Exception as e:
        print(f"  [파싱 오류] {file_path}: {e}")
        return None


class individual_bond:
    """
    KOFIA 실시간 체결정보 일자별 거래현황 수집.

    - 조회일을 하루씩 변경하며 수집 (기간 조회 불가)
    - 주말 자동 스킵
    - 세션당 최대 100일 처리 후 Chrome 재시작 (메모리 관리)
    - 반환: Date 컬럼 포함 전체 DataFrame (저장은 collect_data.py 에서 처리)

    내비게이션:
      fraAMAKMain → genLv1_3_imgLv1 (유통시장)
                  → leftGenLv1_6_leftTxtLv1 (실시간 체결정보)
      maincontent → tabContents1_tab_tabs2 (일자별 거래현황 탭)
      tabContents1_contents_tabs2_body
        → ipcDt_input (조회일 입력)
        → image8 (조회) → fimage3 (엑셀 다운로드)
    """

    def __init__(self, download_dir: str | None = None):
        if download_dir is None:
            self.download_dir = os.path.abspath(os.path.join(os.getcwd(), "data"))
        else:
            self.download_dir = os.path.abspath(download_dir)
        self._tmp_dir = os.path.join(self.download_dir, "tmp")
        os.makedirs(self._tmp_dir, exist_ok=True)

    def collect(self, start_date: str, end_date: str, headless: bool = True) -> pd.DataFrame | None:
        """
        start_date ~ end_date 를 하루씩 수집합니다.

        Args:
            start_date: "YYYY-MM-DD"
            end_date  : "YYYY-MM-DD"
            headless  : True이면 브라우저 창 없이 실행

        Returns:
            Date 컬럼 포함 전체 DataFrame. 실패 시 None.
        """
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end   = datetime.strptime(end_date,   "%Y-%m-%d").date()

        # 주말 제외한 수집 대상 날짜 목록
        all_dates = [
            start + timedelta(days=i)
            for i in range((end - start).days + 1)
            if (start + timedelta(days=i)).weekday() < 5   # 0=월 … 4=금
        ]

        print(f"  기간: {start_date} ~ {end_date}  (평일 {len(all_dates)}일)")

        chromedriver_path = ChromeDriverManager().install()
        all_dfs: list[pd.DataFrame] = []

        for batch_idx, batch_start in enumerate(range(0, len(all_dates), _INDIVIDUAL_SESSION_DAYS)):
            batch = all_dates[batch_start : batch_start + _INDIVIDUAL_SESSION_DAYS]
            print(f"\n  [세션 {batch_idx + 1}] {batch[0]} ~ {batch[-1]}  ({len(batch)}일)")

            driver = webdriver.Chrome(
                service=Service(chromedriver_path),
                options=_build_options(headless, self._tmp_dir),
            )
            wait = WebDriverWait(driver, 30)

            try:
                _navigate_to_individual_page(driver, wait)

                for day in batch:
                    date_str = day.strftime("%Y-%m-%d")
                    print(f"    {date_str}", end=" ... ", flush=True)

                    before = set(os.listdir(self._tmp_dir))
                    try:
                        # 날짜 입력
                        d_el = wait.until(EC.presence_of_element_located((By.ID, "ipcDt_input")))
                        driver.execute_script("arguments[0].value = '';", d_el)
                        d_el.send_keys(date_str)
                        time.sleep(0.5)

                        # 조회
                        _safe_click(driver, wait, By.ID, "image8")
                        time.sleep(5)

                        # 엑셀 다운로드
                        _safe_click(driver, wait, By.ID, "fimage3")
                        dl = _wait_for_new_download(self._tmp_dir, before, timeout=15)

                        if not dl:
                            print("데이터 없음 (공휴일)")
                            continue

                        df = _parse_individual_xls(dl, day)
                        try:
                            os.remove(dl)
                        except Exception:
                            pass

                        if df is None or df.empty:
                            print("빈 데이터")
                            continue

                        all_dfs.append(df)
                        print(f"{len(df)}행")

                    except Exception as e:
                        print(f"오류: {e}")
                        # 프레임 컨텍스트 복구 시도
                        try:
                            driver.switch_to.default_content()
                            wait.until(EC.frame_to_be_available_and_switch_to_it((By.ID, "maincontent")))
                            wait.until(EC.frame_to_be_available_and_switch_to_it(
                                (By.ID, "tabContents1_contents_tabs2_body")
                            ))
                        except Exception:
                            try:
                                _navigate_to_individual_page(driver, wait)
                            except Exception:
                                pass

            except Exception as e:
                print(f"  [세션 {batch_idx + 1} 오류] {e}")
                try:
                    with open(
                        os.path.join(self.download_dir, f"selenium_error_individual_{batch_idx + 1}.html"),
                        "w", encoding="utf-8",
                    ) as f:
                        f.write(driver.page_source)
                except Exception:
                    pass
            finally:
                driver.quit()

        if not all_dfs:
            print("  [실패] 수집된 데이터 없음")
            return None

        result = pd.concat(all_dfs, axis=0, ignore_index=True)
        result["Date"] = pd.to_datetime(result["Date"]).dt.date
        print(f"\n  [완료] 총 {len(result)}행")
        return result


# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    from modules.calculator.kofia import KofiaCalc

    parser = argparse.ArgumentParser(description="KOFIA 수집기 단독 테스트")
    parser.add_argument(
        "target",
        nargs="?",
        default="all",
        choices=["all", "TreasurySummary", "BondSummary", "BondSummary_OTC",
                 "BondFutures", "individual_bond"],
        help="테스트할 클래스명 (생략 시 전체 실행)",
    )
    args = parser.parse_args()
    run = args.target  # 편의상 짧은 이름

    _end = date.today() - timedelta(days=1)
    try:
        _start_1y = _end.replace(year=_end.year - 1)
    except ValueError:
        _start_1y = _end - timedelta(days=365)
    try:
        _start_5y = _end.replace(year=_end.year - 5)
    except ValueError:
        _start_5y = _end - timedelta(days=365 * 5)

    # ── TreasurySummary ───────────────────────────────────────────────────────
    if run in ("all", "TreasurySummary"):
        print(f"=== TreasurySummary | {_start_1y} ~ {_end} ===")
        ts = TreasurySummary()
        df = ts.collect(start_date=str(_start_1y), end_date=str(_end))
        if df is not None:
            print(KofiaCalc.standardize(df).tail())
        print()

    # ── BondSummary ───────────────────────────────────────────────────────────
    if run in ("all", "BondSummary"):
        print(f"=== BondSummary | {_start_5y} ~ {_end} ===")
        bs = BondSummary()
        df = bs.collect(start_date=str(_start_5y), end_date=str(_end))
        if df is not None:
            print(df.tail())
            print(f"컬럼: {df.columns.tolist()}")
        print()

    # ── BondSummary_OTC ───────────────────────────────────────────────────────
    if run in ("all", "BondSummary_OTC"):
        print(f"=== BondSummary_OTC | {_start_5y} ~ {_end} ===")
        otc = BondSummary_OTC()
        df = otc.collect(start_date=str(_start_5y), end_date=str(_end))
        if df is not None:
            print(df.tail())
            print(f"컬럼: {df.columns.tolist()}")
        print()

    # ── BondFutures ───────────────────────────────────────────────────────────
    if run in ("all", "BondFutures"):
        _start_futures = date(_end.year - 5, 1, 1)
        print(f"=== BondFutures | {_start_futures} ~ {_end} ===")
        bf = BondFutures()
        df = bf.collect(start_date=str(_start_futures), end_date=str(_end), headless=False)
        if df is not None:
            print(df.tail())
            print(f"컬럼 ({len(df.columns) - 1}개): {df.columns.tolist()}")
        print()

    # ── individual_bond ───────────────────────────────────────────────────────
    if run in ("all", "individual_bond"):
        _start_ib = _end - timedelta(days=6)
        print(f"=== individual_bond | {_start_ib} ~ {_end} ===")
        ib = individual_bond()
        df = ib.collect(start_date=str(_start_ib), end_date=str(_end), headless=False)
        if df is not None:
            print(df.head())
            print(f"컬럼 ({len(df.columns)}개): {df.columns.tolist()}")
            print(f"수집 날짜: {sorted(df['Date'].unique())}")
        print()
