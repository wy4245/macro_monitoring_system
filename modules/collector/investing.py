"""
investing.com 데이터 수집기

GlobalTreasury : 글로벌 주요국 국채 금리 (Playwright Chromium)
  collect(start_date, end_date) -> pd.DataFrame | None

수집 국가: US / DE / GB / JP / CN  |  만기: 2/3/5/10/20/30년
컬럼 형식: {CC}_{n}Y  (예: US_10Y, DE_2Y)

구현 방식:
  1. Playwright Chromium headless → Cloudflare 우회 (실제 브라우저 실행)
  2. __NEXT_DATA__ > props.pageProps.state.historicalDataStore.historicalData.data 에서 데이터 추출 (주 방법)
  3. 위 방법이 날짜 범위 미포함 시 page.evaluate() fetch POST /instruments/HistoricalDataAjax 로 AJAX 직접 호출
"""

import re
import sys
import json
import time
from io import StringIO
from pathlib import Path
from datetime import datetime

import pandas as pd
from playwright.sync_api import sync_playwright

_root = Path(__file__).resolve().parents[2]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from modules.calculator.global_treasury import TreasuryCalc  # noqa: E402


class GlobalTreasury:
    """
    investing.com에서 글로벌 국채 금리 데이터를 수집합니다.

    국가  : US(미국), DE(독일), GB(영국), JP(일본), CN(중국)
    만기  : 2 / 3 / 5 / 10 / 20 / 30년 (국가별로 없는 만기는 자동 건너뜀)
    단위  : % (수익률)
    빈도  : 일별 (거래일 기준, 주말·공휴일은 ffill로 채움)
    """

    # investing.com URL 슬러그 맵
    BOND_SLUGS: dict[str, dict[int, str]] = {
        "US": {
            2:  "u.s.-2-year-bond-yield",
            3:  "u.s.-3-year-bond-yield",
            5:  "u.s.-5-year-bond-yield",
            10: "u.s.-10-year-bond-yield",
            20: "us-20-year-bond-yield",
            30: "u.s.-30-year-bond-yield",
        },
        "DE": {
            2:  "germany-2-year-bond-yield",
            3:  "germany-3-year-bond-yield",
            5:  "germany-5-year-bond-yield",
            10: "germany-10-year-bond-yield",
            20: "germany-20-year-bond-yield",
            30: "germany-30-year-bond-yield",
        },
        "GB": {
            2:  "uk-2-year-bond-yield",
            3:  "uk-3-year-bond-yield",
            5:  "uk-5-year-bond-yield",
            10: "uk-10-year-bond-yield",
            20: "uk-20-year-bond-yield",
            30: "uk-30-year-bond-yield",
        },
        "JP": {
            2:  "japan-2-year-bond-yield",
            3:  "japan-3-year-bond-yield",
            5:  "japan-5-year-bond-yield",
            10: "japan-10-year-bond-yield",
            20: "japan-20-year-bond-yield",
            30: "japan-30-year-bond-yield",
        },
        "CN": {
            2:  "china-2-year-bond-yield",
            3:  "china-3-year-bond-yield",
            5:  "china-5-year-bond-yield",
            10: "china-10-year-bond-yield",
            20: "china-20-year-bond-yield",
            30: "china-30-year-bond-yield",
        },
    }

    INVESTING_BASE = "https://www.investing.com/rates-bonds"
    HIST_AJAX_URL  = "https://www.investing.com/instruments/HistoricalDataAjax"

    def __init__(self) -> None:
        self._pair_id_cache: dict[str, int] = {}
        self._history_cache: dict[str, str] = {}   # slug -> 인터셉트된 AJAX HTML (레거시)
        self._nd_data_cache: dict[str, list] = {}  # slug -> __NEXT_DATA__ historicalData.data
        self._pw      = None
        self._browser = None
        self._ctx     = None
        self._page    = None

    # ── 브라우저 시작/종료 ─────────────────────────────────────────────────────

    def _start_browser(self) -> None:
        self._pw      = sync_playwright().start()
        self._browser = self._pw.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        self._ctx = self._browser.new_context(
            locale="en-US",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
        )
        self._page    = self._ctx.new_page()
        self._debug_html_saved = False

    def _stop_browser(self) -> None:
        try:
            self._browser.close()
        except Exception:
            pass
        try:
            self._pw.stop()
        except Exception:
            pass
        self._page = self._ctx = self._browser = self._pw = None

    # ── pair_id 조회 ──────────────────────────────────────────────────────────

    def _get_pair_id(self, slug: str) -> int | None:
        if slug in self._pair_id_cache:
            return self._pair_id_cache[slug]

        url = f"{self.INVESTING_BASE}/{slug}-historical-data"

        # 페이지가 자체적으로 보내는 HistoricalDataAjax 응답을 가로채서 캐시 (레거시 폴백용)
        captured_ajax: list[str] = []

        def _on_response(response: object) -> None:
            if "HistoricalDataAjax" in response.url:
                try:
                    captured_ajax.append(response.text())
                except Exception:
                    pass

        self._page.on("response", _on_response)
        try:
            resp = self._page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            if resp and resp.status == 404:
                return None
            # Cloudflare JS 챌린지 + 페이지 로드 완료 대기
            self._page.wait_for_timeout(10_000)
        except Exception as e:
            print(f"    [경고] 페이지 접근 실패 ({slug}): {e}")
            return None
        finally:
            self._page.remove_listener("response", _on_response)

        if captured_ajax:
            self._history_cache[slug] = captured_ajax[-1]

        html = self._page.content()

        # __NEXT_DATA__에서 historicalData 직접 추출 (주 방법)
        nd_data = self._extract_nd_history(html)
        if nd_data:
            self._nd_data_cache[slug] = nd_data
            print(f"    [정보] __NEXT_DATA__에서 {len(nd_data)}개 데이터 포인트 추출")
        else:
            print(f"    [경고] __NEXT_DATA__ 히스토리 미발견 ({slug})")

        pair_id = self._extract_pair_id(html)
        if pair_id:
            self._pair_id_cache[slug] = pair_id
        else:
            print(f"    [경고] pair_id 미발견 ({slug})")
            if not self._debug_html_saved:
                self._debug_html_saved = True
                debug_path = _root / "data" / "debug_investing.html"
                debug_path.parent.mkdir(parents=True, exist_ok=True)
                debug_path.write_text(html, encoding="utf-8")
                print(f"    [디버그] HTML 저장 → {debug_path}")
        return pair_id

    @staticmethod
    def _extract_pair_id(html: str) -> int | None:
        m = re.search(r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>', html, re.DOTALL)
        if m:
            try:
                nd    = json.loads(m.group(1))
                state = nd.get("props", {}).get("pageProps", {}).get("state", {})
                instrument_id = state.get("bondStore", {}).get("instrumentId")
                if instrument_id is not None:
                    v = int(instrument_id)
                    if v > 0:
                        return v
                found = GlobalTreasury._search_in_json(nd)
                if found:
                    return found
            except Exception:
                pass

        for pat in [
            r'"pair_id"\s*:\s*(\d{4,})',
            r"'pair_id'\s*:\s*(\d{4,})",
            r'data-pair-id=["\'](\d{4,})["\']',
            r'var\s+pair_id\s*=\s*(\d{4,})',
        ]:
            m2 = re.search(pat, html)
            if m2:
                return int(m2.group(1))
        return None

    @staticmethod
    def _extract_nd_history(html: str) -> list | None:
        """__NEXT_DATA__.historicalDataStore.historicalData.data 추출."""
        m = re.search(r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>', html, re.DOTALL)
        if not m:
            return None
        try:
            nd = json.loads(m.group(1))
            data = (
                nd.get("props", {})
                  .get("pageProps", {})
                  .get("state", {})
                  .get("historicalDataStore", {})
                  .get("historicalData", {})
                  .get("data", [])
            )
            return data if data else None
        except Exception:
            return None

    @staticmethod
    def _search_in_json(obj: object, _depth: int = 0) -> int | None:
        if _depth > 12:
            return None
        if isinstance(obj, dict):
            for key in ("instrumentId", "pairId", "pair_id"):
                val = obj.get(key)
                if val is not None:
                    try:
                        v = int(val)
                        if v > 1000:
                            return v
                    except (ValueError, TypeError):
                        pass
            for v in obj.values():
                r = GlobalTreasury._search_in_json(v, _depth + 1)
                if r:
                    return r
        elif isinstance(obj, list):
            for item in obj[:30]:
                r = GlobalTreasury._search_in_json(item, _depth + 1)
                if r:
                    return r
        return None

    # ── 시계열 조회 ───────────────────────────────────────────────────────────

    def _fetch_history(self, pair_id: int, slug: str, start_date: str, end_date: str) -> pd.Series | None:
        """
        우선순위:
          1. __NEXT_DATA__ 캐시 (사이트가 페이지에 직접 내장한 데이터)
          2. 응답 인터셉트 캐시 HTML (레거시, 사이트가 AJAX 발행할 때만 동작)
          3. page.evaluate()로 HistoricalDataAjax 직접 POST (더 긴 날짜 범위 필요 시)
        """
        start = pd.to_datetime(start_date)
        end   = pd.to_datetime(end_date)

        # 1순위: __NEXT_DATA__ 캐시
        nd_data = self._nd_data_cache.get(slug)
        if nd_data:
            series = self._parse_nd_series(nd_data, start, end)
            if series is not None and not series.empty:
                return series
            print(f"    [경고] __NEXT_DATA__ 데이터가 요청 기간({start_date}~{end_date}) 미포함, AJAX 시도")

        # 2순위: 인터셉트된 AJAX 캐시 (레거시)
        html_text = self._history_cache.get(slug)
        if html_text:
            series = self._parse_ajax_html(html_text, start, end)
            if series is not None and not series.empty:
                return series

        # 3순위: page.evaluate()로 HistoricalDataAjax 직접 POST
        if self._page and pair_id:
            series = self._fetch_via_ajax(pair_id, slug, start_date, end_date)
            if series is not None and not series.empty:
                return series

        print(f"    [경고] 모든 데이터 취득 방법 실패 ({slug})")
        return None

    @staticmethod
    def _parse_nd_series(data: list, start, end) -> pd.Series | None:
        """__NEXT_DATA__ historicalData.data 리스트를 pd.Series로 변환."""
        rows = []
        for item in data:
            try:
                ts = item.get("rowDateTimestamp") or item.get("rowDate")
                # last_closeRaw가 더 정밀한 값 (소수점 포함 문자열)
                price_raw = item.get("last_closeRaw") or item.get("last_close")
                if ts and price_raw is not None:
                    dt = pd.to_datetime(ts, utc=True).tz_localize(None)
                    price = float(price_raw)
                    if price != 0:
                        rows.append((dt, price))
            except Exception:
                continue
        if not rows:
            return None
        df = pd.DataFrame(rows, columns=["Date", "Price"])
        df = df.sort_values("Date")
        df = df[(df["Date"] >= start) & (df["Date"] <= end)]
        if df.empty:
            return None
        return pd.Series(df["Price"].values, index=df["Date"].dt.date, dtype=float)

    @staticmethod
    def _parse_ajax_html(html_text: str, start, end) -> pd.Series | None:
        """HistoricalDataAjax HTML 테이블을 pd.Series로 변환."""
        try:
            dfs = pd.read_html(StringIO(html_text), flavor="lxml")
            if not dfs:
                return None
            df = dfs[0]
            if "Date" not in df.columns or "Price" not in df.columns:
                return None
            df["Date"]  = pd.to_datetime(df["Date"], errors="coerce")
            df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
            df = df.dropna(subset=["Date", "Price"]).sort_values("Date", ascending=True)
            df = df[(df["Date"] >= start) & (df["Date"] <= end)]
            if df.empty:
                return None
            return pd.Series(df["Price"].values, index=df["Date"].dt.date, dtype=float)
        except Exception as e:
            print(f"    [경고] AJAX HTML 파싱 오류: {e}")
            return None

    def _fetch_via_ajax(self, pair_id: int, slug: str, start_date: str, end_date: str) -> pd.Series | None:
        """page.evaluate()로 HistoricalDataAjax에 직접 POST 요청 (더 긴 날짜 범위용)."""
        try:
            start_fmt = pd.to_datetime(start_date).strftime("%m/%d/%Y")
            end_fmt   = pd.to_datetime(end_date).strftime("%m/%d/%Y")

            js = """async (args) => {
                const resp = await fetch('/instruments/HistoricalDataAjax', {
                    method: 'POST',
                    headers: {
                        'X-Requested-With': 'XMLHttpRequest',
                        'Content-Type': 'application/x-www-form-urlencoded',
                    },
                    body: `curr_id=${args.pair_id}&st_date=${args.st}&end_date=${args.end}&action=historical_data`,
                });
                return await resp.text();
            }"""

            html_text = self._page.evaluate(js, {"pair_id": pair_id, "st": start_fmt, "end": end_fmt})
            if not html_text:
                return None

            start = pd.to_datetime(start_date)
            end   = pd.to_datetime(end_date)
            series = self._parse_ajax_html(html_text, start, end)
            if series is not None:
                print(f"    [정보] AJAX 직접 POST로 {len(series)}개 데이터 취득")
            return series
        except Exception as e:
            print(f"    [경고] AJAX 직접 요청 실패 ({slug}): {e}")
            return None

    # ── 공개 인터페이스 ───────────────────────────────────────────────────────

    def collect(self, start_date: str, end_date: str, countries: list[str] | None = None) -> pd.DataFrame | None:
        """
        investing.com에서 글로벌 국채 금리 데이터를 수집합니다.

        Args:
            start_date: "YYYY-MM-DD" (포함)
            end_date  : "YYYY-MM-DD" (포함)
            countries : 수집할 국가 코드 리스트 (예: ["JP", "CN"]). None이면 전체 수집.

        Returns:
            Date 인덱스, 컬럼명 "{CC}_{n}Y" 의 pd.DataFrame. 실패 시 None.
        """
        slugs = {cc: m for cc, m in self.BOND_SLUGS.items() if countries is None or cc in countries}
        all_series: dict[str, pd.Series] = {}
        total = sum(len(m) for m in slugs.values())
        done  = 0

        for country, maturities in slugs.items():
            print(f"  [브라우저 시작] {country}", flush=True)
            self._start_browser()
            try:
                for tenor, slug in maturities.items():
                    done += 1
                    col = f"{country}_{tenor}Y"
                    print(f"  [{done}/{total}] {col} 수집 중...", flush=True)

                    pair_id = self._get_pair_id(slug)
                    if pair_id is None:
                        print(f"    → 건너뜀 (pair_id 없음)")
                        time.sleep(0.3)
                        continue

                    time.sleep(0.5)
                    series = self._fetch_history(pair_id, slug, start_date, end_date)
                    if series is not None and not series.empty:
                        all_series[col] = series
                        print(f"    → {len(series)}행 수집 완료")
                    else:
                        print(f"    → 데이터 없음")
                    time.sleep(0.5)
            finally:
                self._stop_browser()
                print(f"  [브라우저 종료] {country}", flush=True)

        if not all_series:
            print("  [오류] 수집된 데이터 없음")
            return None

        df = pd.DataFrame(all_series)
        df.index.name = "Date"
        df = df.sort_index()

        missing = [c for c in df.columns if df[c].isna().all()]
        if missing:
            print(f"  [경고] 전체 NaN 컬럼 (미지원): {missing}")

        print(f"  기간: {start_date} ~ {end_date}  |  {len(df)}행 {len(df.columns)}열")
        return TreasuryCalc.fill_calendar(df)


if __name__ == "__main__":
    from datetime import date, timedelta

    _end   = date.today() - timedelta(days=1)
    _start = _end - timedelta(days=365)

    print(f"=== GlobalTreasury | {_start} ~ {_end} ===")
    collector = GlobalTreasury()
    df = collector.collect(start_date=str(_start), end_date=str(_end))

    if df is not None:
        save_path = _root / "data" / "global_treasury.csv"
        save_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(save_path)
        print(f"\n[저장] → {save_path}")
        print(df.tail())
    else:
        print("[실패]")
