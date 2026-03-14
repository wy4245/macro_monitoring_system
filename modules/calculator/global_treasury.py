"""
글로벌 국채 / 병합 데이터 분석

TreasuryCalc
  fill_calendar(df)              : 전체 달력 날짜로 reindex 후 forward fill
  merge(global_df, kr_df)        : GlobalTreasury + KOFIA 데이터 병합
  get_ref_value(df, ref_date)    : 기준일 이하 가장 가까운 행 반환
  build_change_summary(df, ...)  : 2Y/10Y 금리 + 1D/1W/MTD/YTD/YoY bp 요약 테이블
"""

import pandas as pd


class TreasuryCalc:
    """글로벌 국채 + KOFIA 병합 데이터의 분석 및 요약."""

    @staticmethod
    def fill_calendar(df: pd.DataFrame) -> pd.DataFrame:
        """
        주말·공휴일을 포함한 전체 달력 날짜(일별)로 reindex 후 forward fill.

        Args:
            df: Date 인덱스(date 또는 datetime)를 가진 DataFrame

        Returns:
            전체 달력 날짜로 확장·fill된 DataFrame
        """
        df = df.copy()
        df.index = pd.to_datetime(df.index)
        full = pd.date_range(df.index.min(), df.index.max(), freq="D")
        df = df.reindex(full)
        df = df.ffill()
        df.index.name = "Date"
        return df

    @staticmethod
    def merge(global_df: pd.DataFrame, kr_df: pd.DataFrame) -> pd.DataFrame:
        """
        GlobalTreasury + KOFIA 데이터를 outer join 후 ffill.

        Args:
            global_df: GlobalTreasury.collect() 반환 DataFrame
            kr_df    : KofiaCalc.standardize() 적용 완료 DataFrame

        Returns:
            전체 달력 날짜 기준으로 정렬된 병합 DataFrame
        """
        g = global_df.copy()
        g.index = pd.to_datetime(g.index)

        k = kr_df.copy()
        k.index = pd.to_datetime(k.index)

        merged = g.join(k, how="outer")
        merged = merged.ffill()
        merged = merged.sort_index()
        return merged

    @staticmethod
    def get_ref_value(df: pd.DataFrame, ref_date) -> pd.Series:
        """
        ref_date 이하 가장 가까운 날짜의 행을 반환.

        Args:
            df      : DatetimeIndex를 가진 DataFrame
            ref_date: 기준 날짜 (date / datetime / str)

        Returns:
            해당 날짜의 pd.Series. 이전 데이터가 없으면 NaN Series.
        """
        avail = df.index[df.index <= pd.Timestamp(ref_date)]
        if len(avail) == 0:
            return pd.Series(float("nan"), index=df.columns, dtype=float)
        return df.loc[avail[-1]]

    @staticmethod
    def build_change_summary(df: pd.DataFrame, target_date=None) -> pd.DataFrame:
        """
        2Y / 10Y 금리와 1D / 1W / MTD / YTD / YoY 변화량(bp) 요약 테이블 생성.

        Args:
            df         : DatetimeIndex, 컬럼 '{CC}_{n}Y' 형식의 병합 DataFrame
            target_date: 기준일 (없으면 df의 마지막 날짜 사용)

        Returns:
            MultiIndex DataFrame
            - Index: 미국, 한국, 독일, 영국, 일본, 중국
            - Columns: (2년물, 금리 (%)), (2년물, 1D), ... (10년물, YoY)
        """
        today = df.index.max() if target_date is None else pd.Timestamp(target_date)

        today_vals = TreasuryCalc.get_ref_value(df, today)

        ref_infos = [
            ("1D",  today - pd.Timedelta(days=1)),
            ("1W",  today - pd.Timedelta(days=7)),
            ("MTD", pd.Timestamp(today.year, today.month, 1) - pd.Timedelta(days=1)),
            ("MoM", today - pd.DateOffset(months=1)),
            ("YTD", pd.Timestamp(today.year - 1, 12, 31)),
            ("YoY", today - pd.DateOffset(years=1)),
        ]

        country_map    = {"US": "미국", "KR": "한국", "DE": "독일", "GB": "영국", "JP": "일본", "CN": "중국"}
        ordered_codes  = ["US", "KR", "DE", "GB", "JP", "CN"]

        data: dict = {}
        for code in ordered_codes:
            c_name = country_map.get(code, code)
            data[c_name] = {}
            for tenor in [2, 10]:
                tenor_label = f"{tenor}년물"
                col_key     = f"{code}_{tenor}Y"
                curr        = today_vals.get(col_key, float("nan")) if col_key in today_vals.index else float("nan")
                data[c_name][(tenor_label, "금리 (%)")] = curr
                for label, ref_date in ref_infos:
                    ref_vals = TreasuryCalc.get_ref_value(df, ref_date)
                    ref      = ref_vals.get(col_key, float("nan")) if col_key in ref_vals.index else float("nan")
                    diff     = (curr - ref) * 100 if pd.notna(curr) and pd.notna(ref) else float("nan")
                    data[c_name][(tenor_label, label)] = diff

        df_result = pd.DataFrame.from_dict(data, orient="index")

        cols = []
        for t in ["2년물", "10년물"]:
            cols.append((t, "금리 (%)"))
            for label, _ in ref_infos:
                cols.append((t, label))

        df_result.columns = pd.MultiIndex.from_tuples(df_result.columns)
        df_result = df_result[cols]
        df_result.index.name = "구분"
        return df_result
