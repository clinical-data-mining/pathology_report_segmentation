"""
pathology_extract_pdl1.py

This module extracts PD-L1 related annotations from a pathology reports DataFrame.
Input is a DataFrame; no reading/writing happens here.
"""

import re
import pandas as pd
import numpy as np
from msk_cdm.data_processing import mrn_zero_pad, drop_cols

# -------------------------
# Constants (module-level)
# -------------------------

COLS_DROP = [
    "PATH_RPT_ID",
    "ASSOCIATED_PATH_REPORT_ID",
    "DMP_ID",
    "SAMPLE_ID",
    "SPECIMEN_SUBMISSION_LIST",
    "PATH_REPORT_TYPE_GENERAL",
    "RPT_CHAR_LEN",
]

COLS_LOGICALS = [
    "HAS_PDL1_POS",
    "HAS_PDL1_NEG",
    "HAS_PDL1_PERC",
    "HAS_PDL1_CPS_1",
    "HAS_PDL1_CPS_2",
    "HAS_PDL1_TPS_1",
    "HAS_PDL1_TPS_2",
    "HAS_PDL1_IPS",
]

COLS_EXTRACT = [
    "PDl1_PERCENTAGE",
    "PDl1_CPS_1",
    "PDl1_CPS_2",
    "PDl1_TPS_1",
    "PDl1_TPS_2",
    "PDl1_IPS",
]

COL_TEXT_SPLIT = "PDL1_TEXT"
COL_TEXT_SPLIT_DISPLAY = "PDL1_TEXT_DISPLAY"
COL_MENTIONS_PDL1 = "MENTIONS_PDL1"

TEXT_BUFFER = 200  # characters captured after each "PD-L1" mention

# Regex patterns
REGEX_PDL1 = r"pd[-]*l[-]*1"
REGEX_PERC = r"([<>=]*[ ]*[\d+\.\d+-]*\d+[\.\d+]*?[ ]*%)"
REGEX_DIGIT_MATCH = r"([<>=]*\d+[\.\d+]*[/]*[\d]*)"
REGEX_NEG = r"(?:no[\s]*labeling[\s]*is[\s]*seen[\s]*in[\s]*tumor[\s]*cells|negative)"
REGEX_CPS_1 = r"([<>=]*\d+[\.\d]*?)(?=\s*\D*of 100)"
REGEX_CPS_2 = r"combined[\s]*positiv[eity]+[\s]*score[^><=0-9]*"
REGEX_TPS_1 = r"tumor[\s]*proportion[\s]*score[^><=0-9]*"
REGEX_TPS_2 = r"[percentagecontribution]{10,12}[\s]*of[\s]*tumor[\s]*cells[^><=0-9]*"
REGEX_IPS = r"[percentagecontribution]{10,12}[\s]*of[\s]*[inflammatoryimmune]{6,12}[^><=0-9]*"
REGEX_POS = r"positive \(cps >=1|positive \(>=1"


class PathologyExtractPDL1Epic:
    """
    Extracts PD-L1 related annotations from pathology reports.

    This class accepts a DataFrame of pathology reports, processes the text field
    for PD-L1 mentions, extracts relevant numeric scores (percentage, CPS, TPS, IPS),
    flags antibody usage, and outputs a cleaned DataFrame with standardized annotations.

    Example:
        extractor = PathologyExtractPDL1(df_pathology_reports, col_text="REPORT_TEXT")
        df_out = extractor.return_extraction()

    Attributes:
        _df_pathology_reports (pd.DataFrame): Input DataFrame of pathology reports.
        _col_text (str): Name of the text column containing report text.
        _df_extracted (pd.DataFrame): Final processed DataFrame with annotations.
    """

    def __init__(self, df_pathology_reports: pd.DataFrame, col_text: str):
        """
        Initialize extractor with input DataFrame and text column.

        Args:
            df_pathology_reports (pd.DataFrame): Input DataFrame of reports.
            col_text (str): Column containing report text to analyze.
        """
        self._df_pathology_reports = df_pathology_reports.copy()
        self._col_text = col_text
        self._df_extracted = None
        self._process_data()

    def return_extraction(self) -> pd.DataFrame:
        """
        Return the processed DataFrame with PD-L1 annotations.

        Returns:
            pd.DataFrame: DataFrame with new columns for PD-L1 mentions, extracted values,
            antibody annotations, and positivity flag.
        """
        return self._df_extracted

    def _process_data(self):
        """
        Orchestrates the full PD-L1 extraction pipeline:
        - Normalize input
        - Add PD-L1 mention flags and captured text
        - Add antibody annotations
        - Add regex availability flags
        - Extract values by regex
        - Flag rows needing review
        - Drop unused columns
        - Compute PDL1_POSITIVE flag
        """
        df = self._normalize_input(self._df_pathology_reports)
        df = self._add_pdl1_mention(df)
        df = self._add_antibody(df)
        df = self._add_regex_available(df)
        df = self._add_annotations(df)
        df = self._add_needs_review_anno(df)
        df = drop_cols(df=df, cols=COLS_DROP)
        df = df[df[COL_MENTIONS_PDL1] == True]
        df = self._combine_pdl1_score(df_pdl1=df)
        self._df_extracted = df

    def _normalize_input(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Lowercase text column and zero-pad MRN if present.

        Args:
            df (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: Normalized DataFrame.
        """
        if self._col_text not in df.columns:
            raise KeyError(f"Column '{self._col_text}' not found in input DataFrame.")
        df[self._col_text] = df[self._col_text].astype(str).str.lower()
        if "MRN" in df.columns:
            df = mrn_zero_pad(df=df, col_mrn="MRN")
        return df

    def _add_pdl1_mention(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Flag rows mentioning PD-L1 and extract text snippets around matches.

        Args:
            df (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: DataFrame with new columns:
                - MENTIONS_PDL1
                - PDL1_TEXT
                - PDL1_TEXT_DISPLAY
        """
        has_pdl1 = df[self._col_text].str.contains(REGEX_PDL1, regex=True, na=False)

        def get_pdl1_text(x: str) -> str:
            parts = re.split(REGEX_PDL1, x)
            return "".join(["MARKER" + y[:TEXT_BUFFER] for y in parts[1:]])

        def get_pdl1_text_display(x: str) -> str:
            parts = re.split(REGEX_PDL1, x)
            return "".join(["PD-L1" + y[:TEXT_BUFFER] for y in parts[1:]])

        text = df.loc[has_pdl1, self._col_text].apply(get_pdl1_text).rename(COL_TEXT_SPLIT)
        text_display = df.loc[has_pdl1, self._col_text].apply(get_pdl1_text_display).rename(COL_TEXT_SPLIT_DISPLAY)

        df = df.assign(**{COL_MENTIONS_PDL1: has_pdl1})
        df = pd.concat([df, text, text_display], axis=1, sort=False)
        return df

    def _add_regex_available(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add boolean flags indicating presence of different PD-L1 scoring patterns.

        Args:
            df (pd.DataFrame): DataFrame after _add_pdl1_mention.

        Returns:
            pd.DataFrame: DataFrame with boolean columns defined in COLS_LOGICALS.
        """
        pattern_neg = re.compile(REGEX_NEG)
        pattern_cps_2 = re.compile(REGEX_CPS_2)
        pattern_tps_1 = re.compile(REGEX_TPS_1)
        pattern_tps_2 = re.compile(REGEX_TPS_2)
        pattern_ips = re.compile(REGEX_IPS)
        pattern_pos = re.compile(REGEX_POS)

        series_text = df.loc[df[COL_MENTIONS_PDL1] == True, COL_TEXT_SPLIT].fillna("")

        def flags(y: str):
            return [
                pattern_pos.search(y) is not None,
                pattern_neg.search(y) is not None,
                "%" in y,
                "of 100" in y,
                pattern_cps_2.search(y) is not None,
                pattern_tps_1.search(y) is not None,
                pattern_tps_2.search(y) is not None,
                pattern_ips.search(y) is not None,
            ]

        logic_available = series_text.apply(flags)
        df_available = pd.DataFrame(logic_available.tolist(), index=logic_available.index, columns=COLS_LOGICALS)

        df_out = pd.concat([df, df_available], axis=1, sort=False)
        df_out[COLS_LOGICALS] = df_out[COLS_LOGICALS].fillna(False)
        return df_out

    def _add_anno_template(
        self,
        df: pd.DataFrame,
        *,
        regex_pattern: str,
        col_text: str,
        col_rename: str,
        col_filter_by: str,
    ) -> pd.DataFrame:
        """
        Generic helper to extract regex matches into a new column.

        Args:
            df (pd.DataFrame): Input DataFrame.
            regex_pattern (str): Regex pattern to apply.
            col_text (str): Column containing text to search.
            col_rename (str): Name for new column.
            col_filter_by (str): Boolean column name used to filter rows.

        Returns:
            pd.DataFrame: DataFrame with new column of list[str] matches.
        """
        mask = df[col_filter_by] == True
        extracted = df.loc[mask, col_text].fillna("").apply(lambda z: re.findall(regex_pattern, z)).rename(col_rename)
        return pd.concat([df, extracted], axis=1, sort=False)

    def _add_annotations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract PD-L1 score values (percentages, CPS, TPS, IPS) from text.

        Args:
            df (pd.DataFrame): DataFrame with regex availability flags.

        Returns:
            pd.DataFrame: DataFrame with extracted value columns.
        """
        df = self._add_anno_template(df, regex_pattern=REGEX_PERC, col_filter_by="HAS_PDL1_PERC",
                                     col_text=COL_TEXT_SPLIT, col_rename="PDl1_PERCENTAGE")
        df = self._add_anno_template(df, regex_pattern=REGEX_CPS_1, col_filter_by="HAS_PDL1_CPS_1",
                                     col_text=COL_TEXT_SPLIT, col_rename="PDl1_CPS_1")
        df = self._add_anno_template(df, regex_pattern=REGEX_CPS_2 + REGEX_DIGIT_MATCH, col_filter_by="HAS_PDL1_CPS_2",
                                     col_text=COL_TEXT_SPLIT, col_rename="PDl1_CPS_2")
        df = self._add_anno_template(df, regex_pattern=REGEX_TPS_1 + REGEX_DIGIT_MATCH, col_filter_by="HAS_PDL1_TPS_1",
                                     col_text=COL_TEXT_SPLIT, col_rename="PDl1_TPS_1")
        df = self._add_anno_template(df, regex_pattern=REGEX_TPS_2 + REGEX_DIGIT_MATCH, col_filter_by="HAS_PDL1_TPS_2",
                                     col_text=COL_TEXT_SPLIT, col_rename="PDl1_TPS_2")
        df = self._add_anno_template(df, regex_pattern=REGEX_IPS + REGEX_DIGIT_MATCH, col_filter_by="HAS_PDL1_IPS",
                                     col_text=COL_TEXT_SPLIT, col_rename="PDl1_IPS")
        return df

    def _add_antibody(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Flag antibody clone mentions and mask them in split text.

        Args:
            df (pd.DataFrame): DataFrame after _add_pdl1_mention.

        Returns:
            pd.DataFrame: DataFrame with antibody flags and updated PDL1_TEXT.
        """
        df["E1L3N"] = df[self._col_text].str.contains(r"\be1l3n\b", na=False)
        df["SP-142"] = df[self._col_text].str.contains(r"sp-142|sp142", regex=True, na=False)
        df["SP-263"] = df[self._col_text].str.contains(r"sp-263|sp263", regex=True, na=False)
        df["CD-2764"] = df[self._col_text].str.contains(r"cd2764", regex=True, na=False)
        df["22C3"] = df[self._col_text].str.contains(r"\b22c3\b", regex=True, na=False)

        if COL_TEXT_SPLIT in df.columns:
            df[COL_TEXT_SPLIT] = df[COL_TEXT_SPLIT].str.replace(
                r"e1l3n|sp-142|sp142|sp-263|sp263|cd2764|22c3", "ANTIBODY", regex=True
            )
        return df

    def _add_needs_review_anno(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Flag rows where PD-L1 was mentioned but no extractable data was found.

        Args:
            df (pd.DataFrame): DataFrame after annotation extraction.

        Returns:
            pd.DataFrame: DataFrame with DEBUG_REQUIRED boolean column.
        """
        l1 = df[COL_MENTIONS_PDL1] == True
        l2 = df.get("HAS_PDL1_POS", False) == False
        l3 = df.get("HAS_PDL1_NEG", False) == False
        l4 = df.get("PDl1_PERCENTAGE").isnull()
        l5 = df.get("PDl1_CPS_1").isnull()
        l6 = df.get("PDl1_CPS_2").isnull()
        l7 = df.get("PDl1_TPS_1").isnull()
        l8 = df.get("PDl1_TPS_2").isnull()
        l10 = df.get("PDl1_IPS").isnull()

        if "PATH_REPORT_TYPE" in df.columns:
            l9 = df["PATH_REPORT_TYPE"].fillna("").str.contains("Surg", na=False)
        else:
            l9 = pd.Series(False, index=df.index)

        logic_missing = l1 & l2 & l3 & l4 & l5 & l6 & l7 & l8 & l9 & l10
        df = df.assign(DEBUG_REQUIRED=logic_missing)
        return df

    def _combine_pdl1_score(self, df_pdl1: pd.DataFrame) -> pd.DataFrame:
        """
        Convert extracted PD-L1 score strings into numeric estimates and compute positivity.

        Args:
            df_pdl1 (pd.DataFrame): DataFrame with extracted value columns.

        Returns:
            pd.DataFrame: DataFrame with *_EST numeric columns and PDL1_POSITIVE flag.
        """
        df = df_pdl1.copy()

        for col_current in COLS_EXTRACT:
            new_col = f"{col_current}_EST"
            has_vals = df[col_current].apply(lambda x: isinstance(x, list) and len(x) > 0)
            idx = has_vals[has_vals].index

            pct_current = (
                pd.Series(index=idx, data=[df.at[i, col_current][0] for i in idx])
                .astype(str)
                .str.strip()
            )
            pct_current = (
                pct_current.str.replace("%", "", regex=False)
                .str.replace(" ", "", regex=False)
                .str.replace("<1", "0", regex=False)
                .str.replace(">1", "1", regex=False)
                .str.replace(">=1", "1", regex=False)
                .str.replace(">", "", regex=False)
                .str.replace("<", "", regex=False)
                .str.replace("=", "", regex=False)
            )
            is_range = pct_current.str.contains("-", regex=False)
            pct_current.loc[is_range] = pct_current.loc[is_range].str.split("-").str[-1]

            pct_num = pd.to_numeric(pct_current, errors="coerce")
            df[new_col] = np.nan
            df.loc[idx, new_col] = pct_num.values

        pos_logic = (
            (df.get("PDl1_PERCENTAGE_EST") >= 1)
            | (df.get("PDl1_TPS_1_EST") >= 1)
            | (df.get("PDl1_TPS_2_EST") >= 1)
        )
        all_null = (
            df.get("PDl1_PERCENTAGE_EST").isnull()
            & df.get("PDl1_TPS_1_EST").isnull()
            & df.get("PDl1_TPS_2_EST").isnull()
        )

        df["PDL1_POSITIVE"] = np.where(pos_logic, "Yes", "No")
        df.loc[all_null, "PDL1_POSITIVE"] = np.nan
        return df
