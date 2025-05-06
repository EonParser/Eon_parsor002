# analyzer.py
import pandas as pd
import re # Import re for keyword escaping
from typing import Dict, Any

class LogAnalyzer:
    def __init__(self):
        # No internal state needed here if analyze operates purely on input
        pass

    def analyze(self, log_data: pd.DataFrame, search_params: Dict[str, Any]) -> pd.DataFrame:
        """
        Applies filters to the log data based on search parameters.
        Assumes log_data columns are lowercase.
        """
        if log_data is None or log_data.empty:
            return pd.DataFrame() # Return empty if input is empty

        # Make a copy to avoid modifying the original DataFrame slice
        current_data = log_data.copy()

        # --- Time Filtering ---
        time_range = search_params.get("time_range")
        if "datetime" in current_data.columns and \
           pd.api.types.is_datetime64_any_dtype(current_data["datetime"]) and \
           time_range:

            start_time = time_range.get("start")
            end_time = time_range.get("end")

            # Ensure times are timezone-aware (should be UTC from parser)
            # Add error handling in case conversion failed upstream
            try:
                if start_time and pd.notna(start_time):
                    current_data = current_data[current_data["datetime"] >= start_time]
                if end_time and pd.notna(end_time):
                    current_data = current_data[current_data["datetime"] <= end_time]
            except TypeError as e:
                 print(f"Warning: Timestamp comparison error (likely timezone mismatch): {e}")


        # --- Log Type Filtering (Exact Match, Case-Insensitive) ---
        filter_log_type = search_params.get("log_type")
        if filter_log_type and "log_type" in current_data.columns:
            # Column names are already lowercased by parser
            # Ensure the column is string type before using .str accessor
            if pd.api.types.is_string_dtype(current_data["log_type"]):
                 # Use .astype(str) to handle potential mixed types safely before lower()
                current_data = current_data[
                    current_data["log_type"].astype(str).str.lower() == filter_log_type.lower()
                ]

        # --- Action Filtering (Exact Match, Case-Insensitive) ---
        filter_action = search_params.get("action")
        if filter_action and "action" in current_data.columns:
            if pd.api.types.is_string_dtype(current_data["action"]):
                 current_data = current_data[
                    current_data["action"].astype(str).str.lower() == filter_action.lower()
                ]

        # --- IP Address Filtering (Substring Match in relevant columns) ---
        ip_filter = search_params.get("ip_address")
        if ip_filter:
            # Find columns likely containing IP addresses (already lowercase)
            ip_columns = [col for col in current_data.columns if 'ip' in col]
            ip_mask = pd.Series(False, index=current_data.index) # Initialize mask

            for col in ip_columns:
                 # Ensure column is string type and handle potential NAs before .str.contains
                if pd.api.types.is_string_dtype(current_data[col]):
                    ip_mask |= current_data[col].astype(str).str.contains(ip_filter, case=False, na=False, regex=False) # Use regex=False for plain substring

            # Apply the combined mask if any matches were found
            if ip_mask.any():
                current_data = current_data[ip_mask]
            else:
                # If the filter was specified but nothing matched, return an empty DataFrame
                current_data = current_data.iloc[0:0]


        # --- User Filtering (Substring Match in relevant columns) ---
        user_filter = search_params.get("user")
        if user_filter:
            user_columns = [col for col in current_data.columns if 'user' in col] # e.g., 'username', 'user_id'
            user_mask = pd.Series(False, index=current_data.index) # Initialize mask

            for col in user_columns:
                if pd.api.types.is_string_dtype(current_data[col]):
                    user_mask |= current_data[col].astype(str).str.contains(user_filter, case=False, na=False, regex=False)

            if user_mask.any():
                current_data = current_data[user_mask]
            else:
                current_data = current_data.iloc[0:0]


        # --- Keyword Filtering (Substring Match across all string columns) ---
        # More flexible now CSV structure is known but varied
        keywords = search_params.get("keywords")
        if keywords:
            keyword_pattern = '|'.join(map(re.escape, keywords)) # Escape keywords for regex OR logic
            keyword_mask = pd.Series(False, index=current_data.index)

            # Select only object/string columns to search in
            string_columns = current_data.select_dtypes(include=['object', 'string']).columns

            for col in string_columns:
                 # Ensure consistent string conversion and handle NAs
                 keyword_mask |= current_data[col].astype(str).str.contains(keyword_pattern, case=False, na=False, regex=True)

            if keyword_mask.any():
                 current_data = current_data[keyword_mask]
            else:
                 current_data = current_data.iloc[0:0]


        return current_data

    def generate_summary(self, results: pd.DataFrame, search_params: Dict[str, Any]) -> Dict[str, Any]:
        """ Generates a summary dictionary from the analysis results. """
        summary = {
            "total_logs": len(results),
            "time_range": search_params.get("time_range", {}),
            "query": search_params.get("original_query", ""),
            "keywords": search_params.get("keywords", []),
            "viz_type": search_params.get("viz_type", "table")
        }

        # Use .get() for safer access to column names which might vary by CSV
        dt_col = 'datetime' # Assume this is the standardized name
        if dt_col in results.columns and not results[dt_col].isna().all():
            try:
                summary["earliest_log"] = results[dt_col].min()
                summary["latest_log"] = results[dt_col].max()
                if pd.notna(summary["earliest_log"]) and pd.notna(summary["latest_log"]):
                     summary["time_span_hours"] = (summary["latest_log"] - summary["earliest_log"]).total_seconds() / 3600
                else:
                    summary["time_span_hours"] = 0
            except Exception as e:
                 print(f"Error calculating time summary: {e}")
                 summary["time_span_hours"] = "Error"


        lt_col = 'log_type' # Assuming column exists
        if lt_col in results.columns and not results[lt_col].isna().all():
            summary["log_type_distribution"] = results[lt_col].astype(str).value_counts().to_dict()

        act_col = 'action' # Assuming column exists
        if act_col in results.columns and not results[act_col].isna().all():
            summary["action_distribution"] = results[act_col].astype(str).value_counts().to_dict()

        # Find IP columns (lowercase assumed)
        ip_columns = [col for col in results.columns if 'ip' in col]
        if ip_columns:
            valid_ip_cols = [col for col in ip_columns if not results[col].isna().all()]
             # Generate top 10 value counts for each valid IP column
            ip_data = {col: results[col].astype(str).value_counts().head(10).to_dict() for col in valid_ip_cols}
            if ip_data:
                summary["top_ip_addresses"] = ip_data # Changed key name for clarity

        if search_params.get("count_request"):
            summary["count_metrics"] = True
            if dt_col in results.columns and not results[dt_col].isna().all() and not results.empty:
                try:
                    # Ensure datetime column is used for grouping
                    daily_counts = results.groupby(results[dt_col].dt.date).size()
                    summary["daily_counts"] = {str(date): count for date, count in daily_counts.items()} # Convert date keys to string for JSON compatibility

                    hourly_counts = results.groupby(results[dt_col].dt.hour).size()
                    summary["hourly_pattern"] = {str(hour): count for hour, count in hourly_counts.items()} # Convert hour keys to string

                except Exception as e:
                    print(f"Error calculating count metrics: {e}")
                    summary["daily_counts"] = {"Error": str(e)}
                    summary["hourly_pattern"] = {"Error": str(e)}

        return summary