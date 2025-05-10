# analyzer.py
import pandas as pd
import re
from typing import Dict, Any, List, Union
from datetime import datetime, timedelta
import pytz
import numpy as np

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
        
        # Print debugging info about the data
        print(f"Data columns: {list(current_data.columns)}")
        print(f"Number of rows before filtering: {len(current_data)}")
        print(f"Search parameters: {search_params}")

        # --- Time Filtering ---
        time_range = search_params.get("time_range")
        time_column = None
        
        # Find appropriate time column
        for col in ['timestamp', 'datetime']:
            if col in current_data.columns:
                time_column = col
                break
                
        if time_column and time_range:
            # Convert timestamp to datetime if not already
            if not pd.api.types.is_datetime64_any_dtype(current_data[time_column]):
                current_data[time_column] = pd.to_datetime(current_data[time_column], errors='coerce')

            start_time = time_range.get("start")
            end_time = time_range.get("end")

            try:
                # Apply time filters if they exist
                row_count_before = len(current_data)
                if start_time and pd.notna(start_time):
                    current_data = current_data[current_data[time_column] >= start_time]
                    print(f"After start time filter: {len(current_data)} rows (removed {row_count_before - len(current_data)})")
                    
                row_count_before = len(current_data)
                if end_time and pd.notna(end_time):
                    current_data = current_data[current_data[time_column] <= end_time]
                    print(f"After end time filter: {len(current_data)} rows (removed {row_count_before - len(current_data)})")
            except TypeError as e:
                 print(f"Warning: Timestamp comparison error: {e}")

        # --- Severity Filtering ---
        severity = search_params.get("severity")
        if severity is not None and "severity" in current_data.columns:
            row_count_before = len(current_data)
            # Handle both numeric and string severity
            if isinstance(severity, list):
                severity_values = [str(s) for s in severity]
                current_data = current_data[current_data["severity"].astype(str).isin(severity_values)]
            else:
                current_data = current_data[current_data["severity"].astype(str) == str(severity)]
            print(f"After severity filter: {len(current_data)} rows (removed {row_count_before - len(current_data)})")

        # --- Action Filtering (Case Insensitive) ---
        filter_action = search_params.get("action")
        if filter_action is not None and "action" in current_data.columns:
            row_count_before = len(current_data)
            # Print unique action values for debugging
            print(f"Unique action values in data: {current_data['action'].unique()}")
            print(f"Action filter value: {filter_action}")
            
            # Handle both list and single values
            if isinstance(filter_action, list):
                action_values = [str(a).upper() for a in filter_action]
                current_data = current_data[current_data["action"].astype(str).str.upper().isin(action_values)]
            else:
                current_data = current_data[current_data["action"].astype(str).str.upper() == str(filter_action).upper()]
            print(f"After action filter: {len(current_data)} rows (removed {row_count_before - len(current_data)})")

        # --- Protocol Filtering (Case Insensitive) ---
        protocol = search_params.get("protocol")
        if protocol is not None and "protocol" in current_data.columns:
            row_count_before = len(current_data)
            # Print unique protocol values for debugging
            print(f"Unique protocol values in data: {current_data['protocol'].unique()}")
            print(f"Protocol filter value: {protocol}")
            
            # Handle both list and single values
            if isinstance(protocol, list):
                protocol_values = [str(p).upper() for p in protocol]
                current_data = current_data[current_data["protocol"].astype(str).str.upper().isin(protocol_values)]
            else:
                current_data = current_data[current_data["protocol"].astype(str).str.upper() == str(protocol).upper()]
            print(f"After protocol filter: {len(current_data)} rows (removed {row_count_before - len(current_data)})")

        # --- Message ID Filtering ---
        message_id = search_params.get("message_id")
        if message_id and "message_id" in current_data.columns:
            row_count_before = len(current_data)
            # Check for regex option
            if search_params.get("use_regex", False):
                try:
                    pattern = re.compile(message_id, re.IGNORECASE if not search_params.get("case_sensitive", False) else 0)
                    current_data = current_data[current_data["message_id"].astype(str).str.contains(pattern, na=False)]
                except re.error:
                    # If regex is invalid, fall back to normal contains
                    current_data = current_data[current_data["message_id"].astype(str).str.contains(
                        message_id, 
                        case=search_params.get("case_sensitive", False), 
                        na=False
                    )]
            else:
                current_data = current_data[current_data["message_id"].astype(str).str.contains(
                    message_id, 
                    case=search_params.get("case_sensitive", False), 
                    na=False,
                    regex=False
                )]
            print(f"After message_id filter: {len(current_data)} rows (removed {row_count_before - len(current_data)})")

        # --- Hostname Filtering ---
        hostname = search_params.get("hostname")
        if hostname and "hostname" in current_data.columns:
            row_count_before = len(current_data)
            # Check for regex option
            if search_params.get("use_regex", False):
                try:
                    pattern = re.compile(hostname, re.IGNORECASE if not search_params.get("case_sensitive", False) else 0)
                    current_data = current_data[current_data["hostname"].astype(str).str.contains(pattern, na=False)]
                except re.error:
                    # If regex is invalid, fall back to normal contains
                    current_data = current_data[current_data["hostname"].astype(str).str.contains(
                        hostname, 
                        case=search_params.get("case_sensitive", False), 
                        na=False
                    )]
            else:
                current_data = current_data[current_data["hostname"].astype(str).str.contains(
                    hostname, 
                    case=search_params.get("case_sensitive", False), 
                    na=False,
                    regex=False
                )]
            print(f"After hostname filter: {len(current_data)} rows (removed {row_count_before - len(current_data)})")

        # --- Source IP Address Filtering ---
        src_ip = search_params.get("src_ip")
        if src_ip and "src_ip" in current_data.columns:
            row_count_before = len(current_data)
            # Check for regex option
            if search_params.get("use_regex", False):
                try:
                    pattern = re.compile(src_ip, re.IGNORECASE if not search_params.get("case_sensitive", False) else 0)
                    current_data = current_data[current_data["src_ip"].astype(str).str.contains(pattern, na=False)]
                except re.error:
                    # If regex is invalid, fall back to normal contains
                    current_data = current_data[current_data["src_ip"].astype(str).str.contains(
                        src_ip, 
                        case=search_params.get("case_sensitive", False), 
                        na=False
                    )]
            else:
                current_data = current_data[current_data["src_ip"].astype(str).str.contains(
                    src_ip, 
                    case=search_params.get("case_sensitive", False), 
                    na=False,
                    regex=False
                )]
            print(f"After src_ip filter: {len(current_data)} rows (removed {row_count_before - len(current_data)})")

        # --- Destination IP Address Filtering ---
        dst_ip = search_params.get("dst_ip")
        if dst_ip and "dst_ip" in current_data.columns:
            row_count_before = len(current_data)
            # Check for regex option
            if search_params.get("use_regex", False):
                try:
                    pattern = re.compile(dst_ip, re.IGNORECASE if not search_params.get("case_sensitive", False) else 0)
                    current_data = current_data[current_data["dst_ip"].astype(str).str.contains(pattern, na=False)]
                except re.error:
                    # If regex is invalid, fall back to normal contains
                    current_data = current_data[current_data["dst_ip"].astype(str).str.contains(
                        dst_ip, 
                        case=search_params.get("case_sensitive", False), 
                        na=False
                    )]
            else:
                current_data = current_data[current_data["dst_ip"].astype(str).str.contains(
                    dst_ip, 
                    case=search_params.get("case_sensitive", False), 
                    na=False,
                    regex=False
                )]
            print(f"After dst_ip filter: {len(current_data)} rows (removed {row_count_before - len(current_data)})")

        # --- Port Filtering (source or destination) ---
        src_port = search_params.get("src_port")
        if src_port and "src_port" in current_data.columns:
            row_count_before = len(current_data)
            current_data = current_data[current_data["src_port"].astype(str) == str(src_port)]
            print(f"After src_port filter: {len(current_data)} rows (removed {row_count_before - len(current_data)})")

        dst_port = search_params.get("dst_port")
        if dst_port and "dst_port" in current_data.columns:
            row_count_before = len(current_data)
            current_data = current_data[current_data["dst_port"].astype(str) == str(dst_port)]
            print(f"After dst_port filter: {len(current_data)} rows (removed {row_count_before - len(current_data)})")

        # --- Text Search (in message field) ---
        message_text = search_params.get("message_text")
        if message_text and "message" in current_data.columns:
            row_count_before = len(current_data)
            # Check for regex option
            if search_params.get("use_regex", False):
                try:
                    pattern = re.compile(message_text, re.IGNORECASE if not search_params.get("case_sensitive", False) else 0)
                    current_data = current_data[current_data["message"].astype(str).str.contains(pattern, na=False)]
                except re.error:
                    # If regex is invalid, fall back to normal contains
                    current_data = current_data[current_data["message"].astype(str).str.contains(
                        message_text, 
                        case=search_params.get("case_sensitive", False), 
                        na=False
                    )]
            else:
                current_data = current_data[current_data["message"].astype(str).str.contains(
                    message_text, 
                    case=search_params.get("case_sensitive", False), 
                    na=False,
                    regex=False
                )]
            print(f"After message text filter: {len(current_data)} rows (removed {row_count_before - len(current_data)})")

        # --- Full Text Search (across all string columns) ---
        full_text = search_params.get("full_text")
        if full_text:
            row_count_before = len(current_data)
            text_mask = pd.Series(False, index=current_data.index)
            
            # Select only object/string columns to search in
            string_columns = current_data.select_dtypes(include=['object', 'string']).columns
            print(f"Searching for '{full_text}' in columns: {list(string_columns)}")
            
            # Check for regex option
            if search_params.get("use_regex", False):
                try:
                    pattern = re.compile(full_text, re.IGNORECASE if not search_params.get("case_sensitive", False) else 0)
                    for col in string_columns:
                        text_mask |= current_data[col].astype(str).str.contains(pattern, na=False)
                except re.error:
                    # If regex is invalid, fall back to normal contains
                    for col in string_columns:
                        text_mask |= current_data[col].astype(str).str.contains(
                            full_text, 
                            case=search_params.get("case_sensitive", False), 
                            na=False
                        )
            else:
                for col in string_columns:
                    text_mask |= current_data[col].astype(str).str.contains(
                        full_text, 
                        case=search_params.get("case_sensitive", False), 
                        na=False,
                        regex=False
                    )
            
            current_data = current_data[text_mask]
            print(f"After full text search: {len(current_data)} rows (removed {row_count_before - len(current_data)})")

        # Apply results limit if specified
        results_limit = search_params.get("results_limit")
        if results_limit is not None and len(current_data) > results_limit:
            print(f"Limiting results to {results_limit} rows (from {len(current_data)})")
            current_data = current_data.head(results_limit)

        print(f"Final result: {len(current_data)} rows")
        return current_data

    def generate_summary(self, results: pd.DataFrame, search_params: Dict[str, Any]) -> Dict[str, Any]:
        """ Generates a summary dictionary from the analysis results. """
        summary = {
            "total_logs": len(results),
            "time_range": search_params.get("time_range", {}),
            "query_params": {k: v for k, v in search_params.items() if k not in ["time_range", "count_request", "viz_type", "results_limit", "case_sensitive", "use_regex"] and v is not None},
            "viz_type": search_params.get("viz_type", "auto")
        }

        # Find time column
        time_column = None
        for col in ['timestamp', 'datetime']:
            if col in results.columns:
                time_column = col
                break
                
        # Time-based summary
        if time_column and not results[time_column].isna().all():
            try:
                # Ensure timestamp is datetime type
                if not pd.api.types.is_datetime64_any_dtype(results[time_column]):
                    results[time_column] = pd.to_datetime(results[time_column], errors='coerce')
                
                summary["earliest_log"] = results[time_column].min()
                summary["latest_log"] = results[time_column].max()
                
                if pd.notna(summary["earliest_log"]) and pd.notna(summary["latest_log"]):
                    summary["time_span_hours"] = (summary["latest_log"] - summary["earliest_log"]).total_seconds() / 3600
                else:
                    summary["time_span_hours"] = 0
            except Exception as e:
                print(f"Error calculating time summary: {e}")
                summary["time_span_hours"] = "Error"

        # Add distribution summaries for key columns
        for col in ["action", "protocol", "severity", "hostname", "message_id"]:
            if col in results.columns and not results[col].isna().all():
                values = results[col].astype(str).value_counts().to_dict()
                # Filter out empty values
                filtered_values = {k: v for k, v in values.items() if k and k.strip()}
                if filtered_values:
                    summary[f"{col}_distribution"] = filtered_values

        # Add IP statistics
        for ip_col in ["src_ip", "dst_ip"]:
            if ip_col in results.columns and not results[ip_col].isna().all():
                values = results[ip_col].astype(str).value_counts().head(10).to_dict()
                # Filter out empty values
                filtered_values = {k: v for k, v in values.items() if k and k.strip()}
                if filtered_values:
                    summary[f"top_{ip_col}"] = filtered_values

        # Add time-based counts if requested
        if search_params.get("count_request", True) and time_column and not results[time_column].isna().all():
            try:
                # Daily counts
                daily_counts = results.groupby(results[time_column].dt.date).size()
                summary["daily_counts"] = {str(date): count for date, count in daily_counts.items()}

                # Hourly pattern
                hourly_counts = results.groupby(results[time_column].dt.hour).size()
                summary["hourly_pattern"] = {str(hour): count for hour, count in hourly_counts.items()}
                
                # Add weekday distribution
                weekday_counts = results.groupby(results[time_column].dt.day_name()).size()
                summary["weekday_distribution"] = weekday_counts.to_dict()
                
                # Add min, max and average counts per day
                if len(daily_counts) > 0:
                    summary["daily_min"] = daily_counts.min()
                    summary["daily_max"] = daily_counts.max()
                    summary["daily_avg"] = daily_counts.mean()
                else:
                    summary["daily_min"] = 0
                    summary["daily_max"] = 0
                    summary["daily_avg"] = 0
                
            except Exception as e:
                print(f"Error calculating count metrics: {e}")
                summary["count_metrics_error"] = str(e)

        # Add port statistics
        for port_col in ["src_port", "dst_port"]:
            if port_col in results.columns and not results[port_col].isna().all():
                values = results[port_col].astype(str).value_counts().head(10).to_dict()
                # Filter out empty values
                filtered_values = {k: v for k, v in values.items() if k and k.strip()}
                if filtered_values:
                    summary[f"top_{port_col}"] = filtered_values

        # Add protocol statistics by action
        if "protocol" in results.columns and "action" in results.columns:
            try:
                cross_table = pd.crosstab(results["protocol"], results["action"])
                protocol_by_action = {}
                
                for protocol in cross_table.index:
                    protocol_by_action[str(protocol)] = {
                        str(action): int(count) 
                        for action, count in cross_table.loc[protocol].items()
                    }
                
                summary["protocol_by_action"] = protocol_by_action
            except Exception as e:
                print(f"Error calculating protocol by action: {e}")

        return summary
        
    def get_unique_values(self, df: pd.DataFrame, column: str, limit: int = 100) -> List[str]:
        """Get unique values from a DataFrame column with limit"""
        if column not in df.columns:
            return []
            
        try:
            values = df[column].astype(str).dropna().unique().tolist()
            # Filter out empty strings
            values = [v for v in values if v and v.strip()]
            # Sort and limit
            return sorted(values)[:limit]
        except Exception as e:
            print(f"Error getting unique values for {column}: {e}")
            return []
            
    def get_column_statistics(self, df: pd.DataFrame) -> Dict[str, Dict[str, Union[int, List[str]]]]:
        """Get basic statistics for each column in the DataFrame"""
        stats = {}
        
        for col in df.columns:
            col_stats = {
                "count": df[col].count(),
                "null_count": df[col].isna().sum(),
            }
            
            # For non-numeric columns, get value counts
            if df[col].dtype == 'object' or df[col].dtype == 'string':
                value_counts = df[col].value_counts().head(10).to_dict()
                col_stats["top_values"] = value_counts
                col_stats["unique_count"] = df[col].nunique()
            
            # For numeric columns, get min, max, mean
            elif pd.api.types.is_numeric_dtype(df[col]):
                col_stats["min"] = df[col].min()
                col_stats["max"] = df[col].max()
                col_stats["mean"] = df[col].mean()
                
            # For datetime columns
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                if not df[col].isna().all():
                    col_stats["min"] = df[col].min()
                    col_stats["max"] = df[col].max()
                    col_stats["range_days"] = (df[col].max() - df[col].min()).total_seconds() / 86400
            
            stats[col] = col_stats
            
        return stats