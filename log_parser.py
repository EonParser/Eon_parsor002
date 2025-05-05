# log_parser.py
import pandas as pd
import os
from pytz import utc
from typing import List, Optional

class LogParser:
    def __init__(self):
        # No longer need regex patterns
        pass

    def _standardize_timestamps(self, df: pd.DataFrame, time_column: Optional[str]) -> None:
        """
        Attempts to find and convert a timestamp column to timezone-aware UTC datetime objects.
        """
        if time_column and time_column in df.columns:
            try:
                # Convert the specified column, handling potential errors
                df['datetime'] = pd.to_datetime(df[time_column], errors='coerce')

                # Check if timezone aware, if not, assume UTC (or local, then convert - UTC is safer)
                if df['datetime'].dt.tz is None:
                    # Option 1: Assume the times are UTC already
                    df['datetime'] = df['datetime'].dt.tz_localize(utc)
                    # Option 2: Assume local time and convert to UTC (Requires careful consideration)
                    # df['datetime'] = df['datetime'].dt.tz_localize('local').dt.tz_convert(utc) # Example
                else:
                    # If already timezone-aware, convert to UTC
                    df['datetime'] = df['datetime'].dt.tz_convert(utc)

                # Optional: Drop the original timestamp column if desired
                # df.drop(columns=[time_column], inplace=True, errors='ignore')

            except Exception as e:
                print(f"Error standardizing timestamp column '{time_column}': {e}")
                # Create a NaT (Not a Time) datetime column if conversion fails
                df['datetime'] = pd.NaT
        elif 'datetime' not in df.columns: # If no time column specified or found
             print(f"Warning: No valid timestamp column found or specified. 'datetime' column will be missing.")
             # Optionally create a NaT column anyway if downstream code expects it
             # df['datetime'] = pd.NaT


    def find_time_column(self, columns: List[str]) -> Optional[str]:
        """ Tries to guess the timestamp column name. """
        common_names = ['timestamp', 'datetime', 'date', 'time', '@timestamp', 'eventtime']
        for name in common_names:
            if name in columns:
                return name
        # Maybe check columns ending with _time or _date?
        for col in columns:
            if col.lower().endswith(('_time', '_date', 'time', 'date')):
                return col
        return None

    def parse_log_file(self, file_path: str) -> pd.DataFrame:
        """ Parses a single CSV log file. """
        if not os.path.exists(file_path):
            print(f"Error: File not found: {file_path}")
            # Return empty DataFrame with error column for consistency?
            return pd.DataFrame({"error": [f"File not found: {file_path}"]})

        try:
            # Read the CSV file
            # Adjust parameters like delimiter (sep), encoding if needed
            df = pd.read_csv(file_path, low_memory=False, encoding='utf-8', on_bad_lines='warn')

            if df.empty:
                 print(f"Warning: Parsed empty DataFrame from {file_path}")
                 return df # Return empty df

            # Convert column names to lowercase for consistency
            df.columns = df.columns.str.lower().str.strip()

            # Try to find and standardize the timestamp column
            time_col = self.find_time_column(df.columns)
            self._standardize_timestamps(df, time_col)

            # Add the original filename as a column for tracking
            df['source_file'] = os.path.basename(file_path)

            print(f"✅ Successfully parsed CSV: {file_path} ({len(df)} rows)")
            # print(f"🔍 Columns found: {list(df.columns)}") # Debug print

            return df

        except pd.errors.EmptyDataError:
            print(f"Warning: File is empty: {file_path}")
            return pd.DataFrame() # Return empty DataFrame
        except Exception as e:
            print(f"❌ Error parsing CSV file {file_path}: {e}")
            # Return empty DataFrame with error column
            return pd.DataFrame({"error": [f"Parsing failed: {str(e)}"], "source_file": [os.path.basename(file_path)]})


    def parse_log_directory(self, directory_path: str) -> dict:
        """ Parses all CSV files in a directory. """
        if not os.path.exists(directory_path) or not os.path.isdir(directory_path):
            print(f"Error: Directory not found: {directory_path}")
            return {"error": f"Directory not found: {directory_path}"}

        log_data = {}
        for filename in os.listdir(directory_path):
            file_path = os.path.join(directory_path, filename)
            # Check if it's a file and ends with .csv (case-insensitive)
            if os.path.isfile(file_path) and filename.lower().endswith('.csv'):
                df = self.parse_log_file(file_path)
                # Only add if parsing didn't result in an error DataFrame or completely empty
                if not df.empty and "error" not in df.columns:
                    log_data[filename] = df
                elif "error" in df.columns:
                     print(f"Skipping file due to parsing error: {filename}")

        if not log_data:
            print(f"Warning: No valid CSV files found or parsed in directory: {directory_path}")

        return log_data