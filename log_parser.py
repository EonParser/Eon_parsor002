import pandas as pd
import os
from pytz import utc
from typing import List, Optional, Dict, Any
import datetime
import re

class LogParser:
    def __init__(self):
        # Common CSV log file headers
        self.common_headers = {
            'timestamp': ['timestamp', 'date', 'time', 'datetime', '@timestamp', 'eventtime'],
            'hostname': ['hostname', 'host', 'device', 'node', 'system'],
            'severity': ['severity', 'level', 'priority', 'sev'],
            'ip': ['ip', 'source_ip', 'dest_ip', 'src_ip', 'dst_ip', 'source', 'destination'],
            'port': ['port', 'source_port', 'dest_port', 'src_port', 'dst_port'],
            'protocol': ['protocol', 'proto'],
            'action': ['action', 'disposition', 'status'],
            'message': ['message', 'msg', 'log', 'description', 'data', 'event']
        }
        
        # Standard firewall log formats
        self.known_log_formats = {
            'cisco_asa': {
                'pattern': r'ASA-\d+-\d+',  # Pattern in the log that identifies Cisco ASA
                'columns': [
                    'timestamp', 'hostname', 'severity', 'message_id', 
                    'message', 'src_ip', 'src_port', 'dst_ip', 'dst_port', 'protocol', 'action'
                ]
            },
            'palo_alto': {
                'pattern': r'PANOS',  # Pattern for Palo Alto logs
                'columns': [
                    'timestamp', 'hostname', 'severity', 'action', 
                    'src_ip', 'dst_ip', 'src_port', 'dst_port', 'protocol', 'message'
                ]
            }
        }

    def _standardize_timestamps(self, df: pd.DataFrame, time_column: str) -> None:
        """
        Converts timestamp column to timezone-aware UTC datetime objects.
        """
        if time_column and time_column in df.columns:
            try:
                # Handle different timestamp formats
                df['timestamp'] = pd.to_datetime(df[time_column], errors='coerce', utc=True)
                
                # Check if timezone aware, if not, assume UTC
                if df['timestamp'].dt.tz is None:
                    df['timestamp'] = df['timestamp'].dt.tz_localize(utc)
                    
            except Exception as e:
                print(f"Error standardizing timestamp column '{time_column}': {e}")
                df['timestamp'] = pd.NaT
                
            # If timestamp column was renamed, we can drop the original if specified
            if time_column != 'timestamp':
                # Keep the original column for reference, don't drop it
                pass
        else:
            # Create a timestamp column with current time as fallback
            print(f"Warning: No valid timestamp column found. Creating default timestamp column.")
            df['timestamp'] = pd.Timestamp.now(tz=utc)

    def find_time_column(self, columns: List[str]) -> Optional[str]:
        """Tries to identify the timestamp column name."""
        for name in self.common_headers['timestamp']:
            if name in columns:
                return name
                
        # Look for columns with time-related names
        for col in columns:
            if any(keyword in col.lower() for keyword in ['time', 'date']):
                return col
                
        return None

    def detect_log_format(self, df: pd.DataFrame) -> str:
        """
        Detect the log format based on contents and return the format name.
        """
        # Check for known log formats
        for format_name, format_info in self.known_log_formats.items():
            pattern = format_info['pattern']
            
            # Choose a column to check based on which ones exist
            check_columns = ['message', 'hostname', 'message_id']
            for col in check_columns:
                if col in df.columns and not df[col].isna().all():
                    # Check if any row matches the pattern
                    if df[col].astype(str).str.contains(pattern, regex=True).any():
                        return format_name
        
        # If no known format is detected, try to infer format from column names
        return self._infer_format_from_columns(df.columns)
    
    def _infer_format_from_columns(self, columns: List[str]) -> str:
        """Infer log format from column names."""
        # Count matches for each known format
        format_scores = {}
        
        for format_name, format_info in self.known_log_formats.items():
            expected_columns = format_info['columns']
            # Count how many expected columns are present
            matches = sum(1 for col in expected_columns if col in columns)
            format_scores[format_name] = matches / len(expected_columns)
        
        # Choose format with highest score if above threshold
        best_format = max(format_scores.items(), key=lambda x: x[1])
        if best_format[1] >= 0.6:  # At least 60% column match
            return best_format[0]
            
        return 'generic'  # Default to generic format
    
    def parse_log_file(self, file_path: str) -> pd.DataFrame:
        """Parses a single CSV log file with improved format detection."""
        if not os.path.exists(file_path):
            print(f"Error: File not found: {file_path}")
            return pd.DataFrame({"error": [f"File not found: {file_path}"]})

        try:
            # First attempt to detect delimiter and header in the file
            sample_lines = []
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                for _ in range(10):  # Read first 10 lines
                    line = f.readline()
                    if line:
                        sample_lines.append(line)
            
            # Count occurrences of potential delimiters
            delimiters = [',', '\t', '|', ';', ' ']
            delimiter_counts = {}
            
            for delimiter in delimiters:
                # Calculate average number of splits
                count = sum(line.count(delimiter) for line in sample_lines) / len(sample_lines) if sample_lines else 0
                delimiter_counts[delimiter] = count
            
            # Choose delimiter with highest average count
            best_delimiter = max(delimiter_counts.items(), key=lambda x: x[1])[0]
            
            # Now try to determine if header exists
            has_header = True
            if len(sample_lines) >= 2:
                first_line_parts = sample_lines[0].split(best_delimiter)
                second_line_parts = sample_lines[1].split(best_delimiter)
                
                # If first line has different data types than second line, it's likely a header
                first_types = [self._infer_data_type(part) for part in first_line_parts]
                second_types = [self._infer_data_type(part) for part in second_line_parts]
                
                if len(first_types) == len(second_types):
                    if all(a == b for a, b in zip(first_types, second_types)):
                        # If all types match, check if first line could be a header
                        has_header = any(not self._is_likely_data(part) for part in first_line_parts)
            
            # Read the CSV file with detected parameters
            df = pd.read_csv(
                file_path, 
                delimiter=best_delimiter, 
                header=0 if has_header else None,
                encoding='utf-8', 
                on_bad_lines='warn',
                low_memory=False
            )
            
            # If no header was detected, set column names based on data types
            if not has_header:
                new_columns = []
                for i, dtype in enumerate(df.dtypes):
                    if pd.api.types.is_numeric_dtype(dtype):
                        new_columns.append(f"numeric_{i}")
                    elif pd.api.types.is_datetime64_any_dtype(dtype):
                        new_columns.append(f"datetime_{i}")
                    else:
                        new_columns.append(f"text_{i}")
                df.columns = new_columns
            
            # Convert column names to lowercase for consistency
            df.columns = df.columns.str.lower().str.strip()
            
            # Try to detect log format
            log_format = self.detect_log_format(df)
            print(f"Detected log format: {log_format}")
            
            # Standardize column names based on format
            self._standardize_column_names(df, log_format)
            
            # Try to find and standardize the timestamp column
            time_col = self.find_time_column(df.columns)
            self._standardize_timestamps(df, time_col)
            
            # Convert severity to numeric if possible
            if 'severity' in df.columns:
                try:
                    df['severity'] = pd.to_numeric(df['severity'], errors='coerce')
                except:
                    pass
            
            # Add the original filename as a column for tracking
            df['source_file'] = os.path.basename(file_path)
            
            print(f"✅ Successfully parsed CSV: {file_path} ({len(df)} rows)")
            print(f"🔍 Columns found: {list(df.columns)}")
            
            return df
            
        except pd.errors.EmptyDataError:
            print(f"Warning: File is empty: {file_path}")
            return pd.DataFrame()
        except Exception as e:
            print(f"❌ Error parsing CSV file {file_path}: {e}")
            return pd.DataFrame({"error": [f"Parsing failed: {str(e)}"], "source_file": [os.path.basename(file_path)]})
    
    def _infer_data_type(self, value: str) -> str:
        """Infer the data type of a string value."""
        value = value.strip()
        
        # Check if empty
        if not value:
            return 'empty'
            
        # Check if numeric
        try:
            float(value)
            return 'numeric'
        except ValueError:
            pass
            
        # Check if date/time
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
            r'\d{2}-\d{2}-\d{4}',  # DD-MM-YYYY
        ]
        
        for pattern in date_patterns:
            if re.match(pattern, value):
                return 'datetime'
        
        # Check if time
        time_patterns = [
            r'\d{2}:\d{2}:\d{2}',  # HH:MM:SS
            r'\d{2}:\d{2}',        # HH:MM
        ]
        
        for pattern in time_patterns:
            if re.match(pattern, value):
                return 'time'
        
        # Check if IP address
        ip_pattern = r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}'
        if re.match(ip_pattern, value):
            return 'ip'
            
        # Default to text
        return 'text'
    
    def _is_likely_data(self, value: str) -> bool:
        """Check if a string is likely to be data rather than a header."""
        value = value.strip().lower()
        
        # Headers typically don't have numbers or special characters
        if re.search(r'\d', value):
            return True
            
        # Headers are usually shorter than data
        if len(value) > 30:
            return True
            
        # Headers don't usually contain these characters
        if any(char in value for char in ['/', '\\', '@', ':', ';', '=', '<', '>']):
            return True
            
        return False
    
    def _standardize_column_names(self, df: pd.DataFrame, log_format: str) -> None:
        """Standardize column names based on detected format and content."""
        # If we detected a known format, rename columns accordingly
        if log_format in self.known_log_formats:
            expected_columns = self.known_log_formats[log_format]['columns']
            
            # If we have the expected number of columns and no names, rename them
            if len(df.columns) == len(expected_columns) and all(col.startswith(('text_', 'numeric_', 'datetime_')) for col in df.columns):
                df.columns = expected_columns
                return
        
        # Otherwise, try to infer column meanings from names and content
        rename_map = {}
        
        for semantic_type, possible_names in self.common_headers.items():
            # Find columns that match the possible names
            matches = [col for col in df.columns if any(name in col.lower() for name in possible_names)]
            
            if matches:
                # Handle special cases for IP and port, which have source/destination
                if semantic_type == 'ip':
                    src_ip_cols = [col for col in matches if any(s in col.lower() for s in ['source', 'src'])]
                    dst_ip_cols = [col for col in matches if any(s in col.lower() for s in ['dest', 'dst', 'destination'])]
                    
                    if src_ip_cols:
                        rename_map[src_ip_cols[0]] = 'src_ip'
                    if dst_ip_cols:
                        rename_map[dst_ip_cols[0]] = 'dst_ip'
                        
                elif semantic_type == 'port':
                    src_port_cols = [col for col in matches if any(s in col.lower() for s in ['source', 'src'])]
                    dst_port_cols = [col for col in matches if any(s in col.lower() for s in ['dest', 'dst', 'destination'])]
                    
                    if src_port_cols:
                        rename_map[src_port_cols[0]] = 'src_port'
                    if dst_port_cols:
                        rename_map[dst_port_cols[0]] = 'dst_port'
                        
                # For other types, just use the first match
                elif matches and semantic_type not in ['ip', 'port']:
                    rename_map[matches[0]] = semantic_type
        
        # Apply the rename
        df.rename(columns=rename_map, inplace=True)
        
        # If we couldn't find a timestamp column, look for columns that might contain timestamps
        if 'timestamp' not in df.columns:
            for col in df.columns:
                # Sample the column to see if it contains date-like strings
                sample = df[col].dropna().head(10).astype(str)
                date_patterns = [
                    r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
                    r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
                    r'\d{1,2}-\w{3}-\d{4}', # DD-MMM-YYYY
                    r'\d{2}:\d{2}:\d{2}',  # HH:MM:SS
                ]
                
                if any(sample.str.contains(pattern, regex=True).any() for pattern in date_patterns):
                    df.rename(columns={col: 'timestamp'}, inplace=True)
                    break

    def parse_log_directory(self, directory_path: str) -> dict:
        """Parses all CSV files in a directory."""
        if not os.path.exists(directory_path) or not os.path.isdir(directory_path):
            print(f"Error: Directory not found: {directory_path}")
            return {"error": f"Directory not found: {directory_path}"}

        log_data = {}
        for filename in os.listdir(directory_path):
            file_path = os.path.join(directory_path, filename)
            # Check if it's a file and ends with .csv or .log (case-insensitive)
            if os.path.isfile(file_path) and (filename.lower().endswith('.csv') or filename.lower().endswith('.log')):
                df = self.parse_log_file(file_path)
                # Only add if parsing didn't result in an error DataFrame or completely empty
                if not df.empty and "error" not in df.columns:
                    log_data[filename] = df
                elif "error" in df.columns:
                     print(f"Skipping file due to parsing error: {filename}")

        if not log_data:
            print(f"Warning: No valid CSV files found or parsed in directory: {directory_path}")

        return log_data