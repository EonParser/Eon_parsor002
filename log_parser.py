import re
import pandas as pd
import os
from pytz import utc  # Added import


class LogParser:
    def __init__(self):
        self.log_formats = {
            "common_log": r'...',
            "syslog": r'...',
            "firewall": r'(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+FIREWALL\s+ACTION=(?P<action>\w+)\s+SRC=(?P<src_ip>[\d\.]+)\s+DST=(?P<dst_ip>[\d\.]+)\s+PROTO=(?P<protocol>\w+)\s+SPT=(?P<src_port>\d+)\s+DPT=(?P<dst_port>\d+)\s*(?P<message>.*)',
            "endpoint": r'...'
        }

        self.patterns = {name: re.compile(pattern) for name, pattern in self.log_formats.items()}
    
    def detect_log_type(self, log_line: str) -> str:
        for log_type, pattern in self.patterns.items():
            if pattern.match(log_line):
                return log_type
        return "unknown"
    
    def parse_log_file(self, file_path: str) -> pd.DataFrame:
        if not os.path.exists(file_path):
            return pd.DataFrame({"error": [f"File not found: {file_path}"]})
        
        log_type = "unknown"
        try:
            with open(file_path, 'r', errors='ignore') as f:
                for line in f:
                    if line.strip():
                        log_type = self.detect_log_type(line.strip())
                        break
        except Exception as e:
            return pd.DataFrame({"error": [f"Error reading file: {str(e)}"]})
    
        data = []
        with open(file_path, 'r', errors='ignore') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                if log_type == "unknown":
                    data.append({"line_number": line_num, "timestamp": None, "raw_log": line})
                else:
                    match = self.patterns[log_type].match(line)
                    if match:
                        log_entry = match.groupdict()
                        log_entry["line_number"] = line_num
                        log_entry["log_type"] = log_type
                        log_entry["raw_log"] = line
                        data.append(log_entry)
                    else:
                        data.append({"line_number": line_num, "log_type": log_type, "raw_log": line})
    
        df = pd.DataFrame(data)
    
        if "timestamp" in df.columns:
            self._standardize_timestamps(df, log_type)
    
    # 🔥 ADD THIS DEBUG PRINT:
        print(f"\n✅ Parsed file: {file_path}")
        print(f"📄 Log Type Detected: {log_type}")
        print(f"🧹 Total Parsed Logs: {len(df)}")
        if not df.empty:
            print("🔍 Example parsed entries:")
            print(df.head(3))  # Show first 3 rows
        else:
            print("⚠️ No logs parsed! Check if file format matches expected patterns.")

        return df

    
    def _standardize_timestamps(self, df: pd.DataFrame, log_type: str) -> None:
        if "timestamp" not in df.columns:
            return
            
        formats = {
            "common_log": "%d/%b/%Y:%H:%M:%S %z",
            "syslog": "%b %d %H:%M:%S",
            "firewall": "%Y-%m-%d %H:%M:%S",
            "endpoint": "%Y-%m-%d %H:%M:%S"
        }
        
        try:
            if log_type in formats:
                df["datetime"] = pd.to_datetime(
                    df["timestamp"], 
                    format=formats[log_type], 
                    errors="coerce"
                ).dt.tz_localize(utc)
            else:
                df["datetime"] = pd.to_datetime(
                    df["timestamp"], 
                    errors="coerce"
                ).dt.tz_localize(utc)
        except:
            df["datetime"] = pd.to_datetime(
                df["timestamp"], 
                errors="coerce"
            ).dt.tz_localize(utc)
            
    def parse_log_directory(self, directory_path: str) -> dict:
        if not os.path.exists(directory_path) or not os.path.isdir(directory_path):
            return {"error": f"Directory not found: {directory_path}"}
        log_data = {}
        for filename in os.listdir(directory_path):
            file_path = os.path.join(directory_path, filename)
            if os.path.isfile(file_path) and (filename.endswith(('.log', '.txt')) or 'log' in filename.lower()):
                log_data[filename] = self.parse_log_file(file_path)
        return log_data
    


    