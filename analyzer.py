import pandas as pd
from typing import Dict, Any

class LogAnalyzer:
    def __init__(self):
        self.current_data = None
    
    def analyze(self, log_data: pd.DataFrame, search_params: Dict[str, Any]) -> pd.DataFrame:
        self.current_data = log_data.copy()
        
        if "datetime" in self.current_data.columns and search_params["time_range"]:
            if search_params["time_range"]["start"]:
                self.current_data = self.current_data[
                    self.current_data["datetime"] >= search_params["time_range"]["start"]
                ]
            if search_params["time_range"]["end"]:
                self.current_data = self.current_data[
                    self.current_data["datetime"] <= search_params["time_range"]["end"]
                ]
        
        if search_params["log_type"] and "log_type" in self.current_data.columns:
            self.current_data = self.current_data[
                self.current_data["log_type"].str.contains(search_params["log_type"], case=False, na=False)
            ]
        
        if search_params["action"] and "action" in self.current_data.columns:
            self.current_data = self.current_data[
                self.current_data["action"].str.contains(search_params["action"], case=False, na=False)
            ]
        
        if search_params["ip_address"]:
            ip_filter = search_params["ip_address"]
            ip_columns = [col for col in self.current_data.columns if 'ip' in col.lower()]
            if ip_columns:
                ip_mask = self.current_data[ip_columns[0]].str.contains(ip_filter, case=False, na=False)
                for col in ip_columns[1:]:
                    ip_mask |= self.current_data[col].str.contains(ip_filter, case=False, na=False)
                self.current_data = self.current_data[ip_mask]
            else:
                self.current_data = self.current_data[
                    self.current_data["raw_log"].str.contains(ip_filter, case=False, na=False)
                ]
        
        if search_params["user"]:
            user_columns = [col for col in self.current_data.columns if 'user' in col.lower()]
            if user_columns:
                user_mask = self.current_data[user_columns[0]].str.contains(search_params["user"], case=False, na=False)
                for col in user_columns[1:]:
                    user_mask |= self.current_data[col].str.contains(search_params["user"], case=False, na=False)
                self.current_data = self.current_data[user_mask]
            else:
                self.current_data = self.current_data[
                    self.current_data["raw_log"].str.contains(search_params["user"], case=False, na=False)
                ]
        
        if search_params["keywords"]:
            keyword_pattern = '|'.join(search_params["keywords"])
            self.current_data = self.current_data[
                self.current_data["raw_log"].str.contains(keyword_pattern, case=False, na=False)
            ]
            # Add exact match filtering for firewall logs
        if search_params["log_type"] and "log_type" in self.current_data.columns:
            self.current_data = self.current_data[
            self.current_data["log_type"].str.lower() == search_params["log_type"].lower()
            ]
    
    # Add action filtering for firewall logs
        if search_params["action"] and "action" in self.current_data.columns:
            self.current_data = self.current_data[
            self.current_data["action"].str.lower() == search_params["action"].lower()
            ]
        return self.current_data
    
    def generate_summary(self, results: pd.DataFrame, search_params: Dict[str, Any]) -> Dict[str, Any]:
        summary = {
            "total_logs": len(results),
            "time_range": search_params["time_range"],
            "query": search_params["original_query"],
            "keywords": search_params["keywords"],
            "viz_type": search_params["viz_type"]
        }
        
        if "datetime" in results.columns and not results["datetime"].isna().all():
            summary["earliest_log"] = results["datetime"].min()
            summary["latest_log"] = results["datetime"].max()
            summary["time_span"] = (summary["latest_log"] - summary["earliest_log"]).total_seconds() / 3600
        
        if "log_type" in results.columns:
            summary["log_type_distribution"] = results["log_type"].value_counts().to_dict()
        
        if "action" in results.columns:
            summary["action_distribution"] = results["action"].value_counts().to_dict()
        
        ip_columns = [col for col in results.columns if 'ip' in col.lower()]
        if ip_columns:
            ip_data = {col: results[col].value_counts().head(10).to_dict() for col in ip_columns if not results[col].isna().all()}
            if ip_data:
                summary["ip_addresses"] = ip_data
                
        if search_params["count_request"]:
            summary["count_metrics"] = True
            if "datetime" in results.columns and not results["datetime"].isna().all():
                summary["daily_counts"] = results.groupby(results["datetime"].dt.date).size().to_dict()
                if len(results) > 0:
                    summary["hourly_pattern"] = results.groupby(results["datetime"].dt.hour).size().to_dict()
        
        return summary