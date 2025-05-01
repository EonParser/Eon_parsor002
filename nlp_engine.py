import spacy
import re
from typing import Dict, Any
from datetime import datetime, timedelta
from pytz import utc

class NLPQueryProcessor:
    def __init__(self):
        try:
            self.nlp = spacy.load("en_core_web_md")
        except:
            import subprocess
            subprocess.run(["python", "-m", "spacy", "download", "en_core_web_md"], check=True)
            self.nlp = spacy.load("en_core_web_md")
        
        self.query_patterns = {
            "time_pattern": [
                "last [0-9]+ (minutes|hours|days|weeks|months)",
                "between .+ and .+",
                "since .+",
                "from .+ to .+",
                "today",
                "yesterday",
                "this (week|month)"
            ],
            "error_pattern": ["error", "failure", "failed", "critical", "warning", "exception", "timeout"],
            "ip_pattern": [
                "ip [0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+",
                "from ip [0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+",
                "traffic (from|to) [0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+"
            ],
            "count_pattern": ["count", "how many", "frequency", "occurrences"],
            "user_pattern": ["user [a-zA-Z0-9_]+", "username [a-zA-Z0-9_]+"]
        }
        
        self.custom_entities = {
            "LOG_TYPE": ["firewall", "endpoint", "system", "application", "access", "error", "security", "network", "audit"],
            "ACTION": ["block", "allow", "deny", "accept", "reject", "drop", "connect", "disconnect", "login", "logout"]
        }
    
    def process_query(self, query: str) -> Dict[str, Any]:
        query = query.lower().strip()
        doc = self.nlp(query)
        
        params = {
            "time_range": None,
            "keywords": [],
            "log_type": None,
            "action": None,
            "ip_address": None,
            "user": None,
            "count_request": False,
            "viz_type": self._determine_visualization_type(query, doc),
            "original_query": query
        }
        
        params["time_range"] = self._extract_time_range(query, doc)
        
        for entity in self.custom_entities["LOG_TYPE"]:
            if entity in query:
                params["log_type"] = entity
                break
        
        for entity in self.custom_entities["ACTION"]:
            if entity in query:
                params["action"] = entity
                break
        
        ip_match = re.search(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})', query)
        if ip_match:
            params["ip_address"] = ip_match.group(0)
        
        user_match = re.search(r'user[name]* (\w+)', query)
        if user_match:
            params["user"] = user_match.group(1)
        
        for pattern in self.query_patterns["count_pattern"]:
            if pattern in query:
                params["count_request"] = True
                break
        
        for token in doc:
            if token.pos_ in ["NOUN", "VERB", "ADJ"] and token.text not in params["keywords"]:
                skip = any(token.text in entity_list for entity_list in self.custom_entities.values())
                if not skip and len(token.text) > 2:
                    params["keywords"].append(token.text)
        
        return params
 

    def _extract_time_range(self, query: str, doc) -> Dict[str, datetime]:
        time_info = {"start": None, "end": None}
        now = datetime.now(utc)  # Make initial datetime timezone-aware
        
        last_pattern = re.search(r'last (\d+) (minute|hour|day|week|month)s?', query)
        if last_pattern:
            amount = int(last_pattern.group(1))
            unit = last_pattern.group(2)
            end_time = now
            if unit == 'minute':
                start_time = end_time - timedelta(minutes=amount)
            elif unit == 'hour':
                start_time = end_time - timedelta(hours=amount)
            elif unit == 'day':
                start_time = end_time - timedelta(days=amount)
            elif unit == 'week':
                start_time = end_time - timedelta(weeks=amount)
            elif unit == 'month':
                start_time = end_time - timedelta(days=amount*30)
            time_info["start"] = start_time.astimezone(utc)
            time_info["end"] = end_time.astimezone(utc)
            return time_info
        
        if "today" in query:
            today = now.replace(hour=0, minute=0, second=0, microsecond=0)
            time_info["start"] = today.astimezone(utc)
            time_info["end"] = now.astimezone(utc)
            return time_info
        
        if "yesterday" in query:
            today = now.replace(hour=0, minute=0, second=0, microsecond=0)
            time_info["start"] = (today - timedelta(days=1)).astimezone(utc)
            time_info["end"] = today.astimezone(utc)
            return time_info
    
    # Default to LAST 24 hours (not future)
        time_info["start"] = (now - timedelta(hours=24)).astimezone(utc)
        time_info["end"] = now.astimezone(utc)
        return time_info
    
    def _determine_visualization_type(self, query: str, doc) -> str:
        viz_type = "table"
        viz_keywords = {
            "trend": ["trend", "timeline", "over time", "time series"],
            "pie": ["pie", "percentage", "distribution", "breakdown"],
            "bar": ["bar", "histogram", "count by", "frequency"],
            "heatmap": ["heat", "heatmap", "density"]
        }
        
        for viz, keywords in viz_keywords.items():
            if any(keyword in query for keyword in keywords):
                return viz
        
        if any(word in query for word in ["visualize", "visualization", "chart", "graph"]):
            if any(word in query for word in ["time", "trend", "timeline"]):
                return "trend"
            elif any(word in query for word in ["compare", "comparison", "versus"]):
                return "bar"
            elif any(word in query for word in ["distribution", "percentage"]):
                return "pie"
        
        return viz_type
