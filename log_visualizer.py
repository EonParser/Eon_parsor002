import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px

class LogVisualizer:
    def __init__(self):
        sns.set(style="whitegrid")
        plt.rcParams['figure.figsize'] = (12, 8)
        plt.rcParams['font.size'] = 12
    
    def generate_visualization(self, results: pd.DataFrame, summary: dict, viz_type: str = "table"):
        if len(results) == 0:
            return {"type": "table", "data": {"message": "No results to visualize"}}
        
        if viz_type == "trend" and "datetime" in results.columns:
            return self._create_trend_visualization(results, summary)
        elif viz_type == "pie":
            return self._create_pie_visualization(results, summary)
        elif viz_type == "bar":
            return self._create_bar_visualization(results, summary)
        elif viz_type == "heatmap" and "datetime" in results.columns:
            return self._create_heatmap_visualization(results, summary)
        else:
            return {"type": "table", "data": results.head(1000).to_dict('records')}
    
    def _create_trend_visualization(self, results: pd.DataFrame, summary: dict):
        if "datetime" not in results.columns or results["datetime"].isna().all():
            return {"type": "table", "data": results.head(1000).to_dict('records')}
        
        time_span_hours = summary.get("time_span", 24)
        if time_span_hours <= 24:
            time_groups = results.groupby(pd.Grouper(key="datetime", freq="1H"))
            time_unit = "hour"
        elif time_span_hours <= 24*7:
            time_groups = results.groupby(pd.Grouper(key="datetime", freq="4H"))
            time_unit = "4-hour period"
        elif time_span_hours <= 24*30:
            time_groups = results.groupby(pd.Grouper(key="datetime", freq="1D"))
            time_unit = "day"
        else:
            time_groups = results.groupby(pd.Grouper(key="datetime", freq="1W"))
            time_unit = "week"
        
        counts = time_groups.size().reset_index(name='count')
        fig = px.line(counts, x="datetime", y="count", 
                      title=f"Log Frequency by {time_unit.capitalize()}",
                      labels={"count": "Number of Logs", "datetime": "Time"})
        fig.update_layout(xaxis_title=f"Time ({time_unit})", yaxis_title="Number of Log Entries", template="plotly_white", height=500)
        return {"type": "plotly", "data": fig}
    
    def _create_pie_visualization(self, results: pd.DataFrame, summary: dict):
        candidate_columns = ["log_type", "action", "status", "hostname", "process"]
        for col in candidate_columns:
            if col in results.columns and not results[col].isna().all():
                value_counts = results[col].value_counts().head(10)
                fig = px.pie(names=value_counts.index, values=value_counts.values,
                             title=f"Distribution by {col.replace('_', ' ').title()}")
                fig.update_layout(height=500, template="plotly_white")
                return {"type": "plotly", "data": fig}
        
        if "keywords" in summary and summary["keywords"]:
            keyword_counts = {kw: results["raw_log"].str.contains(kw, case=False).sum() for kw in summary["keywords"]}
            if keyword_counts:
                fig = px.pie(names=list(keyword_counts.keys()), values=list(keyword_counts.values()),
                             title="Distribution by Keywords")
                fig.update_layout(height=500, template="plotly_white")
                return {"type": "plotly", "data": fig}
        
        top_lines = results["raw_log"].value_counts().head(5)
        categories = [f"Line {i+1}" for i in range(len(top_lines))]
        fig = px.pie(names=categories, values=top_lines.values, title="Most Frequent Log Entries")
        fig.update_layout(height=500, template="plotly_white")
        return {"type": "plotly", "data": fig}
    
    def _create_bar_visualization(self, results: pd.DataFrame, summary: dict):
        candidate_columns = ["log_type", "action", "status", "hostname", "process"]
        for col in candidate_columns:
            if col in results.columns and not results[col].isna().all():
                counts = results[col].value_counts().head(15)
                fig = px.bar(x=counts.index, y=counts.values,
                             title=f"Count by {col.replace('_', ' ').title()}",
                             labels={"x": col.replace('_', ' ').title(), "y": "Count"})
                fig.update_layout(xaxis_title=col.replace('_', ' ').title(), yaxis_title="Number of Log Entries",
                                  template="plotly_white", height=500)
                return {"type": "plotly", "data": fig}
        
        if "datetime" in results.columns and not results["datetime"].isna().all():
            hour_counts = results.groupby(results["datetime"].dt.hour).size().reset_index(name='count')
            hour_counts.columns = ['hour', 'count']
            fig = px.bar(hour_counts, x='hour', y='count', title="Log Frequency by Hour of Day",
                         labels={"hour": "Hour (0-23)", "count": "Count"})
            fig.update_layout(xaxis=dict(tickmode='linear', dtick=1), template="plotly_white", height=500)
            return {"type": "plotly", "data": fig}
        
        top_lines = results["raw_log"].value_counts().head(10)
        truncated_lines = [line[:50] + "..." if len(line) > 50 else line for line in top_lines.index]
        fig = px.bar(x=truncated_lines, y=top_lines.values, title="Top Log Entries",
                     labels={"x": "Log Entry", "y": "Count"})
        fig.update_layout(template="plotly_white", height=500)
        return {"type": "plotly", "data": fig}
    
    def _create_heatmap_visualization(self, results: pd.DataFrame, summary: dict):
        if "datetime" not in results.columns or results["datetime"].isna().all():
            return {"type": "table", "data": results.head(1000).to_dict('records')}
        
        results['hour'] = results['datetime'].dt.hour
        results['day'] = results['datetime'].dt.day_name()
        heatmap_data = results.pivot_table(index='day', columns='hour', aggfunc='size', fill_value=0)
        fig = px.imshow(heatmap_data, labels=dict(x="Hour", y="Day", color="Count"),
                        title="Log Activity Heatmap", height=500)
        fig.update_layout(template="plotly_white")
        return {"type": "plotly", "data": fig}