import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px

class LogVisualizer:
    def __init__(self):
        sns.set(style="whitegrid")
        plt.rcParams['figure.figsize'] = (12, 8)
        plt.rcParams['font.size'] = 12
    
    def optimize_dataframe_for_visualization(self, df, max_points=5000):
        """Reduces dataframe size for faster visualization rendering"""
        # If dataframe is too large, downsample it
        if len(df) > max_points:
            # Different sampling strategies based on whether time data is present
            if 'datetime' in df.columns and not df['datetime'].isna().all():
                # Time-based sampling that preserves trends
                # Group by time intervals and aggregate
                if len(df) > 50000:
                    # Very large datasets: sample more aggressively
                    return df.sample(max_points, random_state=42)
                else:
                    # For medium datasets, use a more intelligent approach
                    # This preserves time series patterns while reducing points
                    return df.sort_values('datetime').iloc[::len(df)//max_points]
            else:
                # Random sampling for non-time data
                return df.sample(max_points, random_state=42)
        return df

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
    
    def optimize_plotly_figure(self, fig):
        """Optimizes a Plotly figure for better performance"""
        # Reduce trace data for large datasets
        for trace in fig.data:
            # Check if trace has a large number of points
            if hasattr(trace, 'x') and len(trace.x) > 1000:
                # Simplify trace data by removing every other point
                # This maintains the visual pattern while reducing memory usage
                if hasattr(trace, 'mode') and 'lines' in trace.mode:
                    # For line charts, sample points but preserve start and end
                    x = trace.x
                    y = trace.y
                    # Keep approximately 1000 points with even spacing
                    if len(x) > 1000:
                        n = len(x)
                        step = max(1, n // 1000)
                        indices = list(range(0, n, step))
                        # Always keep the last point
                        if (n-1) not in indices:
                            indices.append(n-1)
                        trace.x = [x[i] for i in indices]
                        trace.y = [y[i] for i in indices]
        
        # Optimize layout
        if hasattr(fig, 'layout'):
            # Use static plots (not responsive) for better export performance
            fig.layout.update(autosize=False, width=900, height=600)
            # Use lighter weight fonts
            fig.layout.update(font=dict(family="Arial, sans-serif", size=10))
        
        return fig

    def _create_trend_visualization(self, results: pd.DataFrame, summary: dict):
        """Modified method with better performance for trend visualization"""
        if "datetime" not in results.columns or results["datetime"].isna().all():
            return {"type": "table", "data": results.head(1000).to_dict('records')}
        
        # Sample data if too large (more than 10000 rows)
        if len(results) > 10000:
            # Sort by datetime to maintain the time pattern
            results = results.sort_values("datetime")
            # Take a sample that preserves the time distribution
            sample_size = 10000
            step = len(results) // sample_size
            results = results.iloc[::step]
        
        time_span_hours = summary.get("time_span_hours", 24)
        
        # Choose appropriate time grouping
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
        
        # Create and optimize the figure
        fig = px.line(counts, x="datetime", y="count", 
                    title=f"Log Frequency by {time_unit.capitalize()}",
                    labels={"count": "Number of Logs", "datetime": "Time"})
        
        fig.update_layout(
            xaxis_title=f"Time ({time_unit})", 
            yaxis_title="Number of Log Entries", 
            template="plotly_white", 
            height=500
        )
        
        # Apply optimization
        fig = self.optimize_plotly_figure(fig)
        
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