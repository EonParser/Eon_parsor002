import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from typing import Dict, Any, List, Union

class LogVisualizer:
    def __init__(self):
        # Set color scheme for consistent visualizations
        self.color_scheme = px.colors.qualitative.Safe
        self.time_column = 'timestamp'  # Default time column name
    
    def optimize_dataframe_for_visualization(self, df: pd.DataFrame, max_points: int = 5000) -> pd.DataFrame:
        """Reduces dataframe size for faster visualization rendering"""
        # If dataframe is small enough, return it as is
        if len(df) <= max_points:
            return df
            
        # If timestamp column exists, sample preserving time patterns
        if self.time_column in df.columns and not df[self.time_column].isna().all():
            # Convert to datetime if not already
            if not pd.api.types.is_datetime64_any_dtype(df[self.time_column]):
                df[self.time_column] = pd.to_datetime(df[self.time_column], errors='coerce')
                
            # Sort by time and take evenly spaced samples
            return df.sort_values(self.time_column).iloc[::len(df)//max_points]
        
        # Otherwise, use random sampling
        return df.sample(max_points, random_state=42)

    def generate_visualization(self, results: pd.DataFrame, summary: Dict[str, Any], viz_type: str = "auto") -> Dict[str, Any]:
        """Generate visualization based on the specified type and data"""
        # Return early if results empty
        if len(results) == 0:
            return {"type": "table", "data": {"message": "No results to visualize"}}
        
        # Ensure time column exists if needed for time-based visualizations
        timestamp_exists = self.time_column in results.columns and not results[self.time_column].isna().all()
        
        # Convert timestamp to datetime if it's not already
        if timestamp_exists and not pd.api.types.is_datetime64_any_dtype(results[self.time_column]):
            results[self.time_column] = pd.to_datetime(results[self.time_column], errors='coerce')
        
        # For auto type, determine the best visualization
        if viz_type == "auto":
            viz_type = self._determine_best_visualization(results, summary)
            print(f"Auto-selected visualization type: {viz_type}")
        
        # Handle different visualization types
        if viz_type == "trend" and timestamp_exists:
            return self._create_trend_visualization(results, summary)
        elif viz_type == "pie":
            return self._create_pie_visualization(results, summary)
        elif viz_type == "bar":
            return self._create_bar_visualization(results, summary)
        elif viz_type == "heatmap" and timestamp_exists:
            return self._create_heatmap_visualization(results, summary)
        elif viz_type == "summary":
            return self._create_summary_dashboard(results, summary)
        else:
            # Default to table if requested type can't be created
            if timestamp_exists and viz_type == "trend":
                # Try timeline as fallback
                return self._create_trend_visualization(results, summary)
            elif viz_type == "heatmap" and not timestamp_exists:
                # Try bar chart as fallback
                return self._create_bar_visualization(results, summary)
            else:
                return {"type": "table", "data": {"message": f"Cannot create '{viz_type}' visualization with the current data"}}
    
    def _determine_best_visualization(self, results: pd.DataFrame, summary: Dict[str, Any]) -> str:
        """Determine the best visualization type based on the data"""
        # Check if time data is available
        has_time = self.time_column in results.columns and not results[self.time_column].isna().all()
        
        # Check if categorical columns are available
        categorical_columns = ["action", "protocol", "severity", "hostname"]
        has_categorical = any(col in results.columns for col in categorical_columns)
        
        # Determine the best visualization
        if has_time and len(results) > 5:
            # For time-based data, prefer trend
            return "trend"
        elif has_categorical and len(results) > 3:
            # For categorical data with enough samples, use bar chart
            return "bar"
        elif len(results) > 50:
            # For large datasets with multiple facets, use summary dashboard
            return "summary"
        else:
            # Default to table for small datasets
            return "table"
    
    def optimize_plotly_figure(self, fig: go.Figure) -> go.Figure:
        """Optimize a Plotly figure for better performance"""
        # Set consistent template and colors
        fig.update_layout(
            template="plotly_white",
            colorway=self.color_scheme,
            margin=dict(l=50, r=50, t=80, b=50),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            height=600
        )
        
        # Improve readability
        fig.update_layout(
            font=dict(family="Arial, sans-serif", size=12),
            title=dict(font=dict(size=16, color="#333")),
            plot_bgcolor="white"
        )
        
        # Optimize for large datasets
        if hasattr(fig, 'data') and len(fig.data) > 0:
            for trace in fig.data:
                # Check if trace has a large number of points
                if hasattr(trace, 'x') and len(trace.x) > 1000:
                    # Simplify trace data for large datasets
                    if hasattr(trace, 'mode') and 'lines' in trace.mode:
                        # For line charts, sample points but preserve pattern
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
        
        return fig

    def _create_trend_visualization(self, results: pd.DataFrame, summary: Dict[str, Any]) -> Dict[str, Any]:
        """Create a time-based trend visualization"""
        # Optimize dataframe for visualization
        df = self.optimize_dataframe_for_visualization(results)
        
        # Ensure timestamp is datetime
        if not pd.api.types.is_datetime64_any_dtype(df[self.time_column]):
            df[self.time_column] = pd.to_datetime(df[self.time_column], errors='coerce')
        
        # Calculate appropriate time grouping based on time span
        time_span_hours = summary.get("time_span_hours", 24)
        
        if time_span_hours <= 1:
            # For spans under an hour, group by minute
            freq = "1min"
            time_unit = "minute"
        elif time_span_hours <= 24:
            # For spans under a day, group by hour
            freq = "1H" 
            time_unit = "hour"
        elif time_span_hours <= 24*7:
            # For spans under a week, group by 4 hours
            freq = "4H"
            time_unit = "4-hour period"
        elif time_span_hours <= 24*30:
            # For spans under a month, group by day
            freq = "1D"
            time_unit = "day"
        else:
            # For longer spans, group by week
            freq = "1W"
            time_unit = "week"
        
        # Group data by time
        time_groups = df.groupby(pd.Grouper(key=self.time_column, freq=freq))
        counts = time_groups.size().reset_index(name='count')
        
        # Check if we can split by a categorical column
        category_cols = ["action", "protocol", "severity", "hostname", "message_id"]
        category_col = None
        
        for col in category_cols:
            if col in df.columns and df[col].nunique() <= 10:
                category_col = col
                break
        
        # Create figure
        if category_col:
            # Create figure with category breakdown
            cat_groups = df.groupby([pd.Grouper(key=self.time_column, freq=freq), category_col]).size().reset_index(name='count')
            fig = px.line(
                cat_groups, 
                x=self.time_column, 
                y="count", 
                color=category_col,
                title=f"Log Frequency by {time_unit.capitalize()} and {category_col.replace('_', ' ').title()}",
                labels={"count": "Number of Logs", self.time_column: "Time"}
            )
        else:
            # Create simple time series
            fig = px.line(
                counts, 
                x=self.time_column, 
                y="count",
                title=f"Log Frequency by {time_unit.capitalize()}",
                labels={"count": "Number of Logs", self.time_column: "Time"}
            )
        
        # Add markers for better readability
        fig.update_traces(mode="lines+markers", marker=dict(size=6))
        
        # Improve axis formatting
        fig.update_xaxes(
            title=f"Time ({time_unit})",
            tickangle=-45 if time_unit in ["day", "week"] else 0
        )
        
        fig.update_yaxes(title="Number of Log Entries")
        
        # Apply optimization
        fig = self.optimize_plotly_figure(fig)
        
        return {"type": "plotly", "data": fig}
    
    def _create_pie_visualization(self, results: pd.DataFrame, summary: Dict[str, Any]) -> Dict[str, Any]:
        """Create a pie chart visualization"""
        # Find the best categorical column for the pie chart
        candidate_columns = ["action", "protocol", "severity", "hostname", "message_id"]
        
        for col in candidate_columns:
            if col in results.columns and not results[col].isna().all():
                # Get value counts and limit to top 10
                value_counts = results[col].value_counts().head(10)
                
                # Only create pie chart if there are at least 2 categories
                if len(value_counts) >= 2:
                    # Calculate "Other" category if needed
                    if results[col].nunique() > 10:
                        other_count = results[col].count() - value_counts.sum()
                        if other_count > 0:
                            value_counts["Other"] = other_count
                    
                    # Create the pie chart
                    fig = px.pie(
                        names=value_counts.index, 
                        values=value_counts.values,
                        title=f"Distribution by {col.replace('_', ' ').title()}",
                        color_discrete_sequence=self.color_scheme
                    )
                    
                    # Add percentage and counts in hover text
                    fig.update_traces(
                        textposition='inside',
                        textinfo='percent+label',
                        hovertemplate='%{label}<br>Count: %{value}<br>Percentage: %{percent}'
                    )
                    
                    # Apply optimization
                    fig = self.optimize_plotly_figure(fig)
                    
                    return {"type": "plotly", "data": fig}
        
        # If no suitable column found, try using source_file to compare log sources
        if "source_file" in results.columns:
            value_counts = results["source_file"].value_counts().head(10)
            
            if len(value_counts) >= 2:
                fig = px.pie(
                    names=value_counts.index, 
                    values=value_counts.values,
                    title="Distribution by Log Source",
                    color_discrete_sequence=self.color_scheme
                )
                
                fig.update_traces(
                    textposition='inside',
                    textinfo='percent+label',
                    hovertemplate='%{label}<br>Count: %{value}<br>Percentage: %{percent}'
                )
                
                fig = self.optimize_plotly_figure(fig)
                
                return {"type": "plotly", "data": fig}
        
        # If still no suitable visualization, return a message
        return {"type": "table", "data": {"message": "No suitable categorical data found for pie chart visualization"}}
    
    def _create_bar_visualization(self, results: pd.DataFrame, summary: Dict[str, Any]) -> Dict[str, Any]:
        """Create a bar chart visualization"""
        # Find the best categorical column for the bar chart
        candidate_columns = ["action", "protocol", "severity", "hostname", "message_id"]
        
        for col in candidate_columns:
            if col in results.columns and not results[col].isna().all():
                # Get value counts and limit to top 15
                counts = results[col].value_counts().head(15)
                
                # Only create bar chart if there are at least 2 categories
                if len(counts) >= 2:
                    fig = px.bar(
                        x=counts.index, 
                        y=counts.values,
                        title=f"Count by {col.replace('_', ' ').title()}",
                        labels={"x": col.replace('_', ' ').title(), "y": "Count"},
                        color=counts.index,
                        color_discrete_sequence=self.color_scheme
                    )
                    
                    fig.update_layout(
                        xaxis_title=col.replace('_', ' ').title(), 
                        yaxis_title="Number of Log Entries"
                    )
                    
                    # Apply optimization
                    fig = self.optimize_plotly_figure(fig)
                    
                    return {"type": "plotly", "data": fig}
        
        # If no categorical column works, try time-based grouping if available
        if self.time_column in results.columns and not results[self.time_column].isna().all():
            # Group by hour of day to see patterns
            if not pd.api.types.is_datetime64_any_dtype(results[self.time_column]):
                results[self.time_column] = pd.to_datetime(results[self.time_column], errors='coerce')
                
            hour_counts = results.groupby(results[self.time_column].dt.hour).size().reset_index(name='count')
            hour_counts.columns = ['hour', 'count']
            
            fig = px.bar(
                hour_counts, 
                x='hour', 
                y='count', 
                title="Log Frequency by Hour of Day",
                labels={"hour": "Hour (0-23)", "count": "Count"},
                color='hour',
                color_discrete_sequence=self.color_scheme
            )
            
            fig.update_layout(
                xaxis=dict(tickmode='linear', dtick=1, title="Hour of Day (0-23)"), 
                yaxis_title="Number of Log Entries"
            )
            
            fig = self.optimize_plotly_figure(fig)
            
            return {"type": "plotly", "data": fig}
                
        # If still no suitable visualization, return a message
        return {"type": "table", "data": {"message": "No suitable data found for bar chart visualization"}}
    
    def _create_heatmap_visualization(self, results: pd.DataFrame, summary: Dict[str, Any]) -> Dict[str, Any]:
        """Create a heatmap visualization (e.g., time patterns)"""
        # Ensure timestamp is datetime
        if not pd.api.types.is_datetime64_any_dtype(results[self.time_column]):
            results[self.time_column] = pd.to_datetime(results[self.time_column], errors='coerce')
        
        # Create hour and day columns for heatmap
        df = results.copy()
        df['hour'] = df[self.time_column].dt.hour
        df['day'] = df[self.time_column].dt.day_name()
        
        # Get proper weekday order
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        
        # Create pivot table for heatmap
        try:
            # Convert day names to categorical with proper order
            df['day'] = pd.Categorical(df['day'], categories=day_order, ordered=True)
            
            # Create pivot table with day and hour
            heatmap_data = df.pivot_table(index='day', columns='hour', aggfunc='size', fill_value=0)
            
            # Create the heatmap
            fig = px.imshow(
                heatmap_data,
                labels=dict(x="Hour of Day", y="Day of Week", color="Log Count"),
                title="Log Activity Heatmap by Day and Hour",
                color_continuous_scale="Viridis"
            )
            
            # Improve heatmap formatting
            fig.update_layout(
                xaxis=dict(
                    tickmode='linear',
                    dtick=1,
                    tickvals=list(range(24)),
                    ticktext=[f"{h}:00" for h in range(24)]
                )
            )
            
            # Add count text to cells
            annotations = []
            for i, day in enumerate(heatmap_data.index):
                for j, hour in enumerate(heatmap_data.columns):
                    count = heatmap_data.iloc[i, j]
                    if count > 0:
                        annotations.append(dict(
                            x=hour, 
                            y=day,
                            text=str(count),
                            showarrow=False,
                            font=dict(
                                color="white" if count > heatmap_data.values.max() / 2 else "black"
                            )
                        ))
            
            fig.update_layout(annotations=annotations)
            
            # Apply optimization
            fig = self.optimize_plotly_figure(fig)
            
            return {"type": "plotly", "data": fig}
            
        except Exception as e:
            print(f"Error creating heatmap: {e}")
            # Fall back to bar chart
            return self._create_bar_visualization(results, summary)
    
    def _create_summary_dashboard(self, results: pd.DataFrame, summary: Dict[str, Any]) -> Dict[str, Any]:
        """Create a comprehensive dashboard with multiple visualizations"""
        # Create a subplot figure with multiple charts
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                "Log Count by Category",
                "Time Distribution",
                "Top Sources",
                "Severity Distribution"
            ),
            specs=[
                [{"type": "bar"}, {"type": "pie"}],
                [{"type": "bar"}, {"type": "pie"}]
            ],
            vertical_spacing=0.12,
            horizontal_spacing=0.08
        )
        
        # --- First chart: Log count by category ---
        category_cols = ["action", "protocol", "hostname", "message_id"]
        for col in category_cols:
            if col in results.columns and not results[col].isna().all():
                counts = results[col].value_counts().head(8)
                if len(counts) >= 2:
                    fig.add_trace(
                        go.Bar(
                            x=counts.index,
                            y=counts.values,
                            name=col.replace('_', ' ').title(),
                            marker_color=self.color_scheme[0]
                        ),
                        row=1, col=1
                    )
                    break
        
        # --- Second chart: Time distribution ---
        if self.time_column in results.columns and not results[self.time_column].isna().all():
            # Ensure timestamp is datetime
            if not pd.api.types.is_datetime64_any_dtype(results[self.time_column]):
                results[self.time_column] = pd.to_datetime(results[self.time_column], errors='coerce')
                
            # Get hour distribution for pie chart
            hour_counts = results.groupby(results[self.time_column].dt.hour).size()
            
            # Group hours into periods of day
            periods = {
                "Night (0-5)": hour_counts.loc[0:5].sum(),
                "Morning (6-11)": hour_counts.loc[6:11].sum(),
                "Afternoon (12-17)": hour_counts.loc[12:17].sum(),
                "Evening (18-23)": hour_counts.loc[18:23].sum()
            }
            
            # Add pie chart of time periods
            fig.add_trace(
                go.Pie(
                    labels=list(periods.keys()),
                    values=list(periods.values()),
                    name="Time of Day",
                    marker=dict(colors=self.color_scheme[1:6])
                ),
                row=1, col=2
            )
        else:
            # Fallback: Use another categorical column
            for col in ["source_file", "protocol"]:
                if col in results.columns and col not in category_cols:
                    counts = results[col].value_counts().head(6)
                    if len(counts) >= 2:
                        fig.add_trace(
                            go.Pie(
                                labels=counts.index,
                                values=counts.values,
                                name=col.replace('_', ' ').title()
                            ),
                            row=1, col=2
                        )
                        break
        
        # --- Third chart: Top Sources (IPs or hostnames) ---
        for col in ["src_ip", "hostname", "dst_ip"]:
            if col in results.columns and not results[col].isna().all():
                counts = results[col].value_counts().head(8)
                if len(counts) >= 2:
                    fig.add_trace(
                        go.Bar(
                            x=counts.index,
                            y=counts.values,
                            name=col.replace('_', ' ').title(),
                            marker_color=self.color_scheme[2]
                        ),
                        row=2, col=1
                    )
                    break
        
        # --- Fourth chart: Severity distribution ---
        if "severity" in results.columns and not results["severity"].isna().all():
            severity_counts = results["severity"].value_counts().sort_index()
            
            # If severity is numeric, ensure all levels are represented
            try:
                severity_values = pd.to_numeric(severity_counts.index)
                
                # Fill in missing severity levels
                all_levels = range(int(min(severity_values)), int(max(severity_values)) + 1)
                for level in all_levels:
                    if level not in severity_values:
                        severity_counts[level] = 0
                
                # Sort by severity level
                severity_counts = severity_counts.sort_index()
                
                # Use sequential color scale for severity
                colors = px.colors.sequential.Reds
                
                # Create pie chart labels
                labels = [f"Level {k}" for k in severity_counts.index]
                
            except:
                # Non-numeric severity
                labels = severity_counts.index
                colors = self.color_scheme
            
            fig.add_trace(
                go.Pie(
                    labels=labels,
                    values=severity_counts.values,
                    name="Severity",
                    marker=dict(colors=colors)
                ),
                row=2, col=2
            )
        else:
            # Fallback: action or another categorical
            for col in ["action", "message_id"]:
                if col in results.columns and col not in category_cols:
                    counts = results[col].value_counts().head(6)
                    if len(counts) >= 2:
                        fig.add_trace(
                            go.Pie(
                                labels=counts.index,
                                values=counts.values,
                                name=col.replace('_', ' ').title()
                            ),
                            row=2, col=2
                        )
                        break
        
        # Format the dashboard
        fig.update_layout(
            title_text="Log Analysis Dashboard",
            showlegend=True,
            height=800,
            template="plotly_white"
        )
        
        # Update layout for each subplot
        fig.update_xaxes(title_text="Category", row=1, col=1)
        fig.update_yaxes(title_text="Count", row=1, col=1)
        
        fig.update_xaxes(title_text="Source", row=2, col=1)
        fig.update_yaxes(title_text="Count", row=2, col=1)
        
        # Update traces for pie charts
        fig.update_traces(
            textposition='inside',
            textinfo='percent',
            selector=dict(type='pie')
        )
        
        # Apply optimization
        fig.update_layout(
            colorway=self.color_scheme,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            margin=dict(l=50, r=50, t=100, b=50),
            font=dict(family="Arial, sans-serif", size=12)
        )
        
        return {"type": "plotly", "data": fig}