# report_generator.py
import os
import pandas as pd
from datetime import datetime
import tempfile  # Keep for potential HTML temp files if needed

# --- ReportLab Imports ---
try:
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, PageBreak, Table, TableStyle
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_JUSTIFY, TA_LEFT, TA_CENTER, TA_RIGHT
    from reportlab.lib.units import inch, cm
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter, A4  # Use A4 or letter
    REPORTLAB_AVAILABLE = True
except ImportError:
    print("⚠️ ReportLab library not found. PDF generation will be disabled.")
    print("Install it using: pip install reportlab")
    REPORTLAB_AVAILABLE = False
    # Define dummy classes if reportlab is missing to avoid NameErrors later
    class SimpleDocTemplate: pass
    class Paragraph: pass
    class Spacer: pass
    class Image: pass
    class PageBreak: pass
    class Table: pass
    class TableStyle: pass
    def getSampleStyleSheet(): return {}
    TA_CENTER = 1
    inch = 72.0
    cm = inch / 2.54
    colors = None
    A4 = (595.27, 841.89)


class ReportGenerator:
    def __init__(self):
        self.styles = getSampleStyleSheet() if REPORTLAB_AVAILABLE else {}
        if REPORTLAB_AVAILABLE:
            # Create a copy of styles to modify
            self.custom_styles = {}
            
            # Customize styles by creating new style objects
            self.custom_styles['Heading1'] = ParagraphStyle(
                'Heading1',
                parent=self.styles['Heading1'],
                alignment=TA_CENTER,
                fontSize=16,
                spaceAfter=12
            )
            
            self.custom_styles['Heading2'] = ParagraphStyle(
                'Heading2',
                parent=self.styles['Heading2'],
                fontSize=14,
                spaceBefore=12,
                spaceAfter=6
            )
            
            self.custom_styles['Normal'] = ParagraphStyle(
                'Normal',
                parent=self.styles['Normal'],
                fontSize=10,
                spaceBefore=6,
                spaceAfter=6
            )
            
            self.custom_styles['Code'] = ParagraphStyle(
                'Code',
                parent=self.styles['Normal'],
                fontName='Courier',
                fontSize=9,
                leftIndent=20,
                rightIndent=20,
                spaceBefore=6,
                spaceAfter=6,
                backColor=colors.lightgrey
            )
            
            # Replace original styles with our customized version
            self.styles = self.custom_styles

    def _generate_pdf_report(self, report_data, save_path):
        """ Generates a PDF report using ReportLab. """
        if not REPORTLAB_AVAILABLE:
            raise ImportError("ReportLab library is required for PDF generation.")

        doc = SimpleDocTemplate(
            save_path, 
            pagesize=A4,
            title=report_data.get('title', "Log Analysis Report"),
            author="EONParser",
            subject="Log Analysis Report"
        )
        story = []

        # --- Title ---
        title = report_data.get('title', "Log Analysis Report")
        story.append(Paragraph(title, self.styles['Heading1']))
        story.append(Spacer(1, 0.2*inch))

        # --- Timestamp ---
        timestamp = report_data.get('timestamp', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        story.append(Paragraph(f"Report Generated: {timestamp}", self.styles['Normal']))
        story.append(Spacer(1, 0.3*inch))

        # --- Query ---
        query = report_data.get('query')
        if query:
            story.append(Paragraph("Search Parameters:", self.styles['Heading2']))
            story.append(Paragraph(query.replace('\n', '<br/>'), self.styles['Code']))
            story.append(Spacer(1, 0.2*inch))

        # --- Summary ---
        summary = report_data.get('summary')
        if summary:
            story.append(Paragraph("Summary:", self.styles['Heading2']))
            summary_text = f"Total Matching Logs: {summary.get('total_logs', 'N/A')}<br/>"
            
            if 'time_range' in summary and summary['time_range']:
                start = summary['time_range'].get('start')
                end = summary['time_range'].get('end')
                
                if start:
                    summary_text += f"Time Range Start: {start.strftime('%Y-%m-%d %H:%M:%S %Z')}<br/>"
                if end:
                    summary_text += f"Time Range End: {end.strftime('%Y-%m-%d %H:%M:%S %Z')}<br/>"
            
            if 'earliest_log' in summary and pd.notna(summary['earliest_log']):
                time_format = '%Y-%m-%d %H:%M:%S %Z'
                start = summary['earliest_log'].strftime(time_format)
                end = summary['latest_log'].strftime(time_format)
                summary_text += f"Time Range of Results: {start} to {end}<br/>"
                summary_text += f"Time Span (Hours): {summary.get('time_span_hours', 'N/A'):.2f}<br/>"

            if summary.get('query_params'):
                summary_text += "<br/>Search Criteria:<br/>"
                for key, value in summary.get('query_params', {}).items():
                    if value:
                        summary_text += f"- {key.replace('_', ' ').title()}: {value}<br/>"

            story.append(Paragraph(summary_text, self.styles['Normal']))
            story.append(Spacer(1, 0.1*inch))

            # Distributions (Example for log_type)
            for field in ['action', 'protocol', 'severity', 'hostname', 'message_id']:
                dist_key = f"{field}_distribution"
                if dist_key in summary:
                    story.append(Paragraph(f"{field.replace('_', ' ').title()} Distribution (Top 10):", self.styles['Heading2']))
                    dist_data = [['Value', 'Count']]
                    # Sort and limit for display
                    sorted_items = sorted(summary[dist_key].items(), key=lambda item: item[1], reverse=True)
                    for k, v in sorted_items[:10]:  # Limit rows in table
                        dist_data.append([k, str(v)])
                    if len(sorted_items) > 10: 
                        dist_data.append(["...", "..."])

                    table = Table(dist_data, colWidths=[4*inch, 1*inch])
                    table.setStyle(TableStyle([
                        ('BACKGROUND', (0,0), (-1,0), colors.grey),
                        ('TEXTCOLOR',(0,0),(-1,0),colors.whitesmoke),
                        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                        ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                        ('GRID', (0,0), (-1,-1), 1, colors.black)
                    ]))
                    story.append(table)
                    story.append(Spacer(1, 0.2*inch))

            # IP distribution tables
            for ip_field in ['top_src_ip', 'top_dst_ip']:
                if ip_field in summary:
                    field_name = 'Source IP' if ip_field == 'top_src_ip' else 'Destination IP'
                    story.append(Paragraph(f"Top {field_name} Addresses:", self.styles['Heading2']))
                    ip_data = [['IP Address', 'Count']]
                    sorted_ips = sorted(summary[ip_field].items(), key=lambda item: item[1], reverse=True)
                    for ip, count in sorted_ips[:10]:
                        ip_data.append([ip, str(count)])
                    if len(sorted_ips) > 10: 
                        ip_data.append(["...", "..."])
                        
                    ip_table = Table(ip_data, colWidths=[4*inch, 1*inch])
                    ip_table.setStyle(TableStyle([
                        ('BACKGROUND', (0,0), (-1,0), colors.grey),
                        ('TEXTCOLOR',(0,0),(-1,0),colors.whitesmoke),
                        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                        ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                        ('GRID', (0,0), (-1,-1), 1, colors.black)
                    ]))
                    story.append(ip_table)
                    story.append(Spacer(1, 0.2*inch))

        # --- Visualization ---
        viz_path = report_data.get('visualization_path')
        current_viz = report_data.get('current_visualization')

        if self.include_viz and ((viz_path and os.path.exists(viz_path)) or current_viz):
            story.append(PageBreak())  # Start visualization on new page
            story.append(Paragraph("Visualization:", self.styles['Heading2']))
            
            if viz_path and os.path.exists(viz_path):
                try:
                    # Adjust width/height as needed, maintain aspect ratio
                    img = Image(viz_path, width=6.5*inch, height=4.5*inch)
                    story.append(img)
                    story.append(Spacer(1, 0.2*inch))
                except Exception as e:
                    story.append(Paragraph(f"Error embedding visualization: {e}", self.styles['Normal']))
                    print(f"Error reading/embedding image {viz_path}: {e}")
            elif current_viz and current_viz.get('type') == 'plotly' and 'figure' in current_viz:
                # For PDF, we need to save the visualization to a temporary file
                try:
                    temp_viz_file = tempfile.NamedTemporaryFile(suffix='.png', delete=False).name
                    fig = current_viz['figure']
                    img_bytes = fig.to_image(format='png', scale=1.5)
                    with open(temp_viz_file, 'wb') as f:
                        f.write(img_bytes)
                    
                    img = Image(temp_viz_file, width=6.5*inch, height=4.5*inch)
                    story.append(img)
                    story.append(Spacer(1, 0.2*inch))
                    
                    # Clean up temp file
                    os.unlink(temp_viz_file)
                except Exception as e:
                    story.append(Paragraph(f"Error creating visualization image: {e}", self.styles['Normal']))

        # --- Results Sample ---
        results_df = report_data.get('results_sample')
        if results_df is not None and not results_df.empty:
            story.append(PageBreak()) # Start results on new page
            story.append(Paragraph(f"Results Sample (First {len(results_df)} Records):", self.styles['Heading2']))
            story.append(Spacer(1, 0.1*inch))

            # Prepare data for ReportLab Table (convert all to string)
            # Convert DataFrame to list of lists for table
            headers = [col for col in results_df.columns]
            data = [headers]
            
            for _, row in results_df.iterrows():
                data.append([str(val) if pd.notna(val) else '' for val in row])

            # Create and style the table - limit columns to fit on page
            max_cols = min(len(headers), 5)  # Limit to 5 columns to fit on page
            truncated_data = [row[:max_cols] for row in data]
            if max_cols < len(headers):
                # Add note about truncated columns
                story.append(Paragraph(f"Note: Only showing {max_cols} of {len(headers)} columns due to space constraints.", 
                                      self.styles['Normal']))
                
            col_widths = [1.2*inch] * max_cols  # Evenly distribute column widths
            
            try:
                table = Table(truncated_data, colWidths=col_widths)
                table.setStyle(TableStyle([
                    ('BACKGROUND', (0,0), (-1,0), colors.darkblue), # Header background
                    ('TEXTCOLOR',(0,0),(-1,0),colors.whitesmoke),   # Header text color
                    ('ALIGN', (0,0), (-1,-1), 'LEFT'),             # Align all left
                    ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),          # Vertical align middle
                    ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'), # Header font
                    ('FONTSIZE', (0,0), (-1,-1), 7),               # Data font size
                    ('BOTTOMPADDING', (0,0), (-1,0), 8),           # Header padding
                    ('BACKGROUND', (0,1), (-1,-1), colors.white),  # Data background
                    ('GRID', (0,0), (-1,-1), 0.5, colors.grey),    # Grid lines
                    ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.whitesmoke, colors.white])  # Alternating row colors
                ]))
                story.append(table)
            except Exception as table_err:
                story.append(Paragraph(f"Error generating results table: {table_err}", self.styles['Normal']))
                print(f"Reportlab table error: {table_err}")

        # --- Build PDF ---
        doc.build(story)
        print(f"PDF Report generated successfully: {save_path}")
        return save_path

    def _generate_simple_html_report(self, report_data):
        """Generate a simple HTML report as a string"""
        html = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <title>{report_data.get('title', 'Log Analysis Report')}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; line-height: 1.5; }}
                h1, h2, h3 {{ color: #2c3e50; border-bottom: 1px solid #eee; padding-bottom: 5px; }}
                h1 {{ text-align: center; }}
                table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; font-size: 0.9em; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; vertical-align: top; }}
                th {{ background-color: #f2f2f2; font-weight: bold; }}
                tr:nth-child(even) {{ background-color: #f9f9f9; }}
                .summary, .query {{ background-color: #eef6f9; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
                img.visualization {{ max-width: 100%; height: auto; border: 1px solid #ccc; margin-top: 10px; }}
                .footer {{ margin-top: 30px; font-size: 0.8em; color: #7f8c8d; text-align: center; }}
                .code {{ background-color: #f5f5f5; padding: 5px; border-radius: 3px; font-family: monospace; }}
                ul {{ padding-left: 20px; }}
                .distribution {{ margin-bottom: 20px; }}
                .chart-container {{ text-align: center; margin: 20px 0; }}
            </style>
        </head>
        <body>
            <h1>{report_data.get('title', 'Log Analysis Report')}</h1>
            <p style="text-align: center;">Generated: {report_data.get('timestamp', 'N/A')}</p>
        """

        # Query section
        query = report_data.get('query')
        if query:
            html += f'<h2>Search Parameters</h2><div class="query"><pre class="code">{query}</pre></div>'

        # Summary section
        summary = report_data.get('summary')
        if summary:
            html += '<h2>Summary</h2><div class="summary">'
            html += f'<p><strong>Total Matching Logs:</strong> {summary.get("total_logs", "N/A")}</p>'
            
            # Time range info
            if 'time_range' in summary and summary['time_range']:
                start = summary['time_range'].get('start')
                end = summary['time_range'].get('end')
                
                if start:
                    html += f'<p><strong>Time Range Start:</strong> {start.strftime("%Y-%m-%d %H:%M:%S %Z")}</p>'
                if end:
                    html += f'<p><strong>Time Range End:</strong> {end.strftime("%Y-%m-%d %H:%M:%S %Z")}</p>'
            
            if 'earliest_log' in summary and pd.notna(summary['earliest_log']):
                time_format = '%Y-%m-%d %H:%M:%S %Z'
                start = summary['earliest_log'].strftime(time_format)
                end = summary['latest_log'].strftime(time_format)
                html += f'<p><strong>Time Range of Results:</strong> {start} to {end}</p>'
                html += f'<p><strong>Time Span (Hours):</strong> {summary.get("time_span_hours", "N/A"):.2f}</p>'

            # Show search criteria
            if summary.get('query_params'):
                html += '<h3>Search Criteria</h3><ul>'
                for key, value in summary.get('query_params', {}).items():
                    if value:
                        html += f'<li><strong>{key.replace("_", " ").title()}:</strong> {value}</li>'
                html += '</ul>'

            html += '</div>'  # End summary div

            # Add distributions
            for field in ['action', 'protocol', 'severity', 'hostname', 'message_id']:
                dist_key = f'{field}_distribution'
                if dist_key in summary:
                    html += f'<div class="distribution"><h3>{field.replace("_", " ").title()} Distribution</h3><table>'
                    html += '<tr><th>Value</th><th>Count</th></tr>'
                    
                    sorted_items = sorted(summary[dist_key].items(), key=lambda item: item[1], reverse=True)
                    for k, v in sorted_items[:10]:
                        html += f'<tr><td>{k}</td><td>{v}</td></tr>'
                    
                    if len(sorted_items) > 10:
                        html += '<tr><td colspan="2">...</td></tr>'
                    
                    html += '</table></div>'
            
            # Add IP statistics
            for ip_field in ['top_src_ip', 'top_dst_ip']:
                if ip_field in summary:
                    field_name = 'Source IP' if ip_field == 'top_src_ip' else 'Destination IP'
                    html += f'<div class="distribution"><h3>Top {field_name} Addresses</h3><table>'
                    html += '<tr><th>IP Address</th><th>Count</th></tr>'
                    
                    sorted_ips = sorted(summary[ip_field].items(), key=lambda item: item[1], reverse=True)
                    for ip, count in sorted_ips[:10]:
                        html += f'<tr><td>{ip}</td><td>{count}</td></tr>'
                    
                    if len(sorted_ips) > 10:
                        html += '<tr><td colspan="2">...</td></tr>'
                    
                    html += '</table></div>'

        # Visualization section
        current_visualization = report_data.get('current_visualization')
        if current_visualization and current_visualization.get('type') == 'plotly' and current_visualization.get('figure'):
            html += '<h2>Visualization</h2>'
            html += '<div class="chart-container">'
            
            try:
                # For HTML reports, we can embed the Plotly figure directly
                fig = current_visualization.get('figure')
                if fig:
                    # This is the key line that might be missing - it converts the Plotly figure to HTML
                    viz_html = fig.to_html(include_plotlyjs='cdn', full_html=False)
                    html += viz_html
            except Exception as e:
                html += f'<p><em>Error embedding visualization: {e}</em></p>'
            
            html += '</div>'
        elif report_data.get('visualization_path'):
            # For static image from path
            viz_path = report_data.get('visualization_path')
            if os.path.exists(viz_path):
                html += '<h2>Visualization</h2>'
                html += f'<div class="chart-container"><img src="{viz_path}" alt="Visualization" class="visualization"></div>'

        # Results Sample section
        results_df = report_data.get('results_sample')
        if results_df is not None and not results_df.empty:
            html += f'<h2>Results Sample (First {len(results_df)} Records)</h2>'
            html += results_df.to_html(index=False, escape=True, classes="dataframe")

        # Footer
        html += """
            <div class="footer">
                <p>Generated by EONParser</p>
            </div>
        </body>
        </html>
        """
        
        return html

    def generate_report(self, report_data, save_path, report_format='pdf'):
        """
        Generates the report in the specified format and saves it to save_path.

        Args:
            report_data (dict): Dictionary containing report content.
            save_path (str): The full path where the report should be saved.
            report_format (str): 'pdf' or 'html'.

        Returns:
            str: The path to the generated report file, or None on failure.
        """
        print(f"Generating {report_format.upper()} report to: {save_path}")
        try:
            if report_format == 'pdf':
                return self._generate_pdf_report(report_data, save_path)
            elif report_format == 'html':
                # Generate HTML content
                html_content = self._generate_simple_html_report(report_data)
                
                # Write to file
                with open(save_path, 'w', encoding='utf-8') as f:
                    f.write(html_content)
                    
                return save_path
            else:
                print(f"Error: Unsupported report format '{report_format}'")
                return None
        except Exception as e:
             import traceback
             print(f"Report generation failed: {e}")
             print(traceback.format_exc())
             # Clean up potential partial file
             if os.path.exists(save_path):
                 try: 
                     os.remove(save_path)
                 except OSError: 
                     pass
             return None # Indicate failure