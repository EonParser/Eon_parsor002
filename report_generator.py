# report_generator.py
import os
import pandas as pd
from datetime import datetime
import tempfile # Keep for potential HTML temp files if needed

# --- ReportLab Imports ---
try:
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, PageBreak, Table, TableStyle
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.enums import TA_JUSTIFY, TA_LEFT, TA_CENTER, TA_RIGHT
    from reportlab.lib.units import inch, cm
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter, A4 # Use A4 or letter
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
             # Customize styles if needed
             self.styles['h1'].alignment = TA_CENTER
             self.styles['h2'].alignment = TA_LEFT
             self.styles['Normal'].alignment = TA_LEFT

    def _generate_pdf_report(self, report_data, save_path):
        """ Generates a PDF report using ReportLab. """
        if not REPORTLAB_AVAILABLE:
            raise ImportError("ReportLab library is required for PDF generation.")

        doc = SimpleDocTemplate(save_path, pagesize=A4,
                                title=report_data.get('title', "Log Analysis Report"))
        story = []

        # --- Title ---
        title = report_data.get('title', "Log Analysis Report")
        story.append(Paragraph(title, self.styles['h1']))
        story.append(Spacer(1, 0.2*inch))

        # --- Timestamp ---
        timestamp = report_data.get('timestamp', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        story.append(Paragraph(f"Report Generated: {timestamp}", self.styles['Normal']))
        story.append(Spacer(1, 0.3*inch))

        # --- Query ---
        query = report_data.get('query')
        if query:
            story.append(Paragraph("Query:", self.styles['h2']))
            # Use a Code style if available or format differently
            code_style = self.styles.get('Code', self.styles['Normal'])
            code_style.fontSize = 9
            story.append(Paragraph(query.replace('\n', '<br/>'), code_style))
            story.append(Spacer(1, 0.2*inch))

        # --- Summary ---
        summary = report_data.get('summary')
        if summary:
            story.append(Paragraph("Summary:", self.styles['h2']))
            summary_text = f"Total Matching Logs: {summary.get('total_logs', 'N/A')}<br/>"
            if 'earliest_log' in summary and pd.notna(summary['earliest_log']):
                time_format = '%Y-%m-%d %H:%M:%S %Z'
                start = summary['earliest_log'].strftime(time_format)
                end = summary['latest_log'].strftime(time_format)
                summary_text += f"Time Range of Results: {start} to {end}<br/>"
                summary_text += f"Time Span (Hours): {summary.get('time_span_hours', 'N/A'):.2f}<br/>"

            if summary.get('keywords'):
                summary_text += f"Keywords Searched: {', '.join(summary['keywords'])}<br/>"

            story.append(Paragraph(summary_text, self.styles['Normal']))
            story.append(Spacer(1, 0.1*inch))

            # Distributions (Example for log_type)
            if "log_type_distribution" in summary:
                 story.append(Paragraph("Log Type Distribution:", self.styles['h3']))
                 dist_data = [['Log Type', 'Count']]
                 # Sort and limit for display
                 sorted_types = sorted(summary["log_type_distribution"].items(), key=lambda item: item[1], reverse=True)
                 for k, v in sorted_types[:10]: # Limit rows in table
                     dist_data.append([Paragraph(str(k), self.styles['SmallText']), str(v)]) # Wrap long text
                 if len(sorted_types) > 10: dist_data.append(["...", "..."])

                 table = Table(dist_data, colWidths=[3*inch, 1*inch])
                 table.setStyle(TableStyle([
                     ('BACKGROUND', (0,0), (-1,0), colors.grey),
                     ('TEXTCOLOR',(0,0),(-1,0),colors.whitesmoke),
                     ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                     ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                     ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                     ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                     ('GRID', (0,0), (-1,-1), 1, colors.black)
                 ]))
                 story.append(table)
                 story.append(Spacer(1, 0.2*inch))

            # Add other distributions (action, IPs) similarly if needed

        # --- Visualization ---
        viz_path = report_data.get('visualization_path')
        if viz_path and os.path.exists(viz_path):
             story.append(PageBreak()) # Start visualization on new page
             story.append(Paragraph("Visualization:", self.styles['h2']))
             try:
                 # Adjust width/height as needed, maintain aspect ratio
                 img = Image(viz_path, width=6.5*inch, height=4.5*inch, kind='proportional')
                 story.append(img)
                 story.append(Spacer(1, 0.2*inch))
             except Exception as e:
                 story.append(Paragraph(f"Error embedding visualization: {e}", self.styles['Normal']))
                 print(f"Error reading/embedding image {viz_path}: {e}")


        # --- Results Sample ---
        results_df = report_data.get('results_sample')
        if results_df is not None and not results_df.empty:
            story.append(PageBreak()) # Start results on new page
            story.append(Paragraph(f"Results Sample (First {len(results_df)} Records):", self.styles['h2']))
            story.append(Spacer(1, 0.1*inch))

            # Prepare data for ReportLab Table (convert all to string)
            # Use a smaller font for table data
            table_style = self.styles['SmallText'] if 'SmallText' in self.styles else self.styles['Normal']
            table_style.fontSize = 7 # Smaller font size
            table_style.leading = 9 # Adjust line spacing

            # Function to wrap text in paragraphs for table cells
            def create_cell(text):
                return Paragraph(str(text), table_style)

            # Convert DataFrame to list of lists with Paragraph objects
            headers = [create_cell(col) for col in results_df.columns]
            data = [headers]
            for _, row in results_df.iterrows():
                 data.append([create_cell(val) if pd.notna(val) else '' for val in row])


            # Create and style the table
            # Calculate column widths dynamically or set fixed widths
            num_cols = len(results_df.columns)
            available_width = 7 * inch # Approx width available on A4
            col_width = available_width / num_cols if num_cols > 0 else available_width

            try:
                 table = Table(data, colWidths=[col_width] * num_cols)
                 table.setStyle(TableStyle([
                    ('BACKGROUND', (0,0), (-1,0), colors.darkblue), # Header background
                    ('TEXTCOLOR',(0,0),(-1,0),colors.whitesmoke),   # Header text color
                    ('ALIGN', (0,0), (-1,-1), 'LEFT'),             # Align all left
                    ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),          # Vertical align middle
                    ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'), # Header font
                    ('FONTSIZE', (0,0), (-1,-1), 7),               # Data font size
                    ('BOTTOMPADDING', (0,0), (-1,0), 8),           # Header padding
                    ('TOPPADDING', (0,1), (-1,-1), 4),             # Data padding
                    ('BOTTOMPADDING', (0,1), (-1,-1), 4),          # Data padding
                    ('BACKGROUND', (0,1), (-1,-1), colors.white), # Data background
                    ('GRID', (0,0), (-1,-1), 0.5, colors.grey)     # Grid lines
                 ]))
                 story.append(table)
            except Exception as table_err:
                 story.append(Paragraph(f"Error generating results table: {table_err}", self.styles['Normal']))
                 print(f"Reportlab table error: {table_err}")


        # --- Build PDF ---
        try:
            doc.build(story)
            print(f"PDF Report generated successfully: {save_path}")
            return save_path
        except Exception as e:
            print(f"❌ Error building PDF report: {e}\n{traceback.format_exc()}")
            # Re-raise or handle as appropriate
            raise


    def _generate_html_report(self, report_data, save_path):
        """ Generates an HTML report (kept similar to original for now). """
        html = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <title>{report_data.get('title', 'Log Analysis Report')}</title>
            <style>
                body {{ font-family: sans-serif; margin: 20px; line-height: 1.5; }}
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
            </style>
        </head>
        <body>
            <h1>{report_data.get('title', 'Log Analysis Report')}</h1>
            <p style="text-align: center;">Generated: {report_data.get('timestamp', 'N/A')}</p>
        """

        # Query
        query = report_data.get('query')
        if query:
            html += f"<h2>Query</h2><div class='query'><p class='code'>{query.replace('<', '&lt;').replace('>', '&gt;')}</p></div>"

        # Summary
        summary = report_data.get('summary')
        if summary:
            html += "<h2>Summary</h2><div class='summary'>"
            html += f"<p><strong>Total Matching Logs:</strong> {summary.get('total_logs', 'N/A')}</p>"
            if 'earliest_log' in summary and pd.notna(summary['earliest_log']):
                time_format = '%Y-%m-%d %H:%M:%S %Z'
                start = summary['earliest_log'].strftime(time_format)
                end = summary['latest_log'].strftime(time_format)
                html += f"<p><strong>Time Range of Results:</strong> {start} to {end}</p>"
                html += f"<p><strong>Time Span (Hours):</strong> {summary.get('time_span_hours', 'N/A'):.2f}</p>"

            if summary.get('keywords'):
                html += f"<p><strong>Keywords Searched:</strong> {', '.join(summary['keywords'])}</p>"

            # Distributions (add more as needed)
            if "log_type_distribution" in summary:
                html += "<p><strong>Log Type Distribution:</strong></p><ul>"
                sorted_types = sorted(summary["log_type_distribution"].items(), key=lambda item: item[1], reverse=True)
                for k, v in sorted_types[:10]: html += f"<li>{k}: {v}</li>"
                if len(sorted_types) > 10: html += "<li>...</li>"
                html += "</ul>"

            html += "</div>" # End summary div


        # Visualization
        viz_path = report_data.get('visualization_path')
        if viz_path and os.path.exists(viz_path):
            html += "<h2>Visualization</h2>"
            try:
                # Embed image using base64 data URI
                import base64
                with open(viz_path, 'rb') as img_file:
                    img_data = base64.b64encode(img_file.read()).decode()
                html += f"<img src='data:image/png;base64,{img_data}' alt='Visualization' class='visualization'>"
            except Exception as e:
                html += f"<p><em>Error embedding visualization: {e}</em></p>"


        # Results Sample
        results_df = report_data.get('results_sample')
        if results_df is not None and not results_df.empty:
            html += f"<h2>Results Sample (First {len(results_df)} Records)</h2>"
            # Convert DataFrame to HTML table, escape content
            html += results_df.to_html(index=False, escape=True, border=0) # Use pandas to_html

        # Footer
        html += """
            <div class='footer'>
                <p>Generated by EONParser</p>
            </div>
        </body>
        </html>
        """

        try:
            with open(save_path, 'w', encoding='utf-8') as f:
                f.write(html)
            print(f"HTML Report generated successfully: {save_path}")
            return save_path
        except Exception as e:
            print(f"❌ Error writing HTML report: {e}")
            raise

    # --- Main generate method ---
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
                return self._generate_html_report(report_data, save_path)
            else:
                print(f"Error: Unsupported report format '{report_format}'")
                return None
        except Exception as e:
             print(f"Report generation failed: {e}")
             # Clean up potential partial file?
             if os.path.exists(save_path):
                 try: os.remove(save_path)
                 except OSError: pass
             return None # Indicate failure