# gui.py
import sys
import traceback
from PyQt5.QtWidgets import (QApplication, QMainWindow, QTabWidget, QWidget, QVBoxLayout, QHBoxLayout,
                             QPushButton, QFileDialog, QTreeWidget, QTreeWidgetItem, QTextEdit, QLabel,
                             QRadioButton, QCheckBox, QLineEdit, QProgressBar, QFrame, QMessageBox, QGridLayout) # Added QMessageBox
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QUrl # Added QUrl
from PyQt5.QtGui import QDesktopServices # Added QDesktopServices
import pandas as pd
import os
import numpy as np # Import numpy
from datetime import datetime
# Use Plotly's Qt backend integration if available and desired, or keep Matplotlib
# from plotly.offline import plot
import plotly.graph_objs as go
# Using Matplotlib backend for embedding in PyQt

from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
import tempfile
import webbrowser # Keep for opening HTML reports potentially

# --- Local Imports ---
from nlp_engine import NLPQueryProcessor
from log_parser import LogParser # Will use the new CSV parser
from analyzer import LogAnalyzer   # Will use the updated analyzer
from log_visualizer import LogVisualizer
from report_generator import ReportGenerator # Will use updated report generator

# --- Worker Thread (No changes needed) ---
class WorkerThread(QThread):
    finished = pyqtSignal(object)
    error = pyqtSignal(str)

    def __init__(self, func, *args, **kwargs): # Allow passing args/kwargs
        super().__init__()
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def run(self):
        try:
            result = self.func(*self.args, **self.kwargs) # Pass args/kwargs
            self.finished.emit(result)
        except Exception as e:
            self.error.emit(f"Thread error: {str(e)}\n{traceback.format_exc()}")

    def __del__(self):
        # It's generally better practice to manage thread lifetime explicitly
        # rather than relying on __del__. Ensure threads are properly cleaned up.
        # self.wait() # Avoid wait in __del__ if possible
        pass


# --- Main GUI Class ---
class EONParserGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        try:
            self.setWindowTitle("EONParser - CSV Log Analysis Tool") # Updated title
            self.setGeometry(100, 100, 1200, 800)
            self.setMinimumSize(1000, 700)

            # Initialize components
            self.nlp_processor = NLPQueryProcessor()
            self.log_parser = LogParser() # Uses the new CSV parser
            self.analyzer = LogAnalyzer()
            self.visualizer = LogVisualizer()
            self.report_generator = ReportGenerator() # Uses new PDF generator

            # Stores loaded DataFrames (key: filename, value: DataFrame)
            self.log_data = {}
            self.current_results = pd.DataFrame() # Initialize as empty DataFrame
            self.current_summary = {}
            self.current_visualization = None
            self.active_threads = [] # Use a list to manage active threads

            self.setup_ui()
        except Exception as e:
            QMessageBox.critical(self, "Initialization Error", f"Failed to initialize: {e}\n{traceback.format_exc()}")
            sys.exit(1)

    def setup_ui(self):
        # (Keep UI setup mostly the same, adjust labels if needed for CSV)
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        main_layout = QVBoxLayout(main_widget)

        self.tabs = QTabWidget()
        main_layout.addWidget(self.tabs)

        self.upload_tab = QWidget()
        self.query_tab = QWidget()
        self.results_tab = QWidget()
        self.visualization_tab = QWidget()
        self.report_tab = QWidget()

        self.tabs.addTab(self.upload_tab, "Upload CSV Logs") # Updated tab name
        self.tabs.addTab(self.query_tab, "Query")
        self.tabs.addTab(self.results_tab, "Results")
        self.tabs.addTab(self.visualization_tab, "Visualization")
        self.tabs.addTab(self.report_tab, "Report")

        self.setup_upload_tab()
        self.setup_query_tab()
        self.setup_results_tab()
        self.setup_visualization_tab()
        self.setup_report_tab()

        # Status Bar
        self.status_label = QLabel("Ready")
        self.progress_bar = QProgressBar()
        self.progress_bar.setMaximum(100)
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(False) # Hide initially
        status_layout = QHBoxLayout()
        status_layout.addWidget(self.status_label, 1) # Give label more stretch space
        status_layout.addWidget(self.progress_bar)
        main_layout.addLayout(status_layout)


    def setup_upload_tab(self):
        layout = QVBoxLayout(self.upload_tab)
        layout.addWidget(QLabel("<h2>Upload CSV Log Files</h2>", alignment=Qt.AlignCenter)) # Updated label

        btn_layout = QHBoxLayout()
        upload_file_btn = QPushButton("Upload CSV File") # Updated button text
        upload_dir_btn = QPushButton("Upload Directory (CSVs)") # Updated button text
        clear_btn = QPushButton("Clear All Logs")
        upload_file_btn.clicked.connect(self.upload_log_file)
        upload_dir_btn.clicked.connect(self.upload_log_directory)
        clear_btn.clicked.connect(self.clear_logs)
        btn_layout.addWidget(upload_file_btn)
        btn_layout.addWidget(upload_dir_btn)
        btn_layout.addStretch()
        btn_layout.addWidget(clear_btn)
        layout.addLayout(btn_layout)

        self.file_tree = QTreeWidget()
        # Added 'Columns' - useful for CSVs
        self.file_tree.setHeaderLabels(["Filename", "Rows", "Columns", "Status"])
        self.file_tree.setColumnWidth(0, 350)
        self.file_tree.setColumnWidth(1, 100)
        self.file_tree.setColumnWidth(2, 100)
        self.file_tree.setColumnWidth(3, 150)
        layout.addWidget(self.file_tree)

        layout.addWidget(QLabel("Upload individual CSV files or directories containing CSVs.", alignment=Qt.AlignCenter))

    # --- Query Tab (No major changes needed) ---
    def setup_query_tab(self):
        layout = QVBoxLayout(self.query_tab)
        layout.addWidget(QLabel("<h2>Natural Language Query</h2>", alignment=Qt.AlignCenter))

        self.query_edit = QTextEdit("Show firewall denies in the last hour") # Example query
        self.query_edit.setMaximumHeight(100)
        layout.addWidget(QLabel("Enter your query (e.g., 'find blocks from 1.2.3.4 yesterday', 'count errors by source_file'):"))
        layout.addWidget(self.query_edit)

        # (Optional: Add more relevant CSV examples)
        examples = [
            "Show firewall denies in the last hour",
            "Find traffic to 8.8.8.8 today",
            "Count events by action",
            "Show trend of logs over time",
            "Visualize distribution of protocols",
            "Find logs for user 'admin' in source_file 'vpn.csv'"
        ]
        example_layout = QHBoxLayout()
        for example in examples[:3]: # Limit examples shown initially
            btn = QPushButton(example)
            btn.setStyleSheet("text-align:left; padding: 4px;")
            btn.clicked.connect(lambda _, e=example: self.query_edit.setText(e))
            example_layout.addWidget(btn)
        layout.addLayout(example_layout)


        self.params_display = QTextEdit()
        self.params_display.setReadOnly(True)
        self.params_display.setMaximumHeight(150) # Limit height
        layout.addWidget(QLabel("Interpreted Query Parameters:"))
        layout.addWidget(self.params_display)

        btn_layout = QHBoxLayout()
        parse_btn = QPushButton("Parse Query")
        run_btn = QPushButton("Run Query")
        parse_btn.clicked.connect(self.parse_query)
        run_btn.clicked.connect(self.run_query)
        btn_layout.addWidget(parse_btn)
        btn_layout.addWidget(run_btn)
        layout.addLayout(btn_layout)

    # --- Results Tab (No major changes needed) ---
    def setup_results_tab(self):
        layout = QVBoxLayout(self.results_tab)
        layout.addWidget(QLabel("<h2>Analysis Results</h2>", alignment=Qt.AlignCenter))

        # Use QSplitter for adjustable summary/results view
        splitter = QtWidgets.QSplitter(Qt.Vertical) # Requires importing QtWidgets

        self.summary_display = QTextEdit()
        self.summary_display.setReadOnly(True)
        summary_frame = QFrame()
        summary_layout = QVBoxLayout(summary_frame)
        summary_layout.addWidget(QLabel("Summary:"))
        summary_layout.addWidget(self.summary_display)
        splitter.addWidget(summary_frame)


        self.results_tree = QTreeWidget()
        # Increase row height for better readability
        self.results_tree.setStyleSheet("QTreeWidget::item { height: 25px; }")
        results_frame = QFrame()
        results_layout = QVBoxLayout(results_frame)
        results_layout.addWidget(QLabel("Result Records (Sample):"))
        results_layout.addWidget(self.results_tree)
        splitter.addWidget(results_frame)

        # Adjust initial sizes
        splitter.setSizes([200, 600]) # Give more space to results initially
        layout.addWidget(splitter)


        btn_layout = QHBoxLayout()
        visualize_btn = QPushButton("Visualize Results")
        export_btn = QPushButton("Export Results (CSV)") # Specify format
        visualize_btn.clicked.connect(self.visualize_results)
        export_btn.clicked.connect(self.export_results)
        btn_layout.addWidget(visualize_btn)
        btn_layout.addStretch()
        btn_layout.addWidget(export_btn)
        layout.addLayout(btn_layout)

    # --- Visualization Tab (No major changes needed, ensure LogVisualizer handles DataFrame) ---
    def setup_visualization_tab(self):
        layout = QVBoxLayout(self.visualization_tab)
        layout.addWidget(QLabel("<h2>Visualization</h2>", alignment=Qt.AlignCenter))

        # Options Frame
        options_frame = QFrame()
        options_frame.setFrameShape(QFrame.StyledPanel)
        options_layout = QHBoxLayout(options_frame) # Use QHBoxLayout for horizontal options
        options_layout.addWidget(QLabel("Chart Type:"))

        self.viz_type_group = QtWidgets.QButtonGroup(self) # Use button group

        self.viz_type = "auto" # Default
        # Use a dictionary for easier management
        viz_options = {
             "Auto": "auto", "Trend": "trend", "Pie": "pie", "Bar": "bar", "Heatmap": "heatmap"
        }

        for text, value in viz_options.items():
            radio = QRadioButton(text)
            if value == self.viz_type:
                radio.setChecked(True)
            # Use lambda with default argument to capture correct value
            radio.toggled.connect(lambda checked, v=value: self.set_viz_type(v) if checked else None)
            options_layout.addWidget(radio)
            self.viz_type_group.addButton(radio)

        options_layout.addStretch()
        generate_btn = QPushButton("Generate Visualization")
        generate_btn.clicked.connect(self.generate_visualization)
        options_layout.addWidget(generate_btn)
        layout.addWidget(options_frame)


        # Frame to hold the plot canvas
        self.viz_canvas_frame = QFrame()
        self.viz_canvas_frame.setFrameShape(QFrame.StyledPanel)
        # Use a layout inside the frame to manage the canvas
        self.viz_canvas_layout = QVBoxLayout(self.viz_canvas_frame)
        layout.addWidget(self.viz_canvas_frame, 1) # Allow frame to stretch


        export_btn = QPushButton("Export Visualization (PNG)")
        export_btn.clicked.connect(self.export_visualization)
        # Add alignment or place in a layout for better positioning
        export_layout = QHBoxLayout()
        export_layout.addStretch()
        export_layout.addWidget(export_btn)
        layout.addLayout(export_layout)

    # --- Report Tab (Integrate ReportLab PDF generation) ---
    def setup_report_tab(self):
        layout = QVBoxLayout(self.report_tab)
        layout.addWidget(QLabel("<h2>Report Generation</h2>", alignment=Qt.AlignCenter))

        options_frame = QFrame()
        options_frame.setFrameShape(QFrame.StyledPanel)
        options_layout = QGridLayout(options_frame) # Use QGridLayout for better alignment

        options_layout.addWidget(QLabel("Report Title:"), 0, 0)
        self.report_title = QLineEdit("Log Analysis Report")
        options_layout.addWidget(self.report_title, 0, 1, 1, 2) # Span 2 columns

        options_layout.addWidget(QLabel("Include Sections:"), 1, 0)
        self.include_summary = QCheckBox("Summary", checked=True)
        self.include_results_sample = QCheckBox("Results Sample", checked=True) # Changed label
        self.include_viz = QCheckBox("Visualization", checked=True)
        self.include_query = QCheckBox("Query Details", checked=True)
        options_layout.addWidget(self.include_summary, 1, 1)
        options_layout.addWidget(self.include_results_sample, 1, 2)
        options_layout.addWidget(self.include_viz, 2, 1)
        options_layout.addWidget(self.include_query, 2, 2)


        options_layout.addWidget(QLabel("Format:"), 3, 0)
        self.report_format_group = QtWidgets.QButtonGroup(self)
        self.report_format = "pdf" # Default to PDF
        pdf_radio = QRadioButton("PDF")
        html_radio = QRadioButton("HTML")
        pdf_radio.setChecked(True)
        pdf_radio.toggled.connect(lambda checked: self.set_report_format("pdf") if checked else None)
        html_radio.toggled.connect(lambda checked: self.set_report_format("html") if checked else None)
        self.report_format_group.addButton(pdf_radio)
        self.report_format_group.addButton(html_radio)
        options_layout.addWidget(pdf_radio, 3, 1)
        options_layout.addWidget(html_radio, 3, 2)


        generate_btn = QPushButton("Generate and Save Report")
        generate_btn.clicked.connect(self.generate_report)
        options_layout.addWidget(generate_btn, 4, 0, 1, 3) # Span all columns

        layout.addWidget(options_frame)

        # Remove the preview text edit, as proper PDF isn't easily previewed directly
        # layout.addWidget(QLabel("Report Preview:")) # Remove
        # self.report_preview = QTextEdit() # Remove
        # self.report_preview.setReadOnly(True) # Remove
        # layout.addWidget(self.report_preview) # Remove

    def set_viz_type(self, viz_type):
        self.viz_type = viz_type
        print(f"Visualization type set to: {self.viz_type}")

    def set_report_format(self, report_format):
        self.report_format = report_format
        print(f"Report format set to: {self.report_format}")


    # --- Thread Management ---
    def start_thread(self, func, finished_slot, error_slot, *args, **kwargs):
        """Starts a worker thread and manages it."""
        # Clean up finished threads first
        self.active_threads = [t for t in self.active_threads if not t.isFinished()]

        thread = WorkerThread(func, *args, **kwargs)
        thread.finished.connect(finished_slot)
        thread.error.connect(error_slot)
        # Optional: Connect finished signal to remove thread from list
        thread.finished.connect(lambda: self.cleanup_thread(thread))
        thread.error.connect(lambda: self.cleanup_thread(thread)) # Also cleanup on error
        self.active_threads.append(thread)
        thread.start()
        return thread # Return thread if needed

    def cleanup_thread(self, thread):
         """Removes a thread from the active list."""
         if thread in self.active_threads:
             self.active_threads.remove(thread)
         # print(f"Thread finished or errored. Active threads: {len(self.active_threads)}") # Debug


    def handle_thread_error(self, error_message):
        """Displays errors from threads."""
        print(f"Error from worker thread:\n{error_message}") # Log to console
        self.status_label.setText("Error occurred during processing.")
        self.progress_bar.setVisible(False)
        QMessageBox.warning(self, "Processing Error", f"An error occurred:\n{error_message.splitlines()[0]}\n\nCheck console for details.")


    # --- File Handling ---
    def upload_log_file(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Select CSV Log File", "", "CSV Files (*.csv);;All Files (*.*)")
        if file_path:
            self.status_label.setText(f"Parsing CSV file: {os.path.basename(file_path)}...")
            self.progress_bar.setVisible(True)
            self.progress_bar.setRange(0, 0) # Indeterminate progress
            self.start_thread(self.log_parser.parse_log_file,
                              lambda df: self._process_log_file_result(file_path, df),
                              self.handle_thread_error,
                              file_path) # Pass file_path to thread function

    def _process_log_file_result(self, file_path, df):
        self.progress_bar.setVisible(False)
        filename = os.path.basename(file_path)
        if isinstance(df, pd.DataFrame) and not df.empty and "error" not in df.columns:
            self.log_data[filename] = df
            # Display info in tree view
            cols = list(df.columns)
            col_count = len(cols)
            # Truncate long column list for display
            col_display = ", ".join(cols[:5]) + ('...' if col_count > 5 else '')

            item = QTreeWidgetItem(self.file_tree, [filename, str(len(df)), str(col_count), "Parsed"])
            item.setToolTip(2, "\n".join(cols)) # Show full column list on hover

            self.status_label.setText(f"Parsed {filename}: {len(df)} rows, {col_count} columns")
        elif "error" in df.columns:
             self.status_label.setText(f"Error parsing {filename}: {df['error'].iloc[0]}")
             QMessageBox.warning(self, "Parsing Error", f"Could not parse {filename}:\n{df['error'].iloc[0]}")
             QTreeWidgetItem(self.file_tree, [filename, "N/A", "N/A", f"Error: {df['error'].iloc[0]}"])
        else:
             self.status_label.setText(f"Warning: No data or error parsing {filename}")
             QTreeWidgetItem(self.file_tree, [filename, "0", "0", "Empty/Failed"])


    def upload_log_directory(self):
        dir_path = QFileDialog.getExistingDirectory(self, "Select Directory with CSV Log Files")
        if dir_path:
            self.status_label.setText(f"Scanning directory for CSV files: {os.path.basename(dir_path)}...")
            self.progress_bar.setVisible(True)
            self.progress_bar.setRange(0, 0) # Indeterminate
            self.start_thread(self.log_parser.parse_log_directory,
                              self._process_log_directory_result,
                              self.handle_thread_error,
                              dir_path)

    def _process_log_directory_result(self, logs_data):
        self.progress_bar.setVisible(False)
        if isinstance(logs_data, dict) and "error" not in logs_data:
            parsed_count = 0
            for filename, df in logs_data.items():
                 # Check again for validity, although parser should pre-filter
                if isinstance(df, pd.DataFrame) and not df.empty and "error" not in df.columns:
                    self.log_data[filename] = df
                    cols = list(df.columns)
                    col_count = len(cols)
                    col_display = ", ".join(cols[:5]) + ('...' if col_count > 5 else '')

                    item = QTreeWidgetItem(self.file_tree, [filename, str(len(df)), str(col_count), "Parsed"])
                    item.setToolTip(2, "\n".join(cols)) # Show full column list on hover
                    parsed_count += 1
                else:
                     print(f"Skipping {filename} from directory result (empty or error).")
                     QTreeWidgetItem(self.file_tree, [filename, "N/A", "N/A", "Skipped/Empty"])

            self.status_label.setText(f"Parsed {parsed_count} CSV files from directory.")
            if parsed_count == 0:
                 QMessageBox.information(self, "No Files Parsed", "No valid CSV files were found or parsed in the selected directory.")

        elif isinstance(logs_data, dict) and "error" in logs_data:
             self.status_label.setText(f"Error scanning directory: {logs_data['error']}")
             QMessageBox.warning(self, "Directory Error", logs_data['error'])
        else:
            self.status_label.setText("Failed to process directory.")


    def clear_logs(self):
        # Confirmation dialog
        reply = QMessageBox.question(self, 'Clear Logs',
                                     "Are you sure you want to clear all loaded log data?",
                                     QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

        if reply == QMessageBox.Yes:
            self.log_data.clear()
            self.current_results = pd.DataFrame() # Reset to empty DataFrame
            self.current_summary = {}
            self.current_visualization = None
            self.file_tree.clear()
            self.summary_display.clear()
            self.results_tree.clear()
            self.results_tree.setHeaderLabels(["Results will appear here"]) # Reset header

             # Clear visualization panel
            # Properly remove the old canvas if it exists
            old_canvas = self.viz_canvas_frame.findChild(FigureCanvas)
            if old_canvas:
                 self.viz_canvas_layout.removeWidget(old_canvas)
                 old_canvas.deleteLater()

            self.status_label.setText("All logs cleared")
            print("Logs cleared.")


    # --- Query Processing ---
    def parse_query(self):
        query = self.query_edit.toPlainText().strip()
        if not query:
            self.status_label.setText("Please enter a query.")
            QMessageBox.information(self, "Input Needed", "Please enter a query in the text box.")
            return

        try:
            # Assuming NLP processor is robust enough not to need a thread
            params = self.nlp_processor.process_query(query)
            # Format parameters for display
            param_text = ""
            for key, value in params.items():
                if key == 'time_range' and isinstance(value, dict):
                     start = value.get('start', 'Any')
                     end = value.get('end', 'Any')
                     # Format datetime objects nicely
                     start_str = start.strftime('%Y-%m-%d %H:%M:%S %Z') if isinstance(start, datetime) else str(start)
                     end_str = end.strftime('%Y-%m-%d %H:%M:%S %Z') if isinstance(end, datetime) else str(end)
                     param_text += f"Time Range: {start_str} to {end_str}\n"
                elif key == 'original_query':
                     continue # Don't show the raw query here
                else:
                     param_text += f"{key.replace('_', ' ').title()}: {value or 'Any'}\n"

            self.params_display.setText(param_text.strip())
            self.status_label.setText("Query parsed successfully.")
        except Exception as e:
             self.status_label.setText("Error parsing query.")
             QMessageBox.critical(self, "NLP Error", f"Failed to parse query: {e}")
             print(f"NLP parsing error: {e}\n{traceback.format_exc()}")

    def run_query(self):
        query = self.query_edit.toPlainText().strip()
        params = self.nlp_processor.process_query(query) # Reparse to ensure latest params

        if not self.log_data:
            self.status_label.setText("No log data loaded. Please upload CSV logs first.")
            QMessageBox.warning(self, "No Data", "Please upload some CSV log files before running a query.")
            return

        self.status_label.setText("Applying filters and analyzing...")
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 0) # Indeterminate

        # --- MODIFIED LOGIC: Filter each DataFrame individually, then combine results ---
        # This function will run in the background thread
        def analyze_data_in_thread(log_data_dict, analyzer_instance, search_params):
            all_results = []
            for filename, df in log_data_dict.items():
                print(f"Analyzing {filename} ({len(df)} rows)...")
                try:
                    # Apply filters to this specific DataFrame
                    filtered_df = analyzer_instance.analyze(df, search_params)
                    if not filtered_df.empty:
                        all_results.append(filtered_df)
                        print(f"  -> Found {len(filtered_df)} matching rows in {filename}")
                    else:
                        print(f"  -> No matching rows in {filename}")
                except Exception as e:
                    print(f"Error analyzing {filename}: {e}\n{traceback.format_exc()}")
                    # Optionally, collect errors to show user

            if not all_results:
                 print("No results found across all files.")
                 return pd.DataFrame(), {} # Return empty DataFrame and empty summary

            # Concatenate only the filtered results
            print(f"Combining {len(all_results)} filtered DataFrame(s)...")
            combined_results = pd.concat(all_results, ignore_index=True)
            print(f"Total combined results: {len(combined_results)} rows")

            # Generate summary on the combined results
            print("Generating summary...")
            summary = analyzer_instance.generate_summary(combined_results, search_params)
            print("Analysis complete.")
            return combined_results, summary

        # Start the analysis in a thread
        self.start_thread(analyze_data_in_thread,
                          self._process_query_result,
                          self.handle_thread_error,
                          self.log_data, self.analyzer, params) # Pass data, analyzer, params


    def _process_query_result(self, result):
        self.progress_bar.setVisible(False)
        if result is None: # Handle potential None return from thread function on error
             self.status_label.setText("Query processing failed.")
             QMessageBox.critical(self, "Error", "Query processing failed. Check logs.")
             return

        results_df, summary_dict = result
        self.current_results = results_df # Store combined results
        self.current_summary = summary_dict

        if isinstance(results_df, pd.DataFrame) and not results_df.empty:
            self.status_label.setText(f"Query completed: {len(results_df)} results found.")
            self._update_results_display(results_df, summary_dict)
            self.tabs.setCurrentWidget(self.results_tab) # Switch to results tab
        elif isinstance(results_df, pd.DataFrame) and results_df.empty:
             self.status_label.setText("Query completed: No matching results found.")
             self._update_results_display(results_df, summary_dict) # Show empty state
             QMessageBox.information(self, "No Results", "Your query did not match any log entries.")
             self.tabs.setCurrentWidget(self.results_tab)
        else:
             self.status_label.setText("Query error or unexpected result type.")
             QMessageBox.warning(self, "Query Error", "An issue occurred during the query. Results might be incomplete.")
             # Clear previous results display?
             self.summary_display.clear()
             self.results_tree.clear()


    def _update_results_display(self, results_df, summary):
        # Display Summary
        summary_text = f"Total Matching Logs: {summary.get('total_logs', 'N/A')}\n"
        query = summary.get('query', 'N/A')
        # Truncate long queries for display
        summary_text += f"Query: {query[:100] + '...' if len(query) > 100 else query}\n"

        if 'earliest_log' in summary and pd.notna(summary['earliest_log']):
             time_format = '%Y-%m-%d %H:%M:%S %Z'
             start = summary['earliest_log'].strftime(time_format)
             end = summary['latest_log'].strftime(time_format)
             summary_text += f"Time Range of Results: {start} to {end}\n"
             summary_text += f"Time Span (Hours): {summary.get('time_span_hours', 'N/A'):.2f}\n"

        if summary.get('keywords'):
            summary_text += f"Keywords Searched: {', '.join(summary['keywords'])}\n"

        # Add distributions if available
        if "log_type_distribution" in summary:
            summary_text += "\nLog Type Distribution (Top 5):\n"
            # Sort by count desc, take top 5
            sorted_types = sorted(summary["log_type_distribution"].items(), key=lambda item: item[1], reverse=True)
            for k, v in sorted_types[:5]:
                summary_text += f"- {k}: {v}\n"
            if len(sorted_types) > 5: summary_text += "- ...\n"

        if "action_distribution" in summary:
             summary_text += "\nAction Distribution (Top 5):\n"
             sorted_actions = sorted(summary["action_distribution"].items(), key=lambda item: item[1], reverse=True)
             for k, v in sorted_actions[:5]:
                 summary_text += f"- {k}: {v}\n"
             if len(sorted_actions) > 5: summary_text += "- ...\n"


        if "top_ip_addresses" in summary:
            summary_text += "\nTop IP Addresses Mentioned (Top 5 per column):\n"
            for col, ips in summary["top_ip_addresses"].items():
                 summary_text += f"  Column '{col}':\n"
                 sorted_ips = sorted(ips.items(), key=lambda item: item[1], reverse=True)
                 for ip, count in sorted_ips[:5]:
                     summary_text += f"  - {ip}: {count}\n"
                 if len(sorted_ips) > 5: summary_text += "  - ...\n"


        self.summary_display.setText(summary_text.strip())

        # Display Results Sample in Tree
        self.results_tree.clear()
        if not results_df.empty:
            # Limit the number of rows displayed for performance
            display_limit = 1000
            results_sample = results_df.head(display_limit)

            # Use actual column names from the result DataFrame
            headers = list(results_sample.columns)
            self.results_tree.setHeaderLabels(headers)

            # Adjust column widths dynamically (simple example)
            for i, header in enumerate(headers):
                 # Basic width adjustment - refine as needed
                 width = max(100, len(header) * 10)
                 self.results_tree.setColumnWidth(i, min(width, 300)) # Min/Max width


            # Populate rows
            for _, row in results_sample.iterrows():
                 # Convert all values to string for display, handle NaNs/NaTs
                 row_values = [str(val) if pd.notna(val) else "" for val in row]
                 QTreeWidgetItem(self.results_tree, row_values)

            if len(results_df) > display_limit:
                 # Indicate that only a sample is shown (e.g., in status bar or results label)
                 results_label = self.results_tab.findChild(QLabel, "ResultsLabel") # Need to name the label
                 if results_label: results_label.setText(f"Result Records (Showing {display_limit} of {len(results_df)}):")
                 else: print(f"Note: Showing first {display_limit} results.") # Fallback
        else:
             self.results_tree.setHeaderLabels(["No results to display."])


    # --- Visualization ---
    def visualize_results(self):
        # Check if there are results to visualize
        if self.current_results is None or self.current_results.empty:
            self.status_label.setText("No results to visualize. Run a query first.")
            QMessageBox.information(self, "No Results", "There are no results to visualize. Please run a query first.")
            return

        self.tabs.setCurrentWidget(self.visualization_tab)
        # Automatically generate visualization based on current selection
        self.generate_visualization()


    def generate_visualization(self):
         # Check again before starting thread
        if self.current_results is None or self.current_results.empty:
             self.status_label.setText("Cannot visualize: No results available.")
             return

        # Determine viz type: Use selected type OR the one from NLP if 'auto'
        viz_type_to_generate = self.viz_type
        if viz_type_to_generate == "auto":
             # Fallback to NLP suggestion or a default like 'trend' or 'bar'
             viz_type_to_generate = self.current_summary.get("viz_type", "trend") # Default to trend if NLP gives none
             print(f"Auto visualization: Using type '{viz_type_to_generate}'")

        self.status_label.setText(f"Generating {viz_type_to_generate} visualization...")
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0,0) # Indeterminate


        self.start_thread(self.visualizer.generate_visualization,
                           self._display_visualization,
                           self.handle_thread_error,
                           self.current_results, self.current_summary, viz_type_to_generate)

    def _display_visualization(self, viz_data):
            self.progress_bar.setVisible(False)

            # --- Clear previous visualization ---
            # Find existing canvas widget within the layout/frame
            old_canvas = self.viz_canvas_frame.findChild(FigureCanvas)
            if old_canvas:
                self.viz_canvas_layout.removeWidget(old_canvas)
                old_canvas.deleteLater() # Ensure proper cleanup
            # Also clear any potential error messages/labels
            for i in reversed(range(self.viz_canvas_layout.count())):
                widget = self.viz_canvas_layout.itemAt(i).widget()
                if widget is not None:
                    widget.deleteLater()

            # --- Display new visualization ---
            # Check if viz_data is a dictionary and has 'type'
            if not isinstance(viz_data, dict) or 'type' not in viz_data:
                self.status_label.setText("Visualization failed: Invalid data format received.")
                error_label = QLabel("Failed to generate visualization (Invalid Format).")
                self.viz_canvas_layout.addWidget(error_label)
                print(f"Error: viz_data received is not a valid dictionary: {viz_data}")
                return

            viz_type = viz_data.get("type", "Unknown")
            viz_payload = viz_data.get("data") # Get the data payload

            if viz_type == "plotly" and viz_payload and isinstance(viz_payload, go.Figure): # Check type
                try:
                    fig = viz_payload # Assuming data is the Plotly figure object

                    # --- Using Matplotlib backend to embed Plotly figure ---
                    # Requires installing kaleido: pip install -U kaleido
                    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmpfile:
                        img_path = tmpfile.name
                        try:
                            fig.write_image(img_path, scale=2) # Increase scale for better resolution
                        except Exception as img_err:
                            print(f"Error exporting Plotly figure to image: {img_err}")
                            self.status_label.setText("Error rendering visualization.")
                            error_label = QLabel(f"Error rendering plot: {img_err}")
                            self.viz_canvas_layout.addWidget(error_label)
                            # Clean up temp file on error if it exists
                            if os.path.exists(img_path): 
                                try: os.unlink(img_path); 
                                except OSError: pass
                            return # Stop if image export fails

                    # Read the image back using Matplotlib
                    from matplotlib.image import imread
                    import matplotlib.pyplot as plt # Import pyplot

                    try:
                        img = imread(img_path)
                    finally:
                        # Clean up the temporary file
                        if os.path.exists(img_path):
                            try: os.unlink(img_path)
                            except OSError as e: print(f"Warning: Could not delete temp viz file {img_path}: {e}")


                    # Create a Matplotlib figure and axes to display the image
                    mpl_fig, ax = plt.subplots()
                    ax.imshow(img)
                    ax.axis('off') # Hide axes
                    plt.tight_layout(pad=0) # Remove padding

                    # Create Matplotlib canvas and add it
                    canvas = FigureCanvas(mpl_fig)
                    canvas.setMinimumSize(400, 300) # Ensure minimum size
                    self.viz_canvas_layout.addWidget(canvas)

                    # Store the *original Plotly figure* for potential export
                    self.current_visualization = {'type': 'plotly', 'figure': fig}
                    self.status_label.setText(f"Plotly visualization displayed.")

                except ImportError:
                    self.status_label.setText("Error: Missing 'kaleido' package for Plotly image export.")
                    error_label = QLabel("Error: Please install kaleido (`pip install -U kaleido`) to display Plotly charts.")
                    self.viz_canvas_layout.addWidget(error_label)
                except Exception as e:
                    self.status_label.setText("Error displaying visualization.")
                    error_label = QLabel(f"Could not display plot: {e}")
                    self.viz_canvas_layout.addWidget(error_label)
                    print(f"Visualization display error: {e}\n{traceback.format_exc()}")


            elif viz_type == "table":
                # Handle cases where 'data' might be a dict with 'message' OR a list of records
                message = "Visualization resulted in tabular data (not plotted)." # Default message
                if isinstance(viz_payload, dict) and "message" in viz_payload:
                    # Case 1: Visualizer explicitly returned a message (e.g., "No results")
                    message = viz_payload.get("message", message)
                elif isinstance(viz_payload, list):
                    # Case 2: Visualizer returned raw records (list of dicts)
                    # Provide more context if it's a list of records
                    record_count = len(viz_payload)
                    message = f"Visualization generated tabular data ({record_count} records)."
                    if record_count > 0:
                        message += " Use 'Export Results' to view full data."
                    else:
                        message = "Visualization query resulted in no tabular data."

                elif viz_payload is None:
                    message = "Visualization resulted in empty table data."


                label = QLabel(message)
                label.setAlignment(Qt.AlignCenter)
                label.setWordWrap(True) # Wrap long messages
                self.viz_canvas_layout.addWidget(label)
                self.status_label.setText("Visualization resulted in table (not plotted).")
                self.current_visualization = None # No plottable figure

            else:
                # Handle other types or errors (including plotly failure if viz_payload wasn't a Figure)
                error_msg = f"Unsupported or failed visualization type: {viz_type}"
                if isinstance(viz_payload, dict) and "message" in viz_payload:
                    error_msg = viz_payload.get("message", error_msg) # Use specific error message if available
                elif viz_payload is None and viz_type == "plotly":
                    error_msg = "Failed to generate Plotly figure data."

                label = QLabel(error_msg)
                label.setAlignment(Qt.AlignCenter)
                label.setWordWrap(True)
                self.viz_canvas_layout.addWidget(label)
                self.status_label.setText("Visualization failed or type not supported.")
                self.current_visualization = None


    def export_visualization(self):
        if not self.current_visualization or self.current_visualization['type'] != 'plotly':
            self.status_label.setText("No exportable (Plotly) visualization available.")
            QMessageBox.information(self, "Export Error", "No Plotly visualization is currently displayed to export.")
            return

        # Default filename suggestion
        default_filename = f"visualization_{datetime.now():%Y%m%d_%H%M%S}.png"
        file_path, _ = QFileDialog.getSaveFileName(self, "Save Visualization", default_filename, "PNG Images (*.png);;JPEG Images (*.jpg);;SVG Vector Images (*.svg);;PDF Document (*.pdf)")

        if file_path:
            try:
                 fig = self.current_visualization['figure']
                 # write_image handles different extensions based on file_path
                 fig.write_image(file_path, scale=3) # Use higher scale for export
                 self.status_label.setText(f"Visualization exported to {os.path.basename(file_path)}")
                 QMessageBox.information(self, "Export Successful", f"Visualization saved to:\n{file_path}")
            except ImportError:
                  QMessageBox.critical(self, "Export Error", "Missing 'kaleido' package.\nPlease install using: pip install -U kaleido")
                  self.status_label.setText("Export failed: Missing 'kaleido'.")
            except Exception as e:
                 self.status_label.setText(f"Export failed: {e}")
                 QMessageBox.critical(self, "Export Error", f"Could not save visualization:\n{e}")
                 print(f"Visualization export error: {e}\n{traceback.format_exc()}")


    def export_results(self):
        if self.current_results is None or self.current_results.empty:
            self.status_label.setText("No results to export.")
            QMessageBox.warning(self, "No Results", "There are no results to export. Please run a query first.")
            return

        # Default filename suggestion
        default_filename = f"results_{datetime.now():%Y%m%d_%H%M%S}.csv"
        # Offer CSV, Excel, JSON formats
        file_path, selected_filter = QFileDialog.getSaveFileName(self, "Export Results", default_filename,
                                                                "CSV (Comma delimited) (*.csv);;Excel Worksheet (*.xlsx);;JSON Lines (*.jsonl)")

        if file_path:
            self.status_label.setText(f"Exporting results to {os.path.basename(file_path)}...")
            self.progress_bar.setVisible(True)
            self.progress_bar.setRange(0,0)

            # Use a thread for potentially large file exports
            def export_in_thread(df, path, file_format):
                try:
                    file_format = file_format.lower()
                    if file_format == '.csv':
                        df.to_csv(path, index=False, encoding='utf-8')
                    elif file_format == '.xlsx':
                        # Requires 'openpyxl': pip install openpyxl
                        try:
                             df.to_excel(path, index=False, engine='openpyxl')
                        except ImportError:
                             return "Error: 'openpyxl' package required for Excel export. Install with 'pip install openpyxl'."
                    elif file_format == '.jsonl':
                         # JSON Lines format (one JSON object per line) - better for large data
                         df.to_json(path, orient='records', lines=True, date_format='iso')
                    else:
                        return f"Error: Unsupported export format '{file_format}'."
                    return f"Successfully exported {len(df)} rows to {os.path.basename(path)}"
                except Exception as e:
                    return f"Error exporting results: {e}"

            # Determine format from selected filter or file extension
            file_ext = os.path.splitext(file_path)[1].lower()
            if "(*.csv)" in selected_filter: file_ext = ".csv"
            elif "(*.xlsx)" in selected_filter: file_ext = ".xlsx"
            elif "(*.jsonl)" in selected_filter: file_ext = ".jsonl"

            # Ensure the filepath has the correct extension
            if not file_path.lower().endswith(file_ext):
                 file_path += file_ext


            self.start_thread(export_in_thread,
                               self._handle_export_result,
                               self.handle_thread_error,
                               self.current_results, file_path, file_ext)


    def _handle_export_result(self, message):
         self.progress_bar.setVisible(False)
         if message.startswith("Error"):
             self.status_label.setText("Export failed.")
             QMessageBox.warning(self, "Export Error", message)
         else:
             self.status_label.setText(message)
             QMessageBox.information(self, "Export Successful", message)


    # --- Report Generation ---
    def generate_report(self):
        # Check if results are available
        if self.current_results is None or self.current_summary is None:
            self.status_label.setText("No results available to generate a report.")
            QMessageBox.warning(self, "No Data", "Cannot generate report: No query results available.")
            return

        # Prepare data for the report generator
        report_data = {
            'title': self.report_title.text() or "Log Analysis Report",
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'query': self.current_summary.get('query') if self.include_query.isChecked() else None,
            'summary': self.current_summary if self.include_summary.isChecked() else None,
             # Include only a sample of results in the report for performance/size
            'results_sample': self.current_results.head(100) if self.include_results_sample.isChecked() and not self.current_results.empty else None,
            'visualization_path': None # Will be populated if viz is included
        }

        # Handle visualization inclusion (requires temporary image file)
        temp_viz_file = None
        if self.include_viz.isChecked() and self.current_visualization and self.current_visualization['type'] == 'plotly':
            try:
                # Create a temporary file that ReportLab can access
                with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp:
                    temp_viz_file = tmp.name
                    fig = self.current_visualization['figure']
                    # Export using kaleido
                    fig.write_image(temp_viz_file, scale=2) # Use good scale
                    report_data['visualization_path'] = temp_viz_file
                    print(f"Temporary visualization saved for report: {temp_viz_file}")

            except ImportError:
                 QMessageBox.warning(self, "Report Warning", "Cannot include visualization in report: 'kaleido' package missing. Install with 'pip install -U kaleido'.")
                 report_data['visualization_path'] = None # Ensure it's None if export fails
                 temp_viz_file = None # Ensure no lingering reference
            except Exception as e:
                 QMessageBox.warning(self, "Report Warning", f"Could not export visualization for report: {e}")
                 report_data['visualization_path'] = None
                 temp_viz_file = None # Ensure no lingering reference
                 print(f"Error saving visualization for report: {e}")


        # Ask user where to save the report
        report_format = self.report_format # pdf or html
        default_filename = f"{report_data['title'].replace(' ', '_')}_{datetime.now():%Y%m%d}.{report_format}"
        if report_format == 'pdf':
             filter_str = "PDF Document (*.pdf)"
        else:
             filter_str = "HTML Document (*.html)"

        save_path, _ = QFileDialog.getSaveFileName(self, f"Save {report_format.upper()} Report", default_filename, filter_str)

        if not save_path:
             # User cancelled - clean up temp viz file if created
             if temp_viz_file and os.path.exists(temp_viz_file):
                 try: os.unlink(temp_viz_file)
                 except OSError: pass
             self.status_label.setText("Report generation cancelled.")
             return


        # Ensure correct extension
        if not save_path.lower().endswith(f".{report_format}"):
             save_path += f".{report_format}"

        self.status_label.setText(f"Generating {report_format.upper()} report...")
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0,0)

        # --- Run report generation in thread ---
        def report_thread_func(generator, data, path, format, viz_temp_path):
            try:
                report_file_path = generator.generate_report(data, path, format) # Pass save path
                return report_file_path # Return the final path
            finally:
                 # --- IMPORTANT: Clean up the temporary viz file AFTER report is generated ---
                 if viz_temp_path and os.path.exists(viz_temp_path):
                     try:
                         os.unlink(viz_temp_path)
                         print(f"Cleaned up temp viz file: {viz_temp_path}")
                     except OSError as e:
                         print(f"Warning: Could not delete temp viz file {viz_temp_path}: {e}")

        self.start_thread(report_thread_func,
                           self._handle_report_generated,
                           self.handle_thread_error,
                           self.report_generator, report_data, save_path, report_format, temp_viz_file) # Pass generator, data, path, format, temp file path


    def _handle_report_generated(self, report_file_path):
        self.progress_bar.setVisible(False)
        if report_file_path and os.path.exists(report_file_path):
             self.status_label.setText(f"Report saved: {os.path.basename(report_file_path)}")
             # Ask user if they want to open the report
             reply = QMessageBox.question(self, 'Report Generated',
                                          f"Report saved successfully to:\n{report_file_path}\n\nDo you want to open it?",
                                          QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
             if reply == QMessageBox.Yes:
                  try:
                      # Use QDesktopServices for cross-platform opening
                      QDesktopServices.openUrl(QUrl.fromLocalFile(report_file_path))
                  except Exception as e:
                       QMessageBox.warning(self, "Open Error", f"Could not open report file automatically: {e}")
        else:
             self.status_label.setText("Report generation failed.")
             QMessageBox.critical(self, "Report Error", f"Failed to generate or save the report. Path: {report_file_path}")


    # --- Application Closing ---
    def closeEvent(self, event):
        """ Handles application close requests. """
        # Ensure all threads are properly handled (optional: wait or signal to stop)
        if self.active_threads:
             reply = QMessageBox.question(self, 'Exit Confirmation',
                                          f"{len(self.active_threads)} background task(s) might still be running. Exit anyway?",
                                          QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
             if reply == QMessageBox.No:
                  event.ignore() # Don't close
                  return

        # Proceed with closing
        print("Cleaning up and closing application...")
        # (Add any specific cleanup needed, e.g., stopping threads gracefully if required)
        event.accept() # Allow closing


# --- Main Execution ---
def main():
    # Set high DPI scaling attribute BEFORE creating QApplication
    # Adjust based on PyQt version if needed
    if hasattr(Qt, 'AA_EnableHighDpiScaling'):
        QApplication.setAttribute(Qt.AA_EnableHighDpiScaling, True)
    if hasattr(Qt, 'AA_UseHighDpiPixmaps'):
        QApplication.setAttribute(Qt.AA_UseHighDpiPixmaps, True)


    app = QApplication(sys.argv)

    # --- Import QtWidgets here after QApplication is created ---
    # This is sometimes necessary for certain Qt interactions or plugins
    from PyQt5 import QtWidgets

    try:
        window = EONParserGUI()
        window.show()
        sys.exit(app.exec_())
    except Exception as e:
        # Use QMessageBox for errors during startup if GUI context allows
        # Fallback to print for very early errors
        try:
             QMessageBox.critical(None, "Application Error", f"A critical error occurred on startup:\n{e}\n{traceback.format_exc()}")
        except:
             print(f"Main execution error: {str(e)}\n{traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    # Add QtWidgets import inside main function or here if needed globally after app creation
    from PyQt5 import QtWidgets
    main()