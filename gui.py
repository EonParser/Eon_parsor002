# gui.py
import sys
import traceback
from PyQt5.QtWidgets import (QApplication, QMainWindow, QTabWidget, QWidget, QVBoxLayout, QHBoxLayout,
                             QPushButton, QFileDialog, QTreeWidget, QTreeWidgetItem, QTextEdit, QLabel,
                             QRadioButton, QCheckBox, QLineEdit, QProgressBar, QFrame, QMessageBox, QGridLayout, QProgressDialog) # Added QMessageBox
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QUrl # Added QUrl
from PyQt5.QtGui import QDesktopServices # Added QDesktopServices
from PyQt5.QtWebEngineWidgets import QWebEngineView
import pandas as pd
import os
import gc
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
            "Show connection denies in the last 24 hours", 
            "Find all traffic from 192.168.1.100",
            "Show failed VPN authentication attempts",
            "Count events by severity level",
            "Find all ACL drops by source IP",
            "Show logs with message containing 'crypto'",
            "Visualize connection activity over time",
            "Find all NAT translations for internal network",
            "Show logs with high severity",
            "Count authentication failures by username"
        ]

        # And update the example_layout section to display more examples if needed:

        example_layout = QHBoxLayout()
        for example in examples[:4]:  # Show first 4 examples
            btn = QPushButton(example)
            btn.setStyleSheet("text-align:left; padding: 4px;")
            btn.clicked.connect(lambda _, e=example: self.query_edit.setText(e))
            example_layout.addWidget(btn)
        layout.addLayout(example_layout)

        # Add a second row of examples for better coverage
        example_layout2 = QHBoxLayout()
        for example in examples[4:8]:  # Show next 4 examples
            btn = QPushButton(example)
            btn.setStyleSheet("text-align:left; padding: 4px;")
            btn.clicked.connect(lambda _, e=example: self.query_edit.setText(e))
            example_layout2.addWidget(btn)
        layout.addLayout(example_layout2)


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


        export_btn = QPushButton("Export Visualization")
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
            viz_type_to_generate = self.current_summary.get("viz_type", "trend")
            print(f"Auto visualization: Using type '{viz_type_to_generate}'")

        # Enforce a maximum row limit to prevent memory issues
        max_rows = 20000  # Adjust as needed based on your system capabilities
        if len(self.current_results) > max_rows:
            # Create a notification of the limit
            QMessageBox.information(
                self, 
                "Large Dataset Notice", 
                f"Your dataset contains {len(self.current_results)} rows, which may cause performance issues. " 
                f"The visualization will be created using a sampled subset of {max_rows} rows."
            )

        self.status_label.setText(f"Generating {viz_type_to_generate} visualization...")
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 0)  # Indeterminate

        self.start_thread(
            self.visualizer.generate_visualization,
            self._display_visualization,
            self.handle_thread_error,
            self.current_results, 
            self.current_summary, 
            viz_type_to_generate
        )

    def _cleanup_visualization_resources(self):
        """Free memory resources used by visualizations"""
        # Clear previous visualization
        old_canvas = self.viz_canvas_frame.findChild(FigureCanvas)
        if old_canvas:
            self.viz_canvas_layout.removeWidget(old_canvas)
            old_canvas.deleteLater()
        
        # Clear any web views that might be using memory
        web_view = self.viz_canvas_frame.findChild(QWebEngineView)
        if web_view:
            self.viz_canvas_layout.removeWidget(web_view)
            web_view.deleteLater()
        
        # Clear any other widgets in the layout
        for i in reversed(range(self.viz_canvas_layout.count())):
            widget = self.viz_canvas_layout.itemAt(i).widget()
            if widget is not None:
                self.viz_canvas_layout.removeWidget(widget)
                widget.deleteLater()
        
        # Reset current visualization
        if hasattr(self, 'current_visualization') and self.current_visualization:
            self.current_visualization = None
        
        # Force garbage collection
        gc.collect()

    def _display_visualization(self, viz_data):
        self.progress_bar.setVisible(False)

        # --- Clear previous visualization ---
        # Find existing widgets within the layout/frame and remove them
        for i in reversed(range(self.viz_canvas_layout.count())):
            widget = self.viz_canvas_layout.itemAt(i).widget()
            if widget is not None:
                self.viz_canvas_layout.removeWidget(widget)
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
        viz_payload = viz_data.get("data")  # Get the data payload

        if viz_type == "plotly" and viz_payload and isinstance(viz_payload, go.Figure):  # Check type
            try:
                # Create a QWebEngineView to display the Plotly figure
                web_view = QWebEngineView()
                web_view.setMinimumSize(600, 400)
                
                # Convert Plotly figure to HTML and load it into the web view
                html = viz_payload.to_html(include_plotlyjs='cdn')
                web_view.setHtml(html)
                
                # Add the web view to the layout
                self.viz_canvas_layout.addWidget(web_view)
                
                # Store the Plotly figure for potential export
                self.current_visualization = {'type': 'plotly', 'figure': viz_payload}
                self.status_label.setText(f"Plotly visualization displayed.")
                
            except Exception as e:
                self.status_label.setText("Error displaying visualization.")
                error_label = QLabel(f"Could not display plot: {e}")
                self.viz_canvas_layout.addWidget(error_label)
                print(f"Visualization display error: {e}\n{traceback.format_exc()}")

        elif viz_type == "table":
            # Handle tabular data (same as original)
            message = "Visualization resulted in tabular data (not plotted)."
            if isinstance(viz_payload, dict) and "message" in viz_payload:
                message = viz_payload.get("message", message)
            elif isinstance(viz_payload, list):
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
            label.setWordWrap(True)
            self.viz_canvas_layout.addWidget(label)
            self.status_label.setText("Visualization resulted in table (not plotted).")
            self.current_visualization = None

        else:
            # Handle other types or errors
            error_msg = f"Unsupported or failed visualization type: {viz_type}"
            if isinstance(viz_payload, dict) and "message" in viz_payload:
                error_msg = viz_payload.get("message", error_msg)
            elif viz_payload is None and viz_type == "plotly":
                error_msg = "Failed to generate Plotly figure data."

            label = QLabel(error_msg)
            label.setAlignment(Qt.AlignCenter)
            label.setWordWrap(True)
            self.viz_canvas_layout.addWidget(label)
            self.status_label.setText("Visualization failed or type not supported.")
            self.current_visualization = None

    def export_visualization(self):
        """Diagnostic version with better debugging and direct fallback"""
        if not self.current_visualization or self.current_visualization['type'] != 'plotly':
            self.status_label.setText("No exportable visualization available.")
            QMessageBox.information(self, "Export Error", "No Plotly visualization is currently displayed to export.")
            return

        # Default filename suggestion
        default_filename = f"visualization_{datetime.now():%Y%m%d_%H%M%S}"
        file_path, selected_filter = QFileDialog.getSaveFileName(
            self, "Save Visualization", 
            default_filename, 
            "HTML Document (*.html);;PNG Images (*.png);;JPEG Images (*.jpg)"
        )

        if not file_path:
            return  # User canceled the dialog
        
        # Extract chosen format from selected filter
        is_html = "HTML Document" in selected_filter
        
        # Make sure file has correct extension
        file_ext = ".html" if is_html else ".png"
        if not file_path.lower().endswith(file_ext):
            file_path += file_ext
        
        self.status_label.setText(f"Preparing {os.path.basename(file_path)}...")
        
        # Prepare progress dialog for better user feedback
        progress = QProgressDialog("Exporting visualization...", "Cancel", 0, 100, self)
        progress.setWindowTitle("Export Progress")
        progress.setWindowModality(Qt.WindowModal)
        progress.setMinimumDuration(0)  # Show immediately
        progress.setValue(10)
        QApplication.processEvents()  # Force update of UI
        
        try:
            # Get the figure object
            fig = self.current_visualization['figure']
            
            # Use the simplest possible approach first - HTML export (most reliable)
            if is_html:
                progress.setValue(30)
                progress.setLabelText("Generating HTML...")
                QApplication.processEvents()
                
                try:
                    # Direct HTML export without thread (should be fast enough)
                    fig.write_html(file_path, include_plotlyjs='cdn')
                    progress.setValue(100)
                    self.status_label.setText(f"Visualization saved to {os.path.basename(file_path)}")
                    QMessageBox.information(self, "Export Successful", 
                                        f"Visualization saved successfully to:\n{file_path}")
                    return
                except Exception as e:
                    self.status_label.setText(f"HTML export failed: {str(e)}")
                    QMessageBox.critical(self, "Export Error", 
                                        f"Failed to save HTML: {str(e)}\n\n{traceback.format_exc()}")
                    return
            
            # For image export - try direct approach first
            progress.setValue(30)
            progress.setLabelText("Checking Kaleido installation...")
            QApplication.processEvents()
            
            # Check if kaleido is available
            try:
                import kaleido
                kaleido_version = getattr(kaleido, "__version__", "unknown")
                print(f"Kaleido version: {kaleido_version}")
            except ImportError:
                QMessageBox.critical(self, "Export Error", 
                                "Kaleido package is not installed. Cannot export to image format.\n\n"
                                "Please install using: pip install -U kaleido\n\n"
                                "Would you like to save as HTML instead?")
                return
            
            progress.setValue(50)
            progress.setLabelText("Generating image file...")
            QApplication.processEvents()
            
            # Try exporting with the byte array approach
            try:
                img_bytes = None
                if file_path.lower().endswith('.png'):
                    img_bytes = fig.to_image(format='png', scale=2)
                elif file_path.lower().endswith('.jpg'):
                    img_bytes = fig.to_image(format='jpg', scale=2)
                
                if img_bytes:
                    progress.setValue(80)
                    progress.setLabelText("Writing file...")
                    QApplication.processEvents()
                    
                    # Write bytes to file
                    with open(file_path, 'wb') as f:
                        f.write(img_bytes)
                    
                    progress.setValue(100)
                    self.status_label.setText(f"Visualization saved to {os.path.basename(file_path)}")
                    QMessageBox.information(self, "Export Successful", 
                                        f"Visualization saved successfully to:\n{file_path}")
                    return
            except Exception as e:
                print(f"Image export error: {str(e)}\n{traceback.format_exc()}")
                
                # Offer HTML as fallback
                reply = QMessageBox.question(
                    self, "Export Error",
                    f"Failed to export as image: {str(e)}\n\nWould you like to save as HTML instead?",
                    QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes
                )
                
                if reply == QMessageBox.Yes:
                    # Save as HTML
                    html_path = os.path.splitext(file_path)[0] + ".html"
                    try:
                        fig.write_html(html_path, include_plotlyjs='cdn')
                        self.status_label.setText(f"HTML visualization saved to {os.path.basename(html_path)}")
                        QMessageBox.information(self, "HTML Export Successful", 
                                            f"Visualization saved as HTML to:\n{html_path}")
                    except Exception as html_err:
                        self.status_label.setText(f"HTML export failed: {str(html_err)}")
                        QMessageBox.critical(self, "Export Error", 
                                            f"Failed to save HTML: {str(html_err)}")
                
        except Exception as e:
            self.status_label.setText(f"Export failed: {str(e)}")
            QMessageBox.critical(self, "Export Error", f"Export failed: {str(e)}\n\n{traceback.format_exc()}")
        finally:
            progress.close()

    def _handle_export_visualization_result(self, message):
        """Handle the result from the export_visualization thread"""
        self.progress_bar.setVisible(False)
        
        if message.startswith("ERROR:"):
            error_msg = message[6:].strip()  # Remove "ERROR: " prefix
            self.status_label.setText(f"Export failed: {error_msg}")
            QMessageBox.critical(self, "Export Error", error_msg)
        else:
            self.status_label.setText(message)
            QMessageBox.information(self, "Export Successful", f"{message}\n\nThe file has been saved successfully.")


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
        """Report generation with safe visualization handling for PDF"""
        # Check if results are available
        if self.current_results is None or self.current_summary is None:
            self.status_label.setText("No results available to generate a report.")
            QMessageBox.warning(self, "No Data", "Cannot generate report: No query results available.")
            return

        # Prepare base report data (without visualization yet)
        report_data = {
            'title': self.report_title.text() or "Log Analysis Report",
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'query': self.current_summary.get('query') if self.include_query.isChecked() else None,
            'summary': self.current_summary if self.include_summary.isChecked() else None,
            'results_sample': self.current_results.head(100) if self.include_results_sample.isChecked() and not self.current_results.empty else None,
            'visualization_path': None  # Will be populated only for HTML reports or if export succeeds
        }

        # Get report format preference
        report_format = self.report_format  # pdf or html
        
        # For PDF, check if we should include visualization based on kaleido availability
        skip_viz_for_pdf = False
        if report_format == 'pdf' and self.include_viz.isChecked():
            try:
                import kaleido
                print(f"Kaleido version: {getattr(kaleido, '__version__', 'unknown')} found for PDF reports")
            except ImportError:
                print("Kaleido not installed, visualizations will be skipped for PDF reports")
                skip_viz_for_pdf = True
                QMessageBox.warning(self, "Limited PDF Capabilities", 
                                "The Kaleido package is not installed, so visualizations will be skipped in PDF reports.\n\n"
                                "HTML reports will still include interactive visualizations.\n\n"
                                "To enable visualizations in PDF, install Kaleido with:\npip install kaleido")
        
        # Ask user where to save the report
        if report_format == 'pdf':
            filter_str = "PDF Document (*.pdf);;HTML Document (*.html)"
        else:
            filter_str = "HTML Document (*.html)"
            
        default_filename = f"{report_data['title'].replace(' ', '_')}_{datetime.now():%Y%m%d}.{report_format}"
        save_path, selected_filter = QFileDialog.getSaveFileName(self, f"Save Report", default_filename, filter_str)

        if not save_path:
            self.status_label.setText("Report generation cancelled.")
            return

        # Update format if user changed the selection
        if "HTML Document" in selected_filter:
            report_format = "html"
        elif "PDF Document" in selected_filter:
            report_format = "pdf"
        
        # Ensure correct extension
        if not save_path.lower().endswith(f".{report_format}"):
            save_path += f".{report_format}"

        self.status_label.setText(f"Generating {report_format.upper()} report...")
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(10)

        # --- Handle visualization ONLY for HTML reports (safer approach) ---
        # For PDF reports, we'll skip visualization image creation since it's causing issues
        temp_viz_file = None
        if self.include_viz.isChecked() and self.current_visualization and self.current_visualization['type'] == 'plotly':
            if report_format == 'html':
                # For HTML, no need to create temp file, we'll embed directly
                pass
            elif not skip_viz_for_pdf:
                # For PDF, try to create image but with timeout and careful error handling
                self.status_label.setText("Preparing visualization for PDF report...")
                self.progress_bar.setValue(20)
                try:
                    # For PDF, we need a temporary image file - using the safest approach
                    with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp:
                        temp_viz_file = tmp.name
                    
                    # Only use the direct bytes approach which is most reliable
                    try:
                        fig = self.current_visualization['figure']
                        # Use a simplified version of the figure if possible
                        if hasattr(fig, 'update_layout'):
                            fig.update_layout(
                                width=800, 
                                height=500,
                                font=dict(size=10)
                            )
                        
                        # Try to get image bytes with a small timeout
                        img_bytes = fig.to_image(format='png', scale=1.5)
                        with open(temp_viz_file, 'wb') as f:
                            f.write(img_bytes)
                        report_data['visualization_path'] = temp_viz_file
                        print(f"Visualization saved for PDF report: {temp_viz_file}")
                    except Exception as e:
                        print(f"Could not save visualization for PDF report: {e}")
                        if temp_viz_file and os.path.exists(temp_viz_file):
                            try:
                                os.unlink(temp_viz_file)
                            except:
                                pass
                        temp_viz_file = None
                except Exception as e:
                    print(f"Error preparing visualization: {e}")
                    # Just continue without visualization
                    if temp_viz_file and os.path.exists(temp_viz_file):
                        try:
                            os.unlink(temp_viz_file)
                        except:
                            pass
                    temp_viz_file = None
        
        # --- Generate the report ---
        self.progress_bar.setValue(30)
        try:
            # Generate HTML report directly (more reliable)
            if report_format == 'html':
                self.status_label.setText("Generating HTML report...")
                self.progress_bar.setValue(50)
                
                # Generate HTML content using our custom method
                html_content = self._generate_simple_html_report(report_data)
                
                self.progress_bar.setValue(80)
                # Write HTML file
                with open(save_path, 'w', encoding='utf-8') as f:
                    f.write(html_content)
                
                success = True
                report_file_path = save_path
            
            # For PDF, use the report generator class but with careful error handling
            else:
                self.status_label.setText("Generating PDF report...")
                self.progress_bar.setValue(50)
                
                try:
                    # Use the report generator with a timeout or careful monitoring
                    report_file_path = self.report_generator.generate_report(report_data, save_path, report_format)
                    success = report_file_path and os.path.exists(report_file_path)
                except Exception as pdf_error:
                    print(f"PDF generation error: {pdf_error}")
                    
                    # Ask if user wants HTML instead
                    reply = QMessageBox.question(
                        self, "PDF Generation Failed",
                        f"Could not generate PDF report: {str(pdf_error)}\n\nWould you like to create an HTML report instead?",
                        QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes
                    )
                    
                    if reply == QMessageBox.Yes:
                        # Generate HTML instead
                        html_path = os.path.splitext(save_path)[0] + ".html"
                        html_content = self._generate_simple_html_report(report_data)
                        with open(html_path, 'w', encoding='utf-8') as f:
                            f.write(html_content)
                        success = True
                        report_file_path = html_path
                    else:
                        success = False
                        report_file_path = None
            
            # Clean up the temporary visualization file if it exists
            if temp_viz_file and os.path.exists(temp_viz_file):
                try:
                    os.unlink(temp_viz_file)
                except Exception as e:
                    print(f"Warning: Could not delete temp viz file {temp_viz_file}: {e}")
            
            # Handle success or failure
            if success and report_file_path and os.path.exists(report_file_path):
                self.progress_bar.setValue(100)
                self.status_label.setText(f"Report saved to {os.path.basename(report_file_path)}")
                
                # Ask to open the file
                reply = QMessageBox.question(
                    self, "Report Generated",
                    f"Report saved successfully to:\n{report_file_path}\n\nDo you want to open it?",
                    QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes
                )
                
                if reply == QMessageBox.Yes:
                    try:
                        QDesktopServices.openUrl(QUrl.fromLocalFile(report_file_path))
                    except Exception as e:
                        QMessageBox.warning(self, "Open Error", f"Could not open report file automatically: {e}")
            else:
                self.status_label.setText("Report generation failed.")
                QMessageBox.critical(self, "Report Error", "Failed to generate the report.")
        
        except Exception as e:
            print(f"Report generation error: {e}")
            self.status_label.setText(f"Report generation failed: {str(e)}")
            QMessageBox.critical(self, "Report Error", f"Failed to generate report: {str(e)}")
        
        finally:
            self.progress_bar.setVisible(False)
            # Ensure cleanup of temp file
            if temp_viz_file and os.path.exists(temp_viz_file):
                try:
                    os.unlink(temp_viz_file)
                except:
                    pass
            
    def _generate_simple_html_report(self, report_data):
        """Generate HTML report content directly (not using ReportGenerator)"""
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
            </style>
        </head>
        <body>
            <h1>{report_data.get('title', 'Log Analysis Report')}</h1>
            <p style="text-align: center;">Generated: {report_data.get('timestamp', 'N/A')}</p>
        """

        # Query section
        query = report_data.get('query')
        if query:
            html += f"<h2>Query</h2><div class='query'><p class='code'>{query.replace('<', '&lt;').replace('>', '&gt;')}</p></div>"

        # Summary section
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

            # Distributions
            if "log_type_distribution" in summary:
                html += "<p><strong>Log Type Distribution:</strong></p><ul>"
                sorted_types = sorted(summary["log_type_distribution"].items(), key=lambda item: item[1], reverse=True)
                for k, v in sorted_types[:10]: 
                    html += f"<li>{k}: {v}</li>"
                if len(sorted_types) > 10: 
                    html += "<li>...</li>"
                html += "</ul>"

            html += "</div>"  # End summary div

        # Visualization section
        if self.current_visualization and self.current_visualization['type'] == 'plotly' and self.include_viz.isChecked():
            html += "<h2>Visualization</h2>"
            try:
                # Create a direct HTML representation of the Plotly figure
                fig = self.current_visualization['figure']
                viz_html = fig.to_html(include_plotlyjs='cdn', full_html=False)
                html += f"<div>{viz_html}</div>"
            except Exception as e:
                html += f"<p><em>Error embedding visualization: {e}</em></p>"

        # Results Sample section
        results_df = report_data.get('results_sample')
        if results_df is not None and not results_df.empty:
            html += f"<h2>Results Sample (First {len(results_df)} Records)</h2>"
            # Convert DataFrame to HTML table, escape content
            html += results_df.to_html(index=False, escape=True, border=0)

        # Footer
        html += """
            <div class='footer'>
                <p>Generated by EONParser</p>
            </div>
        </body>
        </html>
        """
        
        return html

    def _handle_report_generated(self, result):
        """Handle the result from the report generation thread"""
        self.progress_bar.setVisible(False)
        
        if result.get("success", False):
            report_file_path = result.get("path")
            if report_file_path and os.path.exists(report_file_path):
                self.status_label.setText(f"Report saved: {os.path.basename(report_file_path)}")
                # Ask user if they want to open the report
                reply = QMessageBox.question(
                    self, 
                    'Report Generated',
                    f"Report saved successfully to:\n{report_file_path}\n\nDo you want to open it?",
                    QMessageBox.Yes | QMessageBox.No, 
                    QMessageBox.Yes
                )
                if reply == QMessageBox.Yes:
                    try:
                        # Use QDesktopServices for cross-platform opening
                        QDesktopServices.openUrl(QUrl.fromLocalFile(report_file_path))
                    except Exception as e:
                        QMessageBox.warning(self, "Open Error", f"Could not open report file automatically: {e}")
            else:
                self.status_label.setText("Report may have been generated but cannot be found.")
                QMessageBox.warning(self, "Report Warning", f"Report was processed but the file cannot be found at: {report_file_path}")
        else:
            error_msg = result.get("error", "Unknown error")
            self.status_label.setText("Report generation failed.")
            QMessageBox.critical(self, "Report Error", f"Failed to generate the report: {error_msg}")


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