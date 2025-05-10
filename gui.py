# gui.py

import sys
import traceback
from PyQt5.QtWidgets import (QApplication, QMainWindow, QTabWidget, QWidget, QVBoxLayout, QHBoxLayout,
                             QPushButton, QFileDialog, QTreeWidget, QTreeWidgetItem, QTextEdit, QLabel,
                             QProgressBar, QFrame, QMessageBox, QGridLayout, 
                             QProgressDialog, QSplitter, QHeaderView, QComboBox, QLineEdit, QCheckBox, QButtonGroup, QRadioButton)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QUrl, QSize
from PyQt5.QtGui import QDesktopServices, QIcon, QFont
from PyQt5.QtWebEngineWidgets import QWebEngineView
import pandas as pd
import os
import gc
import numpy as np
from datetime import datetime
import plotly.graph_objs as go
import tempfile
import webbrowser

# --- Local Imports ---
from log_parser import LogParser
from analyzer import LogAnalyzer
from log_visualizer import LogVisualizer
from report_generator import ReportGenerator
from search_filter_ui import SearchFilterPanel  # Import our new search filter UI

# --- Worker Thread ---
class WorkerThread(QThread):
    finished = pyqtSignal(object)
    error = pyqtSignal(str)

    def __init__(self, func, *args, **kwargs):
        super().__init__()
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def run(self):
        try:
            result = self.func(*self.args, **self.kwargs)
            self.finished.emit(result)
        except Exception as e:
            self.error.emit(f"Thread error: {str(e)}\n{traceback.format_exc()}")

# --- Main GUI Class ---
class EONParserGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        try:
            self.setWindowTitle("EONParser - Advanced Log Analysis Tool")
            self.setGeometry(100, 100, 1280, 800)
            self.setMinimumSize(1000, 700)

            # Initialize components
            self.log_parser = LogParser()
            self.analyzer = LogAnalyzer()
            self.visualizer = LogVisualizer()
            self.report_generator = ReportGenerator()

            # Stores loaded DataFrames (key: filename, value: DataFrame)
            self.log_data = {}
            self.current_results = pd.DataFrame()
            self.current_summary = {}
            self.current_visualization = None
            self.active_threads = []

            self.setup_ui()
            self.set_stylesheet()
        except Exception as e:
            QMessageBox.critical(self, "Initialization Error", f"Failed to initialize: {e}\n{traceback.format_exc()}")
            sys.exit(1)

    def set_stylesheet(self):
        """Apply styling to improve the UI appearance"""
        self.setStyleSheet("""
            QMainWindow { background-color: #f5f5f5; }
            QTabWidget::pane { border: 1px solid #ddd; background-color: #fff; }
            QTabBar::tab {
                background-color: #e6e6e6;
                border: 1px solid #ccc;
                padding: 8px 12px;
                min-width: 100px;
            }
            QTabBar::tab:selected {
                background-color: #fff;
                border-bottom-color: #fff;
                font-weight: bold;
            }
            QGroupBox {
                font-weight: bold;
                border: 1px solid #ddd;
                border-radius: 5px;
                margin-top: 10px;
                padding-top: 10px;
            }
            QPushButton {
                background-color: #f0f0f0;
                border: 1px solid #ccc;
                border-radius: 3px;
                padding: 5px 10px;
            }
            QPushButton:hover {
                background-color: #e0e0e0;
                border-color: #bbb;
            }
            QTreeWidget, QTextEdit {
                border: 1px solid #ddd;
                background-color: white;
            }
            QProgressBar {
                border: 1px solid #ccc;
                border-radius: 3px;
                text-align: center;
            }
            QProgressBar::chunk {
                background-color: #4CAF50;
            }
            QLabel[heading="true"] {
                font-size: 14px;
                font-weight: bold;
                color: #333;
                margin: 5px 0;
                padding: 5px 0;
            }
        """)

    def setup_ui(self):
        # Main widget and layout
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        main_layout = QVBoxLayout(main_widget)

        # Create tab widget
        self.tabs = QTabWidget()
        self.tabs.setDocumentMode(True)  # Cleaner look
        main_layout.addWidget(self.tabs)

        # Create tabs
        self.upload_tab = QWidget()
        self.search_tab = QWidget()  # Renamed from query_tab
        self.results_tab = QWidget()
        self.visualization_tab = QWidget()
        self.report_tab = QWidget()

        # Add tabs to tab widget
        self.tabs.addTab(self.upload_tab, "Upload Logs")
        self.tabs.addTab(self.search_tab, "Search")  # Renamed from "Query"
        self.tabs.addTab(self.results_tab, "Results")
        self.tabs.addTab(self.visualization_tab, "Visualization")
        self.tabs.addTab(self.report_tab, "Report")
        
        # Set up tab contents
        self.setup_upload_tab()
        self.setup_search_tab()  # Renamed from setup_query_tab
        self.setup_results_tab()
        self.setup_visualization_tab()
        self.setup_report_tab()

        # Status Bar
        self.status_label = QLabel("Ready")
        self.progress_bar = QProgressBar()
        self.progress_bar.setMaximum(100)
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(False)
        status_layout = QHBoxLayout()
        status_layout.addWidget(self.status_label, 1)  # Give label more stretch space
        status_layout.addWidget(self.progress_bar)
        main_layout.addLayout(status_layout)

    def setup_upload_tab(self):
        layout = QVBoxLayout(self.upload_tab)
        
        # Header with instructions
        header = QLabel("Upload Log Files")
        header.setProperty("heading", "true")
        header.setAlignment(Qt.AlignCenter)
        layout.addWidget(header)
        
        # Upload buttons
        btn_layout = QHBoxLayout()
        
        upload_file_btn = QPushButton("Upload Log File")
        upload_file_btn.setIcon(QIcon.fromTheme("document-open", QIcon()))
        upload_file_btn.setIconSize(QSize(16, 16))
        
        upload_dir_btn = QPushButton("Upload Directory")
        upload_dir_btn.setIcon(QIcon.fromTheme("folder-open", QIcon()))
        upload_dir_btn.setIconSize(QSize(16, 16))
        
        clear_btn = QPushButton("Clear All Logs")
        clear_btn.setIcon(QIcon.fromTheme("edit-clear", QIcon()))
        clear_btn.setIconSize(QSize(16, 16))
        
        upload_file_btn.clicked.connect(self.upload_log_file)
        upload_dir_btn.clicked.connect(self.upload_log_directory)
        clear_btn.clicked.connect(self.clear_logs)
        
        btn_layout.addWidget(upload_file_btn)
        btn_layout.addWidget(upload_dir_btn)
        btn_layout.addStretch()
        btn_layout.addWidget(clear_btn)
        layout.addLayout(btn_layout)

        # File list with improved layout
        self.file_tree = QTreeWidget()
        self.file_tree.setHeaderLabels(["Filename", "Rows", "Columns", "Status"])
        self.file_tree.setAlternatingRowColors(True)
        self.file_tree.header().setSectionResizeMode(0, QHeaderView.Stretch)
        self.file_tree.header().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.file_tree.header().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.file_tree.header().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        layout.addWidget(self.file_tree)
        
        # Help text
        help_text = QLabel("Upload individual log files (.csv, .log) or directories containing log files.")
        help_text.setAlignment(Qt.AlignCenter)
        help_text.setStyleSheet("color: #666; margin-top: 10px;")
        layout.addWidget(help_text)

    def setup_search_tab(self):
        """Set up the search tab with filter panel (replacing query tab)"""
        layout = QVBoxLayout(self.search_tab)
        
        # Header
        header = QLabel("Search Logs")
        header.setProperty("heading", "true")
        header.setAlignment(Qt.AlignCenter)
        layout.addWidget(header)
        
        # Create filter panel
        self.search_filter_panel = SearchFilterPanel()
        layout.addWidget(self.search_filter_panel)
        
        # Connect signals
        self.search_filter_panel.searchRequested.connect(self.run_search)  # Renamed from run_query
        self.search_filter_panel.resetRequested.connect(self.reset_search)  # New method
    
    def reset_search(self):
        """Reset search results when filters are reset"""
        self.current_results = pd.DataFrame()
        self.current_summary = {}
        self.summary_display.clear()
        self.results_tree.clear()
        self.status_label.setText("Search reset. Ready for new search.")

    def setup_results_tab(self):
        layout = QVBoxLayout(self.results_tab)
        
        # Header
        header = QLabel("Analysis Results")
        header.setProperty("heading", "true")
        header.setAlignment(Qt.AlignCenter)
        layout.addWidget(header)

        # Use QSplitter for adjustable summary/results view
        splitter = QSplitter(Qt.Vertical)

        # Summary section
        summary_frame = QFrame()
        summary_frame.setFrameShape(QFrame.StyledPanel)
        summary_layout = QVBoxLayout(summary_frame)
        summary_layout.setContentsMargins(10, 10, 10, 10)
        
        summary_header = QLabel("Summary:")
        summary_header.setStyleSheet("font-weight: bold;")
        summary_layout.addWidget(summary_header)
        
        self.summary_display = QTextEdit()
        self.summary_display.setReadOnly(True)
        self.summary_display.setStyleSheet("background-color: #f9f9f9; font-family: monospace;")
        summary_layout.addWidget(self.summary_display)
        splitter.addWidget(summary_frame)

        # Results section
        results_frame = QFrame()
        results_frame.setFrameShape(QFrame.StyledPanel)
        results_layout = QVBoxLayout(results_frame)
        results_layout.setContentsMargins(10, 10, 10, 10)
        
        self.results_label = QLabel("Result Records (Sample):")
        self.results_label.setStyleSheet("font-weight: bold;")
        results_layout.addWidget(self.results_label)
        
        self.results_tree = QTreeWidget()
        self.results_tree.setStyleSheet("QTreeWidget::item { height: 25px; }")
        self.results_tree.setAlternatingRowColors(True)
        results_layout.addWidget(self.results_tree)
        splitter.addWidget(results_frame)

        # Adjust initial sizes (30% summary, 70% results)
        splitter.setSizes([300, 700])
        layout.addWidget(splitter)

        # Action buttons
        btn_layout = QHBoxLayout()
        
        visualize_btn = QPushButton("Visualize Results")
        visualize_btn.setIcon(QIcon.fromTheme("image", QIcon()))
        
        export_btn = QPushButton("Export Results")
        export_btn.setIcon(QIcon.fromTheme("document-save", QIcon()))
        
        save_filter_btn = QPushButton("Save This Filter")
        save_filter_btn.setIcon(QIcon.fromTheme("bookmark-new", QIcon()))
        
        visualize_btn.clicked.connect(self.visualize_results)
        export_btn.clicked.connect(self.export_results)
        save_filter_btn.clicked.connect(self.save_current_filter)
        
        btn_layout.addWidget(visualize_btn)
        btn_layout.addWidget(save_filter_btn)
        btn_layout.addStretch()
        btn_layout.addWidget(export_btn)
        layout.addLayout(btn_layout)

    def save_current_filter(self):
        """Save the current filter configuration for future use"""
        if not hasattr(self, 'saved_filters'):
            self.saved_filters = []
            
        # Get current filter parameters
        if hasattr(self, 'current_search_params'):
            filter_name, ok = QInputDialog.getText(self, "Save Filter", "Enter a name for this filter:")
            if ok and filter_name:
                # Add the name to the params
                save_params = dict(self.current_search_params)
                save_params['name'] = filter_name
                
                # Add to saved filters
                self.saved_filters.append(save_params)
                
                # Update any UI elements that might display saved filters
                # This depends on your implementation - for example:
                # self.update_saved_filters_menu()
                
                QMessageBox.information(self, "Filter Saved", f"Filter '{filter_name}' has been saved.")
        else:
            QMessageBox.warning(self, "No Filter to Save", "Please run a search first to save its filter.")

    def setup_visualization_tab(self):
        layout = QVBoxLayout(self.visualization_tab)
        
        # Header
        header = QLabel("Visualization")
        header.setProperty("heading", "true")
        header.setAlignment(Qt.AlignCenter)
        layout.addWidget(header)

        # Options frame
        options_frame = QFrame()
        options_frame.setFrameShape(QFrame.StyledPanel)
        options_layout = QHBoxLayout(options_frame)
        
        options_layout.addWidget(QLabel("Chart Type:"))

        self.viz_type_combo = QComboBox()
        for viz_type in ["Auto", "Trend", "Pie", "Bar", "Heatmap", "Summary"]:
            self.viz_type_combo.addItem(viz_type, viz_type.lower())
        options_layout.addWidget(self.viz_type_combo)
        
        options_layout.addStretch()
        
        generate_btn = QPushButton("Generate Visualization")
        generate_btn.setIcon(QIcon.fromTheme("view-refresh", QIcon()))
        generate_btn.clicked.connect(self.generate_visualization)
        options_layout.addWidget(generate_btn)
        
        layout.addWidget(options_frame)

        # Visualization canvas frame
        self.viz_canvas_frame = QFrame()
        self.viz_canvas_frame.setFrameShape(QFrame.StyledPanel)
        self.viz_canvas_layout = QVBoxLayout(self.viz_canvas_frame)
        layout.addWidget(self.viz_canvas_frame, 1)  # Allow frame to stretch

        # Export button
        export_layout = QHBoxLayout()
        export_layout.addStretch()
        
        export_btn = QPushButton("Export Visualization")
        export_btn.setIcon(QIcon.fromTheme("document-save-as", QIcon()))
        export_btn.clicked.connect(self.export_visualization)
        export_layout.addWidget(export_btn)
        
        layout.addLayout(export_layout)

    def setup_report_tab(self):
        layout = QVBoxLayout(self.report_tab)
        
        # Header
        header = QLabel("Report Generation")
        header.setProperty("heading", "true")
        header.setAlignment(Qt.AlignCenter)
        layout.addWidget(header)

        # Options frame
        options_frame = QFrame()
        options_frame.setFrameShape(QFrame.StyledPanel)
        options_layout = QGridLayout(options_frame)
        options_layout.setContentsMargins(20, 20, 20, 20)
        options_layout.setVerticalSpacing(15)

        # Report title
        options_layout.addWidget(QLabel("Report Title:"), 0, 0)
        self.report_title = QLineEdit("Log Analysis Report")
        options_layout.addWidget(self.report_title, 0, 1, 1, 2)

        # Include sections
        options_layout.addWidget(QLabel("Include Sections:"), 1, 0)
        self.include_summary = QCheckBox("Summary", checked=True)
        self.include_results_sample = QCheckBox("Results Sample", checked=True)
        self.include_viz = QCheckBox("Visualization", checked=True)
        self.include_query = QCheckBox("Search Details", checked=True)  # Renamed from "Query Details"
        
        options_layout.addWidget(self.include_summary, 1, 1)
        options_layout.addWidget(self.include_results_sample, 1, 2)
        options_layout.addWidget(self.include_viz, 2, 1)
        options_layout.addWidget(self.include_query, 2, 2)

        # Report format
        options_layout.addWidget(QLabel("Format:"), 3, 0)
        self.report_format_group = QButtonGroup(self)
        self.report_format = "pdf"  # Default to PDF
        
        pdf_radio = QRadioButton("PDF")
        html_radio = QRadioButton("HTML")
        pdf_radio.setChecked(True)
        
        pdf_radio.toggled.connect(lambda checked: self.set_report_format("pdf") if checked else None)
        html_radio.toggled.connect(lambda checked: self.set_report_format("html") if checked else None)
        
        self.report_format_group.addButton(pdf_radio)
        self.report_format_group.addButton(html_radio)
        
        options_layout.addWidget(pdf_radio, 3, 1)
        options_layout.addWidget(html_radio, 3, 2)

        # Generate button
        generate_btn = QPushButton("Generate and Save Report")
        generate_btn.setIcon(QIcon.fromTheme("document-print", QIcon()))
        generate_btn.setMinimumHeight(40)
        generate_btn.setStyleSheet("font-weight: bold; background-color: #4CAF50; color: white;")
        generate_btn.clicked.connect(self.generate_report)
        
        options_layout.addWidget(generate_btn, 4, 0, 1, 3)

        layout.addWidget(options_frame)
        layout.addStretch()

    def set_report_format(self, report_format):
        self.report_format = report_format
        print(f"Report format set to: {report_format}")

    # --- Thread Management ---
    def start_thread(self, func, finished_slot, error_slot, *args, **kwargs):
        """Starts a worker thread and manages it."""
        # Clean up finished threads first
        self.active_threads = [t for t in self.active_threads if not t.isFinished()]

        thread = WorkerThread(func, *args, **kwargs)
        thread.finished.connect(finished_slot)
        thread.error.connect(error_slot)
        thread.finished.connect(lambda: self.cleanup_thread(thread))
        thread.error.connect(lambda: self.cleanup_thread(thread))
        self.active_threads.append(thread)
        thread.start()
        return thread

    def cleanup_thread(self, thread):
        """Removes a thread from the active list."""
        if thread in self.active_threads:
            self.active_threads.remove(thread)

    def handle_thread_error(self, error_message):
        """Displays errors from threads."""
        print(f"Error from worker thread:\n{error_message}")
        self.status_label.setText("Error occurred during processing.")
        self.progress_bar.setVisible(False)
        QMessageBox.warning(self, "Processing Error", 
                           f"An error occurred:\n{error_message.splitlines()[0]}\n\nCheck console for details.")

    # --- File Handling ---
    def upload_log_file(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self, 
            "Select Log File", 
            "", 
            "Log Files (*.csv *.log);;CSV Files (*.csv);;All Files (*.*)"
        )
        
        if file_path:
            self.status_label.setText(f"Parsing file: {os.path.basename(file_path)}...")
            self.progress_bar.setVisible(True)
            self.progress_bar.setRange(0, 0)
            
            self.start_thread(
                self.log_parser.parse_log_file,
                lambda df: self._process_log_file_result(file_path, df),
                self.handle_thread_error,
                file_path
            )

    def _process_log_file_result(self, file_path, df):
        self.progress_bar.setVisible(False)
        filename = os.path.basename(file_path)
        
        if isinstance(df, pd.DataFrame) and not df.empty and "error" not in df.columns:
            self.log_data[filename] = df
            
            cols = list(df.columns)
            col_count = len(cols)
            # Truncate long column list for display
            col_display = ", ".join(cols[:5]) + ('...' if col_count > 5 else '')

            item = QTreeWidgetItem(self.file_tree, [
                filename, 
                str(len(df)), 
                str(col_count), 
                "✅ Parsed"
            ])
            item.setToolTip(2, "\n".join(cols))  # Show full column list on hover

            self.status_label.setText(f"Parsed {filename}: {len(df)} rows, {col_count} columns")
            
            # Update the search filter panel with new data options
            self.search_filter_panel.update_field_options(self.log_data)
            
        elif "error" in df.columns:
            self.status_label.setText(f"Error parsing {filename}: {df['error'].iloc[0]}")
            QMessageBox.warning(self, "Parsing Error", f"Could not parse {filename}:\n{df['error'].iloc[0]}")
            QTreeWidgetItem(self.file_tree, [
                filename, 
                "N/A", 
                "N/A", 
                f"❌ Error: {df['error'].iloc[0]}"
            ])
        else:
            self.status_label.setText(f"Warning: No data or error parsing {filename}")
            QTreeWidgetItem(self.file_tree, [
                filename, 
                "0", 
                "0", 
                "⚠️ Empty/Failed"
            ])

    def upload_log_directory(self):
        dir_path = QFileDialog.getExistingDirectory(self, "Select Directory with Log Files")
        
        if dir_path:
            self.status_label.setText(f"Scanning directory for log files: {os.path.basename(dir_path)}...")
            self.progress_bar.setVisible(True)
            self.progress_bar.setRange(0, 0)
            
            self.start_thread(
                self.log_parser.parse_log_directory,
                self._process_log_directory_result,
                self.handle_thread_error,
                dir_path
            )

    def _process_log_directory_result(self, logs_data):
        self.progress_bar.setVisible(False)
        
        if isinstance(logs_data, dict) and "error" not in logs_data:
            parsed_count = 0
            
            for filename, df in logs_data.items():
                if isinstance(df, pd.DataFrame) and not df.empty and "error" not in df.columns:
                    self.log_data[filename] = df
                    
                    cols = list(df.columns)
                    col_count = len(cols)
                    
                    item = QTreeWidgetItem(self.file_tree, [
                        filename, 
                        str(len(df)), 
                        str(col_count), 
                        "✅ Parsed"
                    ])
                    item.setToolTip(2, "\n".join(cols))
                    parsed_count += 1
                else:
                    print(f"Skipping {filename} from directory result (empty or error).")
                    QTreeWidgetItem(self.file_tree, [
                        filename, 
                        "N/A", 
                        "N/A", 
                        "⚠️ Skipped/Empty"
                    ])

            self.status_label.setText(f"Parsed {parsed_count} log files from directory.")
            
            if parsed_count == 0:
                QMessageBox.information(self, "No Files Parsed", 
                                       "No valid log files were found or parsed in the selected directory.")
            else:
                # Update the search filter panel with new data options
                self.search_filter_panel.update_field_options(self.log_data)
                
        elif isinstance(logs_data, dict) and "error" in logs_data:
            self.status_label.setText(f"Error scanning directory: {logs_data['error']}")
            QMessageBox.warning(self, "Directory Error", logs_data['error'])
        else:
            self.status_label.setText("Failed to process directory.")

    def clear_logs(self):
        # Confirmation dialog
        reply = QMessageBox.question(
            self, 
            'Clear Logs',
            "Are you sure you want to clear all loaded log data?",
            QMessageBox.Yes | QMessageBox.No, 
            QMessageBox.No
        )

        if reply == QMessageBox.Yes:
            self.log_data.clear()
            self.current_results = pd.DataFrame()
            self.current_summary = {}
            self.current_visualization = None
            
            self.file_tree.clear()
            self.summary_display.clear()
            self.results_tree.clear()
            self.results_tree.setHeaderLabels(["Results will appear here"])

            # Clear visualization panel
            old_canvas = self.viz_canvas_frame.findChild(QWebEngineView)
            if old_canvas:
                self.viz_canvas_layout.removeWidget(old_canvas)
                old_canvas.deleteLater()

            self.status_label.setText("All logs cleared")
            print("Logs cleared.")

    # --- Search Processing (Replacing Query Processing) ---
    def run_search(self, search_params):
        """Process a search with filter parameters"""
        if not self.log_data:
            self.status_label.setText("No log data loaded. Please upload log files first.")
            QMessageBox.warning(self, "No Data", "Please upload some log files before searching.")
            return

        # Store current search params for potential saving
        self.current_search_params = dict(search_params)

        self.status_label.setText("Applying filters and analyzing...")
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 0)

        # Function to run in the background thread
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

            if not all_results:
                print("No results found across all files.")
                return pd.DataFrame(), {}

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
        self.start_thread(
            analyze_data_in_thread,
            self._process_search_result,
            self.handle_thread_error,
            self.log_data, 
            self.analyzer, 
            search_params
        )

    def _process_search_result(self, result):
        self.progress_bar.setVisible(False)
        
        if result is None:
            self.status_label.setText("Search processing failed.")
            QMessageBox.critical(self, "Error", "Search processing failed. Check logs.")
            return

        results_df, summary_dict = result
        self.current_results = results_df
        self.current_summary = summary_dict

        if isinstance(results_df, pd.DataFrame) and not results_df.empty:
            self.status_label.setText(f"Search completed: {len(results_df)} results found.")
            self._update_results_display(results_df, summary_dict)
            self.tabs.setCurrentWidget(self.results_tab)
        elif isinstance(results_df, pd.DataFrame) and results_df.empty:
            self.status_label.setText("Search completed: No matching results found.")
            self._update_results_display(results_df, summary_dict)
            QMessageBox.information(self, "No Results", "Your search criteria did not match any log entries.")
            self.tabs.setCurrentWidget(self.results_tab)
        else:
            self.status_label.setText("Search error or unexpected result type.")
            QMessageBox.warning(self, "Search Error", "An issue occurred during the search. Results might be incomplete.")
            self.summary_display.clear()
            self.results_tree.clear()

    def _update_results_display(self, results_df, summary):
        # Display Summary
        summary_text = f"Total Matching Logs: {summary.get('total_logs', 'N/A')}\n"
        
        # Show time range if available
        if 'time_range' in summary and summary['time_range']:
            start = summary['time_range'].get('start')
            end = summary['time_range'].get('end')
            
            if start:
                summary_text += f"Time Range Start: {start.strftime('%Y-%m-%d %H:%M:%S %Z')}\n"
            if end:
                summary_text += f"Time Range End: {end.strftime('%Y-%m-%d %H:%M:%S %Z')}\n"
        
        # Show search criteria used
        if 'query_params' in summary and summary['query_params']:
            summary_text += "\nSearch Criteria:\n"
            for key, value in summary['query_params'].items():
                if value:
                    summary_text += f"- {key.replace('_', ' ').title()}: {value}\n"

        # Show earliest/latest log times if available
        if 'earliest_log' in summary and pd.notna(summary['earliest_log']):
            time_format = '%Y-%m-%d %H:%M:%S %Z'
            start = summary['earliest_log'].strftime(time_format)
            end = summary['latest_log'].strftime(time_format)
            summary_text += f"\nTime Range of Results: {start} to {end}\n"
            summary_text += f"Time Span (Hours): {summary.get('time_span_hours', 'N/A'):.2f}\n"

        # Add distributions if available
        for field in ['action', 'protocol', 'severity', 'hostname', 'message_id']:
            dist_key = f"{field}_distribution"
            if dist_key in summary:
                summary_text += f"\n{field.replace('_', ' ').title()} Distribution (Top 5):\n"
                sorted_items = sorted(summary[dist_key].items(), key=lambda item: item[1], reverse=True)
                for k, v in sorted_items[:5]:
                    summary_text += f"- {k}: {v}\n"
                if len(sorted_items) > 5: 
                    summary_text += "- ...\n"

        # Add IP statistics if available
        for ip_field in ['top_src_ip', 'top_dst_ip']:
            if ip_field in summary:
                field_name = 'Source IP' if ip_field == 'top_src_ip' else 'Destination IP'
                summary_text += f"\nTop {field_name} Addresses (Top 5):\n"
                sorted_ips = sorted(summary[ip_field].items(), key=lambda item: item[1], reverse=True)
                for ip, count in sorted_ips[:5]:
                    summary_text += f"- {ip}: {count}\n"
                if len(sorted_ips) > 5: 
                    summary_text += "- ...\n"

        # Add port statistics if available
        for port_field in ['top_src_port', 'top_dst_port']:
            if port_field in summary:
                field_name = 'Source Port' if port_field == 'top_src_port' else 'Destination Port'
                summary_text += f"\nTop {field_name}s (Top 5):\n"
                sorted_ports = sorted(summary[port_field].items(), key=lambda item: item[1], reverse=True)
                for port, count in sorted_ports[:5]:
                    summary_text += f"- {port}: {count}\n"
                if len(sorted_ports) > 5: 
                    summary_text += "- ...\n"

        # Add day/hour patterns if available
        if 'daily_counts' in summary:
            summary_text += f"\nDaily Activity Pattern:\n"
            summary_text += f"- Min: {summary.get('daily_min', 'N/A')}\n"
            summary_text += f"- Max: {summary.get('daily_max', 'N/A')}\n"
            summary_text += f"- Average: {summary.get('daily_avg', 'N/A'):.2f}\n"

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

            # Adjust column widths dynamically
            for i, header in enumerate(headers):
                width = max(100, len(header) * 10)
                self.results_tree.setColumnWidth(i, min(width, 300))

            # Populate rows
            for _, row in results_sample.iterrows():
                row_values = [str(val) if pd.notna(val) else "" for val in row]
                QTreeWidgetItem(self.results_tree, row_values)

            if len(results_df) > display_limit:
                self.results_label.setText(f"Result Records (Showing {display_limit} of {len(results_df)}):")
            else:
                self.results_label.setText(f"Result Records (All {len(results_df)} shown):")
        else:
            self.results_tree.setHeaderLabels(["No results to display."])
            self.results_label.setText("Result Records (No results):")

    # --- Visualization ---
    def visualize_results(self):
        if self.current_results is None or self.current_results.empty:
            self.status_label.setText("No results to visualize. Run a search first.")
            QMessageBox.information(self, "No Results", "There are no results to visualize. Please run a search first.")
            return

        self.tabs.setCurrentWidget(self.visualization_tab)
        self.generate_visualization()

    def generate_visualization(self):
        if self.current_results is None or self.current_results.empty:
            self.status_label.setText("Cannot visualize: No results available.")
            return

        # Get visualization type from combo box
        viz_type = self.viz_type_combo.currentData()
        
        # Enforce a maximum row limit to prevent memory issues
        max_rows = 20000
        if len(self.current_results) > max_rows:
            QMessageBox.information(
                self, 
                "Large Dataset Notice", 
                f"Your dataset contains {len(self.current_results)} rows, which may cause performance issues. " 
                f"The visualization will be created using a sampled subset of {max_rows} rows."
            )

        self.status_label.setText(f"Generating {viz_type} visualization...")
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 0)

        self.start_thread(
            self.visualizer.generate_visualization,
            self._display_visualization,
            self.handle_thread_error,
            self.current_results, 
            self.current_summary, 
            viz_type
        )

    def _display_visualization(self, viz_data):
        self.progress_bar.setVisible(False)

        # Clear previous visualization
        for i in reversed(range(self.viz_canvas_layout.count())):
            widget = self.viz_canvas_layout.itemAt(i).widget()
            if widget is not None:
                self.viz_canvas_layout.removeWidget(widget)
                widget.deleteLater()

        # Check if viz_data is valid
        if not isinstance(viz_data, dict) or 'type' not in viz_data:
            self.status_label.setText("Visualization failed: Invalid data format received.")
            error_label = QLabel("Failed to generate visualization (Invalid Format).")
            error_label.setAlignment(Qt.AlignCenter)
            self.viz_canvas_layout.addWidget(error_label)
            print(f"Error: viz_data received is not a valid dictionary: {viz_data}")
            return

        viz_type = viz_data.get("type", "Unknown")
        viz_payload = viz_data.get("data")

        if viz_type == "plotly" and viz_payload and isinstance(viz_payload, go.Figure):
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
                error_label.setAlignment(Qt.AlignCenter)
                self.viz_canvas_layout.addWidget(error_label)
                print(f"Visualization display error: {e}\n{traceback.format_exc()}")

        elif viz_type == "table":
            # Handle tabular data
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
        """Export the current visualization to a file"""
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
            
            # HTML export
            if is_html:
                progress.setValue(30)
                progress.setLabelText("Generating HTML...")
                QApplication.processEvents()
                
                try:
                    # Direct HTML export
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
            
            # For image export
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

    def export_results(self):
        """Export the current results to a file"""
        if self.current_results is None or self.current_results.empty:
            self.status_label.setText("No results to export.")
            QMessageBox.warning(self, "No Results", "There are no results to export. Please run a search first.")
            return

        # Default filename suggestion
        default_filename = f"results_{datetime.now():%Y%m%d_%H%M%S}.csv"
        file_path, selected_filter = QFileDialog.getSaveFileName(
            self, "Export Results", 
            default_filename,
            "CSV (Comma delimited) (*.csv);;Excel Worksheet (*.xlsx);;JSON Lines (*.jsonl)"
        )

        if file_path:
            self.status_label.setText(f"Exporting results to {os.path.basename(file_path)}...")
            self.progress_bar.setVisible(True)
            self.progress_bar.setRange(0, 0)

            # Function to run in the background thread
            def export_in_thread(df, path, file_format):
                try:
                    file_format = file_format.lower()
                    if file_format == '.csv':
                        df.to_csv(path, index=False, encoding='utf-8')
                    elif file_format == '.xlsx':
                        try:
                             df.to_excel(path, index=False, engine='openpyxl')
                        except ImportError:
                             return "Error: 'openpyxl' package required for Excel export. Install with 'pip install openpyxl'."
                    elif file_format == '.jsonl':
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

            self.start_thread(
                export_in_thread,
                self._handle_export_result,
                self.handle_thread_error,
                self.current_results, 
                file_path, 
                file_ext
            )

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
        """Generate a report based on current results"""
        if self.current_results is None or self.current_summary is None:
            self.status_label.setText("No results available to generate a report.")
            QMessageBox.warning(self, "No Data", "Cannot generate report: No search results available.")
            return

        # Prepare base report data
        report_data = {
            'title': self.report_title.text() or "Log Analysis Report",
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'query': str(self.current_summary.get('query_params', {})) if self.include_query.isChecked() else None,
            'summary': self.current_summary if self.include_summary.isChecked() else None,
            'results_sample': self.current_results.head(100) if self.include_results_sample.isChecked() and not self.current_results.empty else None,
            'visualization_path': None  # Will be populated only for HTML reports or if export succeeds
        }

        if self.include_viz.isChecked() and self.current_visualization:
            report_data['current_visualization'] = self.current_visualization

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

        # Handle visualization preparation
        temp_viz_file = None
        if self.include_viz.isChecked() and self.current_visualization and self.current_visualization['type'] == 'plotly':
            if report_format == 'html':
                # For HTML, no need to create temp file, we'll embed directly
                pass
            elif not skip_viz_for_pdf:
                # For PDF, try to create image but with careful error handling
                self.status_label.setText("Preparing visualization for PDF report...")
                self.progress_bar.setValue(20)
                try:
                    # Create a temporary image file
                    with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp:
                        temp_viz_file = tmp.name
                    
                    # Generate the image using the direct bytes approach
                    try:
                        fig = self.current_visualization['figure']
                        # Use a simplified version of the figure if possible
                        if hasattr(fig, 'update_layout'):
                            fig.update_layout(
                                width=800, 
                                height=500,
                                font=dict(size=10)
                            )
                        
                        # Get image bytes with a small timeout
                        img_bytes = fig.to_image(format='png', scale=1.5)
                        with open(temp_viz_file, 'wb') as f:
                            f.write(img_bytes)
                        report_data['visualization_path'] = temp_viz_file
                        print(f"Visualization saved for PDF report: {temp_viz_file}")
                        if self.include_viz.isChecked() and self.current_visualization:
                            report_data['current_visualization'] = self.current_visualization
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
                    # Continue without visualization
                    if temp_viz_file and os.path.exists(temp_viz_file):
                        try:
                            os.unlink(temp_viz_file)
                        except:
                            pass
                    temp_viz_file = None
        
        # Generate the report
        self.progress_bar.setValue(30)
        try:
            # Generate HTML report directly
            if report_format == 'html':
                self.status_label.setText("Generating HTML report...")
                self.progress_bar.setValue(50)
                
                # Generate HTML content using our custom method
                html_content = self.report_generator._generate_simple_html_report(report_data)
                
                self.progress_bar.setValue(80)
                # Write HTML file
                with open(save_path, 'w', encoding='utf-8') as f:
                    f.write(html_content)
                
                success = True
                report_file_path = save_path
            
            # For PDF, use the report generator class
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
                        html_content = self.report_generator._generate_simple_html_report(report_data)
                        with open(html_path, 'w', encoding='utf-8') as f:
                            f.write(html_content)
                        success = True
                        report_file_path = html_path
                    else:
                        success = False
                        report_file_path = None
            
            # Clean up temporary files
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

    # --- Application Closing ---
    def closeEvent(self, event):
        """Handles application close requests."""
        if self.active_threads:
            reply = QMessageBox.question(
                self, 'Exit Confirmation',
                f"{len(self.active_threads)} background task(s) might still be running. Exit anyway?",
                QMessageBox.Yes | QMessageBox.No, QMessageBox.No
            )
            if reply == QMessageBox.No:
                event.ignore()
                return

        print("Cleaning up and closing application...")
        event.accept()

# --- For direct testing/execution ---
if __name__ == "__main__":
    # Set high DPI scaling attribute BEFORE creating QApplication
    if hasattr(Qt, 'AA_EnableHighDpiScaling'):
        QApplication.setAttribute(Qt.AA_EnableHighDpiScaling, True)
    if hasattr(Qt, 'AA_UseHighDpiPixmaps'):
        QApplication.setAttribute(Qt.AA_UseHighDpiPixmaps, True)

    app = QApplication(sys.argv)

    from PyQt5 import QtWidgets

    try:
        window = EONParserGUI()
        window.show()
        sys.exit(app.exec_())
    except Exception as e:
        try:
            QMessageBox.critical(None, "Application Error", 
                               f"A critical error occurred on startup:\n{e}\n{traceback.format_exc()}")
        except:
            print(f"Main execution error: {str(e)}\n{traceback.format_exc()}")
        sys.exit(1)