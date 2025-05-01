import sys
import traceback
from PyQt5.QtWidgets import (QApplication, QMainWindow, QTabWidget, QWidget, QVBoxLayout, QHBoxLayout, 
                             QPushButton, QFileDialog, QTreeWidget, QTreeWidgetItem, QTextEdit, QLabel, 
                             QRadioButton, QCheckBox, QLineEdit, QProgressBar, QFrame)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
import pandas as pd
import os
from datetime import datetime
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
import plotly
import tempfile
import webbrowser

from nlp_engine import NLPQueryProcessor
from log_parser import LogParser
from analyzer import LogAnalyzer
from log_visualizer import LogVisualizer
from report_generator import ReportGenerator

class WorkerThread(QThread):
    finished = pyqtSignal(object)
    error = pyqtSignal(str)

    def __init__(self, func):
        super().__init__()
        self.func = func

    def run(self):
        try:
            result = self.func()
            self.finished.emit(result)
        except Exception as e:
            self.error.emit(f"Thread error: {str(e)}\n{traceback.format_exc()}")

    def __del__(self):
        self.wait()  # Ensure thread finishes before destruction

class EONParserGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        try:
            self.setWindowTitle("EONParser - Log Analysis Tool")
            self.setGeometry(100, 100, 1200, 800)
            self.setMinimumSize(1000, 700)

            self.nlp_processor = NLPQueryProcessor()
            self.log_parser = LogParser()
            self.analyzer = LogAnalyzer()
            self.visualizer = LogVisualizer()
            self.report_generator = ReportGenerator()

            self.log_data = {}
            self.current_results = None
            self.current_summary = None
            self.current_visualization = None

            self.setup_ui()
        except Exception as e:
            print(f"Initialization error: {str(e)}\n{traceback.format_exc()}")
            sys.exit(1)

    def setup_ui(self):
        try:
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

            self.tabs.addTab(self.upload_tab, "Upload Logs")
            self.tabs.addTab(self.query_tab, "Query")
            self.tabs.addTab(self.results_tab, "Results")
            self.tabs.addTab(self.visualization_tab, "Visualization")
            self.tabs.addTab(self.report_tab, "Report")

            self.setup_upload_tab()
            self.setup_query_tab()
            self.setup_results_tab()
            self.setup_visualization_tab()
            self.setup_report_tab()

            self.status_label = QLabel("Ready")
            self.progress_bar = QProgressBar()
            self.progress_bar.setMaximum(100)
            self.progress_bar.setValue(0)
            status_layout = QHBoxLayout()
            status_layout.addWidget(self.status_label)
            status_layout.addWidget(self.progress_bar)
            main_layout.addLayout(status_layout)
        except Exception as e:
            print(f"UI setup error: {str(e)}\n{traceback.format_exc()}")
            sys.exit(1)

    def setup_upload_tab(self):
        layout = QVBoxLayout(self.upload_tab)
        layout.addWidget(QLabel("<h2>Upload Log Files</h2>", alignment=Qt.AlignCenter))

        btn_layout = QHBoxLayout()
        upload_file_btn = QPushButton("Upload Log File")
        upload_dir_btn = QPushButton("Upload Log Directory")
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
        self.file_tree.setHeaderLabels(["Filename", "Records", "Type", "Status"])
        self.file_tree.setColumnWidth(0, 300)
        self.file_tree.setColumnWidth(1, 100)
        self.file_tree.setColumnWidth(2, 150)
        self.file_tree.setColumnWidth(3, 150)
        layout.addWidget(self.file_tree)

        layout.addWidget(QLabel("Upload log files or directories to begin analysis.", alignment=Qt.AlignCenter))

    def setup_query_tab(self):
        layout = QVBoxLayout(self.query_tab)
        layout.addWidget(QLabel("<h2>Natural Language Query</h2>", alignment=Qt.AlignCenter))

        self.query_edit = QTextEdit("Show me all error logs from yesterday")
        self.query_edit.setMaximumHeight(100)
        layout.addWidget(QLabel("Enter your query in plain English:"))
        layout.addWidget(self.query_edit)

        examples = [
            "Show me all error logs from yesterday",
            "Find failed login attempts in the last 24 hours",
            "Count firewall block events by source IP",
            "Show a trend of network errors over time",
            "Visualize distribution of actions in the firewall logs",
            "Find all events related to user admin in the last week"
        ]
        for example in examples:
            btn = QPushButton(example)
            btn.clicked.connect(lambda _, e=example: self.query_edit.setText(e))
            layout.addWidget(btn)

        self.params_display = QTextEdit()
        self.params_display.setReadOnly(True)
        layout.addWidget(QLabel("Query Parameters:"))
        layout.addWidget(self.params_display)

        btn_layout = QHBoxLayout()
        parse_btn = QPushButton("Parse Query")
        run_btn = QPushButton("Run Query")
        parse_btn.clicked.connect(self.parse_query)
        run_btn.clicked.connect(self.run_query)
        btn_layout.addWidget(parse_btn)
        btn_layout.addWidget(run_btn)
        layout.addLayout(btn_layout)

    def setup_results_tab(self):
        layout = QVBoxLayout(self.results_tab)
        layout.addWidget(QLabel("<h2>Analysis Results</h2>", alignment=Qt.AlignCenter))

        self.summary_display = QTextEdit()
        self.summary_display.setReadOnly(True)
        layout.addWidget(QLabel("Summary:"))
        layout.addWidget(self.summary_display)

        self.results_tree = QTreeWidget()
        layout.addWidget(self.results_tree)

        btn_layout = QHBoxLayout()
        visualize_btn = QPushButton("Visualize Results")
        export_btn = QPushButton("Export Results")
        visualize_btn.clicked.connect(self.visualize_results)
        export_btn.clicked.connect(self.export_results)
        btn_layout.addWidget(visualize_btn)
        btn_layout.addStretch()
        btn_layout.addWidget(export_btn)
        layout.addLayout(btn_layout)

    def setup_visualization_tab(self):
        layout = QVBoxLayout(self.visualization_tab)
        layout.addWidget(QLabel("<h2>Visualization</h2>", alignment=Qt.AlignCenter))

        viz_frame = QFrame()
        viz_layout = QVBoxLayout(viz_frame)
        self.viz_type = "auto"
        for text, value in [("Auto (NLP Determined)", "auto"), ("Trend Chart", "trend"), 
                            ("Pie Chart", "pie"), ("Bar Chart", "bar"), ("Heat Map", "heatmap")]:
            radio = QRadioButton(text)
            if value == "auto":
                radio.setChecked(True)
            radio.toggled.connect(lambda checked, v=value: self.set_viz_type(v) if checked else None)
            viz_layout.addWidget(radio)
        generate_btn = QPushButton("Generate Visualization")
        generate_btn.clicked.connect(self.generate_visualization)
        viz_layout.addWidget(generate_btn)
        layout.addWidget(viz_frame)

        self.viz_frame = QFrame()
        layout.addWidget(self.viz_frame)

        export_btn = QPushButton("Export Visualization")
        export_btn.clicked.connect(self.export_visualization)
        layout.addWidget(export_btn)

    def setup_report_tab(self):
        layout = QVBoxLayout(self.report_tab)
        layout.addWidget(QLabel("<h2>Report Generation</h2>", alignment=Qt.AlignCenter))

        options_frame = QFrame()
        options_layout = QVBoxLayout(options_frame)
        self.report_title = QLineEdit("Log Analysis Report")
        options_layout.addWidget(QLabel("Report Title:"))
        options_layout.addWidget(self.report_title)

        self.include_summary = QCheckBox("Include Summary", checked=True)
        self.include_results = QCheckBox("Include Results Table", checked=True)
        self.include_viz = QCheckBox("Include Visualizations", checked=True)
        self.include_query = QCheckBox("Include Query Details", checked=True)
        options_layout.addWidget(self.include_summary)
        options_layout.addWidget(self.include_results)
        options_layout.addWidget(self.include_viz)
        options_layout.addWidget(self.include_query)

        self.report_format = "pdf"
        for text, value in [("PDF", "pdf"), ("HTML", "html")]:
            radio = QRadioButton(text)
            if value == "pdf":
                radio.setChecked(True)
            radio.toggled.connect(lambda checked, v=value: setattr(self, "report_format", v) if checked else None)
            options_layout.addWidget(radio)

        generate_btn = QPushButton("Generate Report")
        generate_btn.clicked.connect(self.generate_report)
        options_layout.addWidget(generate_btn)
        layout.addWidget(options_frame)

        self.report_preview = QTextEdit()
        self.report_preview.setReadOnly(True)
        layout.addWidget(QLabel("Report Preview:"))
        layout.addWidget(self.report_preview)

    def set_viz_type(self, viz_type):
        self.viz_type = viz_type

    def upload_log_file(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Select Log File", "", "Log Files (*.log *.txt);;All Files (*.*)")
        if file_path:
            self.status_label.setText(f"Parsing log file: {os.path.basename(file_path)}...")
            thread = WorkerThread(lambda: self.log_parser.parse_log_file(file_path))
            thread.finished.connect(lambda df: self._process_log_file_result(file_path, df))
            thread.error.connect(lambda err: self.status_label.setText(f"Error: {err}"))
            thread.start()
            self.threads = getattr(self, 'threads', [])  # Keep track of threads
            self.threads.append(thread)

    def _process_log_file_result(self, file_path, df):
        filename = os.path.basename(file_path)
        self.log_data[filename] = df
        log_type = df["log_type"].iloc[0] if "log_type" in df.columns and not df["log_type"].isna().all() else "Unknown"
        QTreeWidgetItem(self.file_tree, [filename, str(len(df)), log_type, "Parsed"])
        self.status_label.setText(f"Parsed {filename}: {len(df)} records")

    def upload_log_directory(self):
        dir_path = QFileDialog.getExistingDirectory(self, "Select Directory with Log Files")
        if dir_path:
            self.status_label.setText(f"Parsing log directory: {os.path.basename(dir_path)}...")
            thread = WorkerThread(lambda: self.log_parser.parse_log_directory(dir_path))
            thread.finished.connect(self._process_log_directory_result)
            thread.error.connect(lambda err: self.status_label.setText(f"Error: {err}"))
            thread.start()
            self.threads = getattr(self, 'threads', [])
            self.threads.append(thread)

    def _process_log_directory_result(self, logs_data):
        if "error" in logs_data:
            self.status_label.setText(f"Error: {logs_data['error']}")
            return
        for filename, df in logs_data.items():
            self.log_data[filename] = df
            log_type = df["log_type"].iloc[0] if "log_type" in df.columns and not df["log_type"].isna().all() else "Unknown"
            QTreeWidgetItem(self.file_tree, [filename, str(len(df)), log_type, "Parsed"])
        self.status_label.setText(f"Parsed {len(logs_data)} log files from directory")

    def clear_logs(self):
        self.log_data.clear()
        self.current_results = None
        self.current_summary = None
        self.current_visualization = None
        self.file_tree.clear()
        self.summary_display.clear()
        self.results_tree.clear()
        self.viz_frame.deleteLater()
        self.viz_frame = QFrame()
        self.visualization_tab.layout().insertWidget(1, self.viz_frame)
        self.status_label.setText("All logs cleared")

    def parse_query(self):
        query = self.query_edit.toPlainText().strip()
        if not query:
            self.status_label.setText("Please enter a query")
            return
        params = self.nlp_processor.process_query(query)
        param_text = (f"Time Range: {params['time_range']['start']} to {params['time_range']['end']}\n"
                      f"Log Type: {params['log_type'] or 'Any'}\n"
                      f"Action: {params['action'] or 'Any'}\n"
                      f"IP Address: {params['ip_address'] or 'Any'}\n"
                      f"User: {params['user'] or 'Any'}\n"
                      f"Count Request: {'Yes' if params['count_request'] else 'No'}\n"
                      f"Visualization Type: {params['viz_type']}\n"
                      f"Keywords: {', '.join(params['keywords']) if params['keywords'] else 'None'}")
        self.params_display.setText(param_text)
        self.status_label.setText("Query parsed successfully")

    def run_query(self):
        query = self.query_edit.toPlainText().strip()
        if not query:
            self.status_label.setText("Please enter a query")
            return
        if not self.log_data:
            self.status_label.setText("No log data loaded. Please upload logs first.")
            return
        params = self.nlp_processor.process_query(query)
        combined_data = pd.concat(self.log_data.values(), ignore_index=True)
        
        # Add timezone conversion
        if "datetime" in combined_data.columns:
            combined_data["datetime"] = combined_data["datetime"].dt.tz_convert('UTC')
        
        self.status_label.setText("Running query...")
        thread = WorkerThread(lambda: (self.analyzer.analyze(combined_data, params), 
                                      self.analyzer.generate_summary(combined_data, params)))
        thread.finished.connect(self._process_query_result)
        thread.error.connect(lambda err: self.status_label.setText(f"Error running query: {err}"))
        thread.start()
        self.threads = getattr(self, 'threads', [])
        self.threads.append(thread)

    def _process_query_result(self, result):
        results, summary = result
        self.current_results = results
        self.current_summary = summary
        self._update_results_display(results, summary)
        self.tabs.setCurrentWidget(self.results_tab)
        self.status_label.setText(f"Query completed: {len(results)} results found")
            
    # Add debug output
        print(f"Query returned {len(results)} records")
        print("Sample results:", results.head(2).to_dict())
    
        self.current_results = results
        self.current_summary = summary
        self._update_results_display(results, summary)

    def _update_results_display(self, results, summary):
        summary_text = f"Total Logs: {summary['total_logs']}\n"
        if 'earliest_log' in summary:
            summary_text += f"Time Range: {summary['earliest_log']} to {summary['latest_log']}\n"
        if summary.get('keywords'):
            summary_text += f"Keywords: {', '.join(summary['keywords'])}\n"
        if "log_type_distribution" in summary:
            summary_text += "\nLog Type Distribution:\n" + "\n".join(f"- {k}: {v}" for k, v in summary["log_type_distribution"].items())
        self.summary_display.setText(summary_text)
        
        self.results_tree.clear()
        self.results_tree.setHeaderLabels(list(results.columns))
        for _, row in results.head(1000).iterrows():
            QTreeWidgetItem(self.results_tree, [str(val) if val is not None else "" for val in row])

# In visualize_results method
    def visualize_results(self):
    # Replace this check
    # if not self.current_results or not self.current_summary:
    
    # With this corrected version
        if self.current_results is None or \
            self.current_summary is None or \
        (isinstance(self.current_results, pd.DataFrame) and self.current_results.empty):
        
            self.status_label.setText("No results to visualize. Run a query first.")
            return
        
        self.tabs.setCurrentWidget(self.visualization_tab)
        self.generate_visualization()

    def generate_visualization(self):
        viz_type = self.viz_type if self.viz_type != "auto" else self.current_summary.get("viz_type", "table")
        self.status_label.setText(f"Generating {viz_type} visualization...")
        thread = WorkerThread(lambda: self.visualizer.generate_visualization(self.current_results, self.current_summary, viz_type))
        thread.finished.connect(self._display_visualization)
        thread.error.connect(lambda err: self.status_label.setText(f"Error: {err}"))
        thread.start()
        self.threads = getattr(self, 'threads', [])
        self.threads.append(thread)

    def _display_visualization(self, viz_data):
        self.viz_frame.deleteLater()
        self.viz_frame = QFrame()
        self.visualization_tab.layout().insertWidget(1, self.viz_frame)
        layout = QVBoxLayout(self.viz_frame)
        
        if viz_data["type"] == "plotly":
            fig = viz_data["data"]
            canvas = FigureCanvas(fig)
            layout.addWidget(canvas)
            self.current_visualization = {'type': viz_data["type"], 'figure': fig}
        self.status_label.setText(f"{viz_data['type'].capitalize()} visualization created successfully")

    def export_visualization(self):
        if not self.current_visualization:
            self.status_label.setText("No visualization to export.")
            return
        file_path, _ = QFileDialog.getSaveFileName(self, "Save Visualization", "", "PNG (*.png);;JPEG (*.jpg);;PDF (*.pdf);;SVG (*.svg)")
        if file_path:
            self.current_visualization['figure'].write_image(file_path, scale=2)
            self.status_label.setText(f"Visualization exported to {os.path.basename(file_path)}")

    def export_results(self):
        if not self.current_results:
            self.status_label.setText("No results to export.")
            return
        file_path, _ = QFileDialog.getSaveFileName(self, "Export Results", "", "CSV (*.csv);;Excel (*.xlsx);;JSON (*.json)")
        if file_path:
            ext = os.path.splitext(file_path)[1].lower()
            if ext == '.csv':
                self.current_results.to_csv(file_path, index=False)
            elif ext == '.xlsx':
                self.current_results.to_excel(file_path, index=False)
            elif ext == '.json':
                self.current_results.to_json(file_path, orient='records')
            self.status_label.setText(f"Results exported to {os.path.basename(file_path)}")

    def generate_report(self):
        if not self.current_results or not self.current_summary:
            self.status_label.setText("No results to include in report.")
            return
        report_data = {
            'title': self.report_title.text(),
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'summary': self.current_summary if self.include_summary.isChecked() else None,
            'results': self.current_results if self.include_results.isChecked() else None,
            'query': self.query_edit.toPlainText().strip() if self.include_query.isChecked() else None
        }
        if self.include_viz.isChecked() and self.current_visualization:
            with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp:
                self.current_visualization['figure'].write_image(tmp.name)
                report_data['visualization'] = {'path': tmp.name, 'type': self.current_visualization['type']}
        
        thread = WorkerThread(lambda: self.report_generator.generate_report(report_data, self.report_format))
        thread.finished.connect(self._display_report)
        thread.error.connect(lambda err: self.status_label.setText(f"Error: {err}"))
        thread.start()
        self.threads = getattr(self, 'threads', [])
        self.threads.append(thread)

    def _display_report(self, result):
        report_file, preview_text = result
        self.report_preview.setText(preview_text)
        if self.report_format == 'html':
            webbrowser.open(f'file://{report_file}')
        self.status_label.setText(f"Report generated: {os.path.basename(report_file)}")

    def closeEvent(self, event):
        # Ensure all threads are finished before closing
        if hasattr(self, 'threads'):
            for thread in self.threads:
                thread.quit()
                thread.wait()
        event.accept()

def main():
    app = QApplication(sys.argv)
    try:
        window = EONParserGUI()
        window.show()
        sys.exit(app.exec_())
    except Exception as e:
        print(f"Main error: {str(e)}\n{traceback.format_exc()}")
        sys.exit(1)

if __name__ == "__main__":
    main()


