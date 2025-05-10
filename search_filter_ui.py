# search_filter_ui.py
from PyQt5.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QFormLayout, 
                            QLabel, QLineEdit, QComboBox, QDateTimeEdit, 
                            QPushButton, QGroupBox, QRadioButton, QCheckBox, 
                            QScrollArea, QSpacerItem, QSizePolicy, QFrame,
                            QTabWidget, QCompleter)
from PyQt5.QtCore import Qt, QDateTime, QDate, QTime, QStringListModel, pyqtSignal
from PyQt5.QtGui import QIcon, QFont, QIntValidator
import pandas as pd
from datetime import datetime, timedelta
import pytz

class DateRangeSelector(QWidget):
    """Custom date range selector widget with presets and calendar options"""
    
    dateRangeChanged = pyqtSignal(dict)
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)
        
        # Time range presets
        self.presets_group = QGroupBox("Time Range Presets")
        self.presets_layout = QVBoxLayout()
        
        # Create radio buttons for common time ranges
        self.radio_all = QRadioButton("All Time")
        self.radio_today = QRadioButton("Today")
        self.radio_yesterday = QRadioButton("Yesterday")
        self.radio_last_hour = QRadioButton("Last Hour")
        self.radio_last_24hours = QRadioButton("Last 24 Hours")
        self.radio_last_7days = QRadioButton("Last 7 Days")
        self.radio_last_30days = QRadioButton("Last 30 Days")
        self.radio_custom = QRadioButton("Custom Range")
        
        # Default to Last 24 Hours
        self.radio_last_24hours.setChecked(True)
        
        # Add radio buttons to layout
        self.presets_layout.addWidget(self.radio_all)
        self.presets_layout.addWidget(self.radio_today)
        self.presets_layout.addWidget(self.radio_yesterday)
        self.presets_layout.addWidget(self.radio_last_hour)
        self.presets_layout.addWidget(self.radio_last_24hours)
        self.presets_layout.addWidget(self.radio_last_7days)
        self.presets_layout.addWidget(self.radio_last_30days)
        self.presets_layout.addWidget(self.radio_custom)
        self.presets_group.setLayout(self.presets_layout)
        
        # Custom date range selector
        self.custom_group = QGroupBox("Custom Date Range")
        self.custom_layout = QFormLayout()
        
        # From date/time
        self.from_datetime = QDateTimeEdit()
        self.from_datetime.setDateTime(QDateTime.currentDateTime().addDays(-1))
        self.from_datetime.setCalendarPopup(True)
        self.from_datetime.setDisplayFormat("yyyy-MM-dd HH:mm:ss")
        
        # To date/time
        self.to_datetime = QDateTimeEdit()
        self.to_datetime.setDateTime(QDateTime.currentDateTime())
        self.to_datetime.setCalendarPopup(True)
        self.to_datetime.setDisplayFormat("yyyy-MM-dd HH:mm:ss")
        
        self.custom_layout.addRow("From:", self.from_datetime)
        self.custom_layout.addRow("To:", self.to_datetime)
        self.custom_group.setLayout(self.custom_layout)
        self.custom_group.setEnabled(False)  # Disabled by default
        
        # Add groups to layout
        self.layout.addWidget(self.presets_group)
        self.layout.addWidget(self.custom_group)
        self.layout.addStretch()
        
        # Connect signals
        self.radio_all.toggled.connect(self.handle_preset_change)
        self.radio_today.toggled.connect(self.handle_preset_change)
        self.radio_yesterday.toggled.connect(self.handle_preset_change)
        self.radio_last_hour.toggled.connect(self.handle_preset_change)
        self.radio_last_24hours.toggled.connect(self.handle_preset_change)
        self.radio_last_7days.toggled.connect(self.handle_preset_change)
        self.radio_last_30days.toggled.connect(self.handle_preset_change)
        self.radio_custom.toggled.connect(self.handle_preset_change)
        
        self.from_datetime.dateTimeChanged.connect(self.handle_custom_change)
        self.to_datetime.dateTimeChanged.connect(self.handle_custom_change)
        
        # Set initial time range
        self.handle_preset_change()
    
    def handle_preset_change(self):
        """Handle changes in time range preset selection"""
        self.custom_group.setEnabled(self.radio_custom.isChecked())
        
        # Calculate time range based on selected preset
        now = QDateTime.currentDateTime()
        start_time = None
        end_time = None
        
        if self.radio_all.isChecked():
            # All time = no start/end time constraints
            start_time = None
            end_time = None
        
        elif self.radio_today.isChecked():
            # Today = midnight to now
            start_time = QDateTime(now.date(), QTime(0, 0))
            end_time = now
        
        elif self.radio_yesterday.isChecked():
            # Yesterday = previous day, midnight to midnight
            yesterday = now.addDays(-1).date()
            start_time = QDateTime(yesterday, QTime(0, 0))
            end_time = QDateTime(yesterday, QTime(23, 59, 59, 999))
        
        elif self.radio_last_hour.isChecked():
            # Last hour = 1 hour ago to now
            start_time = now.addSecs(-3600)
            end_time = now
        
        elif self.radio_last_24hours.isChecked():
            # Last 24 hours = 24 hours ago to now
            start_time = now.addDays(-1)
            end_time = now
        
        elif self.radio_last_7days.isChecked():
            # Last 7 days = 7 days ago to now
            start_time = now.addDays(-7)
            end_time = now
        
        elif self.radio_last_30days.isChecked():
            # Last 30 days = 30 days ago to now
            start_time = now.addDays(-30)
            end_time = now
        
        elif self.radio_custom.isChecked():
            # Custom range = use values from date/time pickers
            start_time = self.from_datetime.dateTime()
            end_time = self.to_datetime.dateTime()
        
        # Update custom date/time pickers to match preset (for easy customization)
        if not self.radio_custom.isChecked() and start_time and end_time:
            self.from_datetime.setDateTime(start_time)
            self.to_datetime.setDateTime(end_time)
        
        # Convert QDateTime to Python datetime with timezone (UTC)
        time_range = {}
        
        if start_time:
            start_dt = start_time.toPyDateTime()
            time_range["start"] = start_dt.replace(tzinfo=pytz.UTC)
        else:
            time_range["start"] = None
            
        if end_time:
            end_dt = end_time.toPyDateTime()
            time_range["end"] = end_dt.replace(tzinfo=pytz.UTC)
        else:
            time_range["end"] = None
        
        # Emit signal with time range
        self.dateRangeChanged.emit(time_range)
    
    def handle_custom_change(self):
        """Handle changes in custom date/time selection"""
        if self.radio_custom.isChecked():
            self.handle_preset_change()
    
    def get_date_range(self):
        """Get the currently selected date range as a dictionary"""
        if self.radio_custom.isChecked():
            start_dt = self.from_datetime.dateTime().toPyDateTime().replace(tzinfo=pytz.UTC)
            end_dt = self.to_datetime.dateTime().toPyDateTime().replace(tzinfo=pytz.UTC)
            return {"start": start_dt, "end": end_dt}
        
        # If not custom, recalculate based on the preset
        self.handle_preset_change()
        
        # Radio_all returns None for both start and end
        if self.radio_all.isChecked():
            return {"start": None, "end": None}
        
        # Otherwise get the values from the date/time pickers (which were updated by handle_preset_change)
        start_dt = self.from_datetime.dateTime().toPyDateTime().replace(tzinfo=pytz.UTC)
        end_dt = self.to_datetime.dateTime().toPyDateTime().replace(tzinfo=pytz.UTC)
        return {"start": start_dt, "end": end_dt}


class SearchFilterPanel(QWidget):
    """Complete search filter panel with all filter options"""
    
    searchRequested = pyqtSignal(dict)
    resetRequested = pyqtSignal()
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setup_ui()
    
    def setup_ui(self):
        """Set up the user interface"""
        main_layout = QVBoxLayout(self)
        main_layout.setSpacing(10)
        
        # Create a scroll area to contain all filters
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setFrameShape(QFrame.NoFrame)
        
        # Container widget for scroll area
        scroll_content = QWidget()
        scroll_layout = QVBoxLayout(scroll_content)
        scroll_layout.setSpacing(15)
        
        # === Time Range Section ===
        time_group = QGroupBox("Time Range")
        time_layout = QVBoxLayout()
        self.date_range_selector = DateRangeSelector()
        time_layout.addWidget(self.date_range_selector)
        time_group.setLayout(time_layout)
        scroll_layout.addWidget(time_group)
        
        # === IP & Port Filters ===
        network_group = QGroupBox("Network Filters")
        network_layout = QFormLayout()
        
        self.src_ip_input = QLineEdit()
        self.src_ip_input.setPlaceholderText("e.g. 192.168.1")
        
        self.dst_ip_input = QLineEdit()
        self.dst_ip_input.setPlaceholderText("e.g. 10.0.0")
        
        self.src_port_input = QLineEdit()
        self.src_port_input.setPlaceholderText("e.g. 443")
        self.src_port_input.setValidator(QIntValidator(1, 65535))
        
        self.dst_port_input = QLineEdit()
        self.dst_port_input.setPlaceholderText("e.g. 80")
        self.dst_port_input.setValidator(QIntValidator(1, 65535))
        
        network_layout.addRow("Source IP:", self.src_ip_input)
        network_layout.addRow("Destination IP:", self.dst_ip_input)
        network_layout.addRow("Source Port:", self.src_port_input)
        network_layout.addRow("Destination Port:", self.dst_port_input)
        network_group.setLayout(network_layout)
        scroll_layout.addWidget(network_group)
        
        # === Log Details ===
        details_group = QGroupBox("Log Details")
        details_layout = QFormLayout()
        
        self.hostname_input = QLineEdit()
        self.hostname_input.setPlaceholderText("e.g. ASA-5510")
        
        self.message_id_input = QLineEdit()
        self.message_id_input.setPlaceholderText("e.g. 302013")
        
        # Use standard QComboBox instead of custom component
        self.protocol_combo = QComboBox()
        self.protocol_combo.addItem("Any", None)
        for protocol in ["TCP", "UDP", "ICMP", "HTTP", "HTTPS", "DNS"]:
            self.protocol_combo.addItem(protocol, protocol)
        
        self.severity_combo = QComboBox()
        self.severity_combo.addItem("Any", None)
        for severity in range(1, 6):
            self.severity_combo.addItem(f"Level {severity}", severity)
        
        # Use standard QComboBox for action as well
        self.action_combo = QComboBox()
        self.action_combo.addItem("Any", None)
        for action in ["ALLOW", "DENY", "DROP", "REJECT"]:
            self.action_combo.addItem(action, action)
        
        details_layout.addRow("Hostname:", self.hostname_input)
        details_layout.addRow("Message ID:", self.message_id_input)
        details_layout.addRow("Protocol:", self.protocol_combo)
        details_layout.addRow("Severity:", self.severity_combo)
        details_layout.addRow("Action:", self.action_combo)
        details_group.setLayout(details_layout)
        scroll_layout.addWidget(details_group)
        
        # === Text Search ===
        text_group = QGroupBox("Text Search")
        text_layout = QVBoxLayout()
        
        self.message_text_input = QLineEdit()
        self.message_text_input.setPlaceholderText("Search in message field")
        
        self.full_text_input = QLineEdit()
        self.full_text_input.setPlaceholderText("Search in all fields")
        
        text_form = QFormLayout()
        text_form.addRow("Message Text:", self.message_text_input)
        text_form.addRow("Full Text Search:", self.full_text_input)
        text_layout.addLayout(text_form)
        text_group.setLayout(text_layout)
        scroll_layout.addWidget(text_group)
        
        # === Advanced Options ===
        advanced_group = QGroupBox("Advanced Options")
        advanced_layout = QFormLayout()
        
        # Case sensitive search option
        self.case_sensitive_check = QCheckBox("Case Sensitive Search")
        self.case_sensitive_check.setChecked(False)
        
        # Regex search option
        self.regex_check = QCheckBox("Use Regular Expressions")
        self.regex_check.setChecked(False)
        
        # Visualization type selection
        self.viz_type_combo = QComboBox()
        for viz_type in ["Auto", "Trend", "Pie", "Bar", "Heatmap", "Summary"]:
            self.viz_type_combo.addItem(viz_type, viz_type.lower())
        
        # Results limit
        self.results_limit_combo = QComboBox()
        for limit in ["100", "500", "1000", "5000", "10000", "All"]:
            value = None if limit == "All" else int(limit)
            self.results_limit_combo.addItem(limit, value)
        self.results_limit_combo.setCurrentIndex(2)  # Default to 1000
        
        advanced_layout.addRow("", self.case_sensitive_check)
        advanced_layout.addRow("", self.regex_check)
        advanced_layout.addRow("Visualization Type:", self.viz_type_combo)
        advanced_layout.addRow("Results Limit:", self.results_limit_combo)
        
        advanced_group.setLayout(advanced_layout)
        scroll_layout.addWidget(advanced_group)
        
        # === Buttons ===
        button_layout = QHBoxLayout()
        
        self.search_button = QPushButton("Search")
        self.search_button.setIcon(QIcon.fromTheme("search", QIcon()))
        self.search_button.setStyleSheet("font-weight: bold; background-color: #4CAF50; color: white;")
        self.search_button.setMinimumHeight(40)
        
        self.reset_button = QPushButton("Reset Filters")
        self.reset_button.setIcon(QIcon.fromTheme("edit-clear", QIcon()))
        
        button_layout.addWidget(self.reset_button)
        button_layout.addWidget(self.search_button)
        
        # Add everything to the scroll layout
        scroll_layout.addStretch()
        scroll_area.setWidget(scroll_content)
        
        # Add to main layout
        main_layout.addWidget(scroll_area)
        main_layout.addLayout(button_layout)
        
        # Connect signals
        self.search_button.clicked.connect(self.build_search_params)
        self.reset_button.clicked.connect(self.reset_filters)
        
    def reset_filters(self):
        """Reset all filters to default values"""
        self.src_ip_input.clear()
        self.dst_ip_input.clear()
        self.src_port_input.clear()
        self.dst_port_input.clear()
        self.hostname_input.clear()
        self.message_id_input.clear()
        self.protocol_combo.setCurrentIndex(0)
        self.severity_combo.setCurrentIndex(0)
        self.action_combo.setCurrentIndex(0)
        self.message_text_input.clear()
        self.full_text_input.clear()
        
        # Reset advanced options
        self.case_sensitive_check.setChecked(False)
        self.regex_check.setChecked(False)
        self.viz_type_combo.setCurrentIndex(0)
        self.results_limit_combo.setCurrentIndex(2)  # Default to 1000
        
        # Reset date range to Last 24 Hours
        self.date_range_selector.radio_last_24hours.setChecked(True)
        
        # Emit reset signal
        self.resetRequested.emit()
    
    def build_search_params(self):
        """Build search parameters from UI values"""
        params = {
            "time_range": self.date_range_selector.get_date_range(),
            "src_ip": self.src_ip_input.text().strip() or None,
            "dst_ip": self.dst_ip_input.text().strip() or None,
            "src_port": self.src_port_input.text().strip() or None,
            "dst_port": self.dst_port_input.text().strip() or None,
            "hostname": self.hostname_input.text().strip() or None,
            "message_id": self.message_id_input.text().strip() or None,
            "protocol": self.protocol_combo.currentData(),
            "severity": self.severity_combo.currentData(),
            "action": self.action_combo.currentData(),
            "message_text": self.message_text_input.text().strip() or None,
            "full_text": self.full_text_input.text().strip() or None,
            "case_sensitive": self.case_sensitive_check.isChecked(),
            "use_regex": self.regex_check.isChecked(),
            "viz_type": self.viz_type_combo.currentData(),
            "results_limit": self.results_limit_combo.currentData(),
            "count_request": True  # Always include count metrics
        }
        
        # Emit search signal with params
        self.searchRequested.emit(params)
    
    def update_field_options(self, log_data: dict):
        """Update dropdown options based on actual log data"""
        if not log_data:
            return
            
        # Combine all dataframes
        dfs = list(log_data.values())
        if not dfs:
            return
            
        # Create combined df for analysis (just headers, don't need all data)
        try:
            # Get a sample from each DataFrame to avoid memory issues
            samples = [df.head(100) for df in dfs if isinstance(df, pd.DataFrame) and not df.empty]
            if not samples:
                return
                
            combined = pd.concat(samples, ignore_index=True)
            
            # Update Protocol dropdown
            if 'protocol' in combined.columns:
                # Get unique protocols
                protocols = combined['protocol'].dropna().unique()
                # Clear existing items
                self.protocol_combo.clear()
                # Add "Any" option
                self.protocol_combo.addItem("Any", None)
                # Add protocols from data
                for protocol in sorted(protocols):
                    if protocol and str(protocol).strip():
                        self.protocol_combo.addItem(str(protocol), str(protocol))
            
            # Update Action dropdown
            if 'action' in combined.columns:
                # Get unique actions
                actions = combined['action'].dropna().unique()
                # Clear existing items
                self.action_combo.clear()
                # Add "Any" option
                self.action_combo.addItem("Any", None)
                # Add actions from data
                for action in sorted(actions):
                    if action and str(action).strip():
                        self.action_combo.addItem(str(action), str(action))
            
            # Update Severity dropdown
            if 'severity' in combined.columns:
                # Get unique severities
                severities = combined['severity'].dropna().unique()
                # Clear existing items
                self.severity_combo.clear()
                # Add "Any" option
                self.severity_combo.addItem("Any", None)
                # Add severities from data
                for severity in sorted(severities):
                    if severity and str(severity).strip():
                        self.severity_combo.addItem(f"Level {severity}", severity)
            
            # Setup autocompleters for text fields
            for field, input_widget in [
                ('hostname', self.hostname_input),
                ('message_id', self.message_id_input)
            ]:
                if field in combined.columns:
                    # Get unique values, limited to 100
                    values = combined[field].dropna().astype(str).unique()
                    if len(values) > 0:
                        # Create completer
                        completer = QCompleter(sorted(values[:100]))
                        completer.setCaseSensitivity(Qt.CaseInsensitive)
                        input_widget.setCompleter(completer)
            
        except Exception as e:
            print(f"Error updating field options: {e}")
            import traceback
            traceback.print_exc()