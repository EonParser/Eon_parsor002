"""
EONParser - Advanced Log Analysis Tool with Traditional Search

This is the main entry point for the EONParser application. It launches the
graphical user interface for analyzing, visualizing, and reporting on log data.
"""

import sys
import os
import traceback
from PyQt5.QtWidgets import QApplication, QMessageBox, QSplashScreen
from PyQt5.QtGui import QPixmap, QFont
from PyQt5.QtCore import Qt, QTimer

# Import the main GUI class
from gui import EONParserGUI

def show_error(message):
    """Display an error message to the user."""
    print(f"ERROR: {message}")
    try:
        app = QApplication.instance() or QApplication(sys.argv)
        QMessageBox.critical(None, "EONParser Error", str(message))
    except Exception:
        pass  # If GUI fails, at least we printed the error

def check_dependencies():
    """Check if required dependencies are installed."""
    try:
        import pandas
        import plotly
        import pytz
        return True
    except ImportError as e:
        show_error(f"Missing required dependency: {e}\n\n"
                  f"Please install the required packages using:\n"
                  f"pip install -r requirements.txt")
        return False

def main():
    """Main entry point for the application."""
    # Set high DPI scaling attribute before creating QApplication
    if hasattr(Qt, 'AA_EnableHighDpiScaling'):
        QApplication.setAttribute(Qt.AA_EnableHighDpiScaling, True)
    if hasattr(Qt, 'AA_UseHighDpiPixmaps'):
        QApplication.setAttribute(Qt.AA_UseHighDpiPixmaps, True)
    
    # Create application
    app = QApplication(sys.argv)
    app.setApplicationName("EONParser")
    app.setApplicationVersion("2.0.0")
    
    # Check for dependencies
    if not check_dependencies():
        return 1
    
    # Create and show splash screen
    splash_pixmap = QPixmap(400, 300)
    splash_pixmap.fill(Qt.white)
    splash = QSplashScreen(splash_pixmap, Qt.WindowStaysOnTopHint)
    
    # Add text to splash screen
    splash.showMessage("Starting EONParser...\nInitializing components...", 
                     Qt.AlignCenter | Qt.AlignBottom, Qt.black)
    font = splash.font()
    font.setPointSize(12)
    splash.setFont(font)
    splash.show()
    app.processEvents()
    
    # Delay for splash screen visibility (500ms)
    QTimer.singleShot(500, lambda: None)
    app.processEvents()
    
    try:
        # Update splash message
        splash.showMessage("Loading user interface...", 
                         Qt.AlignCenter | Qt.AlignBottom, Qt.black)
        app.processEvents()
        
        # Create main window
        main_window = EONParserGUI()
        
        # Update splash message
        splash.showMessage("Ready!", 
                         Qt.AlignCenter | Qt.AlignBottom, Qt.black)
        app.processEvents()
        
        # Show main window and close splash screen
        main_window.show()
        splash.finish(main_window)
        
        # Start event loop
        return app.exec_()
        
    except Exception as e:
        # Handle any unhandled exceptions at the application level
        splash.close()
        error_msg = f"An error occurred while starting the application:\n{str(e)}\n\n{traceback.format_exc()}"
        show_error(error_msg)
        return 1

if __name__ == "__main__":
    sys.exit(main())