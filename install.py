"""
Installation script for EONParser
This script checks if all dependencies are installed and installs missing ones.
"""

import sys
import subprocess
import importlib.util
import os
import platform

# Colors for terminal output
GREEN = '\033[92m'
YELLOW = '\033[93m'
RED = '\033[91m'
RESET = '\033[0m'
BOLD = '\033[1m'

# Required and optional packages
REQUIRED_PACKAGES = [
    'pandas',
    'numpy',
    'plotly',
    'PyQt5',
    'PyQtWebEngine',
    'pytz',
]

OPTIONAL_PACKAGES = [
    ('kaleido', 'Required for exporting visualizations as PNG/JPG'),
    ('reportlab', 'Required for PDF report generation'),
    ('openpyxl', 'Required for Excel export support'),
]

def print_colored(text, color, bold=False):
    """Print colored text to the terminal."""
    if sys.platform == 'win32':
        # Windows doesn't support ANSI color codes in cmd by default
        print(text)
    else:
        if bold:
            print(f"{BOLD}{color}{text}{RESET}")
        else:
            print(f"{color}{text}{RESET}")

def is_package_installed(package_name):
    """Check if a package is installed."""
    try:
        spec = importlib.util.find_spec(package_name)
        return spec is not None
    except ImportError:
        return False

def install_package(package_name):
    """Install a package using pip."""
    try:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', package_name])
        return True
    except subprocess.CalledProcessError:
        return False

def main():
    """Main installation function."""
    print_colored("\nEONParser Installation Script", GREEN, bold=True)
    print_colored("--------------------------------------", GREEN)
    
    # Check Python version
    python_version = platform.python_version()
    print(f"Python version: {python_version}")
    
    if sys.version_info < (3, 7):
        print_colored("Error: Python 3.7 or higher is required.", RED)
        return 1
    
    # Check pip
    try:
        subprocess.check_call([sys.executable, '-m', 'pip', '--version'], 
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print_colored("Pip is available.", GREEN)
    except subprocess.CalledProcessError:
        print_colored("Error: Pip is not available. Please install pip first.", RED)
        return 1
    
    # Check and install required packages
    print_colored("\nChecking required packages:", YELLOW, bold=True)
    missing_required = []
    
    for package in REQUIRED_PACKAGES:
        if is_package_installed(package):
            print(f"✓ {package} is already installed.")
        else:
            print(f"✗ {package} is not installed.")
            missing_required.append(package)
    
    if missing_required:
        print_colored("\nInstalling missing required packages...", YELLOW)
        for package in missing_required:
            print(f"Installing {package}...")
            if install_package(package):
                print_colored(f"✓ {package} installed successfully.", GREEN)
            else:
                print_colored(f"✗ Failed to install {package}.", RED)
                print("Please try to install it manually with:")
                print(f"  pip install {package}")
    else:
        print_colored("\nAll required packages are already installed!", GREEN)
    
    # Check optional packages
    print_colored("\nChecking optional packages:", YELLOW, bold=True)
    missing_optional = []
    
    for package, description in OPTIONAL_PACKAGES:
        if is_package_installed(package):
            print(f"✓ {package} is already installed.")
        else:
            print(f"✗ {package} is not installed. {description}")
            missing_optional.append((package, description))
    
    # Ask to install optional packages
    if missing_optional:
        print_colored("\nWould you like to install missing optional packages? (y/n)", YELLOW)
        choice = input().lower()
        
        if choice.startswith('y'):
            for package, description in missing_optional:
                print(f"Installing {package}...")
                if install_package(package):
                    print_colored(f"✓ {package} installed successfully.", GREEN)
                else:
                    print_colored(f"✗ Failed to install {package}.", RED)
                    print("Please try to install it manually with:")
                    print(f"  pip install {package}")
    
    # Installation complete
    print_colored("\nInstallation process completed!", GREEN, bold=True)
    print_colored("You can now run EONParser using:", GREEN)
    print_colored("  python main.py", YELLOW)
    print_colored("\nEnjoy using EONParser!", GREEN)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())