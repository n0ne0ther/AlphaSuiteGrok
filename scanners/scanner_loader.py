"""
A utility for dynamically discovering and loading scanner modules.

This module allows the framework to be plug-and-play for scanners. To add a new
scanner, simply create a new Python file in the `scanners` directory that
contains a class inheriting from `BaseScanner`. This loader will automatically
find and register it.
"""

import os
import importlib
import inspect
import pkgutil

from scanners.scanner_sdk import BaseScanner

def get_scanner_class_map():
    """
    Dynamically discovers and returns a map of all available scanner classes.
    It scans the 'scanners' directory for modules containing BaseScanner subclasses.
    """
    scanner_map = {}
    # Correctly locate the 'scanners' directory relative to this file's location
    scanners_path = os.path.dirname(os.path.abspath(__file__))
    
    for _, name, _ in pkgutil.iter_modules([scanners_path]):
        try:
            # The module name is just the filename without .py
            module = importlib.import_module(f'scanners.{name}')
            for _, obj in inspect.getmembers(module, inspect.isclass):
                if issubclass(obj, BaseScanner) and obj is not BaseScanner:
                    # The key is the snake_case module name, e.g., 'strongest_industries'
                    scanner_map[name] = obj
        except Exception as e:
            print(f"Could not load scanner from {name}: {e}")
            
    return scanner_map

SCANNER_CLASS_MAP = get_scanner_class_map()

def load_scanner_class(scanner_name: str):
    """Loads a scanner class by its snake_case name."""
    return SCANNER_CLASS_MAP.get(scanner_name)