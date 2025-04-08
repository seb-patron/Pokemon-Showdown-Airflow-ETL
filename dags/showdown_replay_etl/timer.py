"""
Utility for timing and logging the runtime of processes.

This module provides a decorator and context manager for measuring
the execution time of specific processes in the ETL pipeline.
"""
import time
import logging
import functools
import os
from contextlib import contextmanager
from typing import Optional, Callable, Any

# Global settings
# This can be set via environment variable or at runtime
ENABLE_DETAILED_TIMING = os.environ.get("ENABLE_DETAILED_TIMING", "false").lower() == "true"

logger = logging.getLogger(__name__)

def enable_detailed_timing(enabled: bool = True) -> None:
    """
    Enable or disable detailed timing globally.
    
    Args:
        enabled: Whether to enable detailed timing
    """
    global ENABLE_DETAILED_TIMING
    ENABLE_DETAILED_TIMING = enabled
    logger.info(f"Detailed process timing {'enabled' if enabled else 'disabled'}")

def is_timing_enabled() -> bool:
    """Check if detailed timing is enabled."""
    return ENABLE_DETAILED_TIMING

@contextmanager
def time_process(process_name: str, log_level: int = logging.INFO):
    """
    Context manager to time a process and log its duration.
    
    Args:
        process_name: Name of the process to measure
        log_level: Logging level to use for the timing message
    
    Example:
        with time_process("Loading replay data"):
            data = load_replay_data()
    """
    if not ENABLE_DETAILED_TIMING:
        yield
        return
    
    start_time = time.time()
    try:
        yield
    finally:
        end_time = time.time()
        duration = end_time - start_time
        logger.log(log_level, f"TIMING: {process_name} took {duration:.3f} seconds")

def timed(func: Optional[Callable] = None, process_name: Optional[str] = None):
    """
    Decorator to time a function and log its duration.
    
    Args:
        func: The function to time
        process_name: Optional name for the process, defaults to function name
    
    Example:
        @timed
        def process_data():
            # ...
            
        @timed(process_name="Custom process name")
        def another_function():
            # ...
    """
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            if not ENABLE_DETAILED_TIMING:
                return f(*args, **kwargs)
                
            proc_name = process_name or f.__name__
            start_time = time.time()
            result = f(*args, **kwargs)
            end_time = time.time()
            duration = end_time - start_time
            logger.info(f"TIMING: {proc_name} took {duration:.3f} seconds")
            return result
        return wrapper
    
    # Handle both @timed and @timed(process_name="...")
    if func is None:
        return decorator
    return decorator(func)

def time_section(section_name: str, log_level: int = logging.INFO) -> Callable[[], None]:
    """
    Start timing a section of code and return a function to end and log the timing.
    
    Args:
        section_name: Name of the section to time
        log_level: Logging level to use for the timing message
        
    Returns:
        A function that when called will end the timing and log the result
        
    Example:
        end_timing = time_section("Processing batch of files")
        # ... do work ...
        end_timing()  # Logs the timing result
    """
    if not ENABLE_DETAILED_TIMING:
        return lambda: None
        
    start_time = time.time()
    
    def end_timing():
        end_time = time.time()
        duration = end_time - start_time
        logger.log(log_level, f"TIMING: {section_name} took {duration:.3f} seconds")
    
    return end_timing 