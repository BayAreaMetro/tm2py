#!/usr/bin/env python3
# perfmon_launcher.py
# 
# A helper script to start, stop, or convert your Windows Performance Monitor collector
# for model-run resource monitoring, using ShellExecute to elevate.

import subprocess
import argparse
import os
import sys
import ctypes
from typing import List, Optional


def is_admin() -> bool:
    """
    Check if the script is running with administrator privileges.
    """
    try:
        return ctypes.windll.shell32.IsUserAnAdmin() != 0
    except Exception:
        return False


def start_perfmon(script_path: str, additional_args: Optional[List[str]] = None) -> None:
    """
    Start the PerfMon collector, elevating via ShellExecute (UAC prompt).

    :param script_path: Path to Setup-TravelModelMonitor.ps1
    :param additional_args: Optional list of extra flags to pass to PowerShell script
    """
    if additional_args is None:
        additional_args = []
    if not os.path.isfile(script_path):
        print(f"PowerShell script not found: {script_path}", file=sys.stderr)
        sys.exit(1)

    # Build PowerShell arguments
    ps_args = f'-ExecutionPolicy Bypass -File "{script_path}" -StartImmediately'
    if additional_args:
        ps_args += ' ' + ' '.join(additional_args)

    # Use ShellExecute to elevate
    try:
        result = ctypes.windll.shell32.ShellExecuteW(
            None,                # hwnd
            "runas",            # operation
            "powershell.exe",   # file to execute
            ps_args,             # parameters
            None,                # directory
            1                    # SW_SHOWNORMAL
        )
        if result <= 32:
            print(f"Elevation failed with code: {result}", file=sys.stderr)
            sys.exit(1)
    except Exception as e:
        print(f"Failed to elevate PerfMon: {e}", file=sys.stderr)
        sys.exit(1)


def stop_perfmon(collector_name: str) -> None:
    """
    Stop the running PerfMon collector using logman.

    :param collector_name: Name of the collector set to stop
    """
    command = [
        "powershell.exe",
        "-Command", f"logman stop {collector_name}"
    ]
    print(f"Stopping PerfMon collector: {collector_name}")
    try:
        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Error stopping collector: {result.stderr}", file=sys.stderr)
            sys.exit(result.returncode)
        print(result.stdout)
    except Exception as e:
        print(f"Failed to stop PerfMon: {e}", file=sys.stderr)
        sys.exit(1)


def convert_blg_to_csv(script_path: str) -> None:
    """
    Convert the existing BLG log to CSV via the PowerShell script.

    :param script_path: Path to Setup-TravelModelMonitor.ps1
    """
    if not os.path.isfile(script_path):
        print(f"PowerShell script not found: {script_path}", file=sys.stderr)
        sys.exit(1)
    command = [
        "powershell.exe",
        "-ExecutionPolicy", "Bypass",
        "-File", script_path,
        "-ConvertOnly"
    ]
    print("Converting BLG to CSV...")
    try:
        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Error converting: {result.stderr}", file=sys.stderr)
            sys.exit(result.returncode)
        print(result.stdout)
    except Exception as e:
        print(f"Failed conversion: {e}", file=sys.stderr)
        sys.exit(1)


def main() -> None:
    if not is_admin():
        sys.stderr.write("This script must be run as Administrator.\n")
        sys.exit(1)

    parser = argparse.ArgumentParser(
        description="Launch, stop, or convert Windows Performance Monitor for model runs"
    )
    parser.add_argument(
        "--script", "-s", required=True,
        help="Path to Setup-TravelModelMonitor.ps1"
    )
    parser.add_argument(
        "--start", action="store_true",
        help="Start the PerfMon collector"
    )
    parser.add_argument(
        "--stop", action="store_true",
        help="Stop the PerfMon collector"
    )
    parser.add_argument(
        "--convert", action="store_true",
        help="Convert BLG to CSV via the PS script"
    )
    parser.add_argument(
        "--collector-name", "-n", default="TravelModelMonitor",
        help="Collector set name (default: TravelModelMonitor)"
    )
    parser.add_argument(
        "--additional-args", nargs="*", default=[],
        help="Additional arguments to pass to the PowerShell script"
    )
    args = parser.parse_args()

    if args.start:
        start_perfmon(args.script, additional_args=args.additional_args)
    elif args.stop:
        stop_perfmon(args.collector_name)
    elif args.convert:
        convert_blg_to_csv(args.script)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()


