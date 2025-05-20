#!/usr/bin/env python3
# perfmon_launcher.py
# 
# A helper script to start, stop, or convert your Windows Performance Monitor collector
# for model-run resource monitoring. Must be run from an elevated (Administrator) Python prompt.

import subprocess
import argparse
import os
import sys
import ctypes


def is_admin():
    """
    Check if the script is running with administrator privileges.
    """
    try:
        return ctypes.windll.shell32.IsUserAnAdmin()
    except:
        return False


def start_perfmon(script_path):
    """
    Invoke the PowerShell script to start the PerfMon collector.

    :param script_path: Path to Setup-TravelModelMonitor.ps1
    """
    if not os.path.isfile(script_path):
        print(f"PowerShell script not found: {script_path}", file=sys.stderr)
        sys.exit(1)
    command = [
        "powershell.exe",
        "-ExecutionPolicy", "Bypass",
        "-File", script_path,
        "-StartImmediately"
    ]
    print(f"Starting PerfMon with script: {script_path}")
    try:
        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Error: {result.stderr}", file=sys.stderr)
            sys.exit(result.returncode)
        print(result.stdout)
    except Exception as e:
        print(f"Failed to start PerfMon: {e}", file=sys.stderr)
        sys.exit(1)


def stop_perfmon(collector_name):
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


def convert_blg_to_csv(script_path):
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


def main():
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
    args = parser.parse_args()

    if args.start:
        start_perfmon(args.script)
    elif args.stop:
        stop_perfmon(args.collector_name)
    elif args.convert:
        convert_blg_to_csv(args.script)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
