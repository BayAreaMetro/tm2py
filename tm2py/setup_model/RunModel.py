""" This is the main run model script for TM2.

It gets copied into the model run directory as part of the setup_model process.
"""
import datetime
import pathlib
import random
import subprocess
import sys
import traceback
import tm2py
import toml

def notify_slack(message, extra_info=None):
    """Send notification to Slack using the notify_slack.py script
    """
    try:
        # Check if Slack notifications are enabled in config
        config_file = pathlib.Path("scenario_config.toml")
        if config_file.exists():
            with open(config_file, "r", encoding="utf-8") as f:
                config = toml.load(f)
            slack_enabled = config.get("slack_notifications", {}).get("enabled", False)
        else:
            slack_enabled = False
        
        if not slack_enabled:
            print(f"Slack notifications disabled. Message: {message}")
            return
        
        # Get the path to the tm2py scripts directory
        tm2py_path = pathlib.Path(tm2py.__file__).parent.parent
        notify_script = tm2py_path / "scripts" / "notify_slack.py"
        
        # Check if the notify script exists
        if not notify_script.exists():
            print(f"Slack notification script not found at {notify_script}. Message: {message}")
            return
        
        # Build enhanced message with extra info
        enhanced_message = message
        if extra_info:
            enhanced_message += f"\n{extra_info}"
        
        # Run the notification script
        subprocess.run([sys.executable, str(notify_script), enhanced_message], 
                      check=True, capture_output=True, text=True)
        print(f"Slack notification sent: {enhanced_message}")
    except Exception as e:
        print(f"Failed to send Slack notification: {e}")
        print(f"Message was: {message}")

def main() -> int:
    """ Basical run model script method.

    Returns success code (0 if true)
    """
    run_successful = False
    error_message = ""
    start_time = datetime.datetime.now()
    
    # Get current directory and scenario info for context
    current_dir = pathlib.Path(".").resolve()
    
    # Try to get scenario name from config
    scenario_name = "Unknown Scenario"
    scenario_year = "Unknown Year"
    try:
        config_file = pathlib.Path("scenario_config.toml")
        if config_file.exists():
            with open(config_file, "r", encoding="utf-8") as f:
                config = toml.load(f)
            scenario_name = config.get("scenario", {}).get("name", "Unknown Scenario")
            scenario_year = config.get("scenario", {}).get("year", "Unknown Year")
    except Exception:
        pass
    
    # Build enhanced start notification with run configuration
    start_info = f"📍 Directory: {current_dir}\n🏷️ Scenario: {scenario_name} ({scenario_year})\n⏰ Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}"
    
    # Add iteration info if available in config
    try:
        start_iteration = config.get("run", {}).get("start_iteration", 0)
        end_iteration = config.get("run", {}).get("end_iteration", 1)
        if start_iteration is not None and end_iteration is not None:
            start_info += f"\n🔄 Iterations: {start_iteration} to {end_iteration}"
    except Exception:
        pass
    
    # Send start notification
    notify_slack(f"🚀 Travel Model Two run starting", start_info)
    
    try:
        controller = tm2py.RunController(
            config_file = ["scenario_config.toml", "model_config.toml"],
            run_dir = pathlib.Path(".")
        )
        controller.run()
        run_successful = True
        
    except Exception as e:
        error_message = str(e)
        print(f"Model run failed with error: {error_message}")
        traceback.print_exc()
    
    # Calculate runtime
    end_time = datetime.datetime.now()
    runtime = end_time - start_time
    runtime_str = str(runtime).split('.')[0]  # Remove microseconds
    
    # Send Slack notification based on run status
    if run_successful:
        rewards = [
            "tiramisu",
            "a long run",
            "bunny pets",
            "a nap",
            "dancing parrot",
            "well-constructed gluten-free vegan cake",
            "a pat on the back from Dave Vautin"
        ]
        reward = random.choice(rewards)
        
        success_info = f"🏷️ Scenario: {scenario_name} ({scenario_year})\n⏱️ Runtime: {runtime_str}\n📍 Directory: {current_dir}\n⏰ Completed: {end_time.strftime('%Y-%m-%d %H:%M:%S')}"
        
        # Try to add summary results if topsheet exists
        try:
            topsheet_path = current_dir / "output_summaries" / "topsheet.csv"
            if topsheet_path.exists():
                import pandas as pd
                df = pd.read_csv(topsheet_path)
                # Look for key metrics
                vmt_row = df[df['Metric'].str.contains('Total Daily VMT', case=False, na=False)]
                if not vmt_row.empty:
                    vmt_value = vmt_row.iloc[0]['Value']
                    if isinstance(vmt_value, (int, float)):
                        vmt_millions = vmt_value / 1_000_000
                        success_info += f"\n📊 Total Daily VMT: {vmt_millions:.1f}M"
                        
                        # Check for truck VMT split
                        car_vmt_row = df[df['Metric'].str.contains('Total Daily Car VMT', case=False, na=False)]
                        truck_vmt_row = df[df['Metric'].str.contains('Total Daily Truck VMT', case=False, na=False)]
                        if not car_vmt_row.empty and not truck_vmt_row.empty:
                            car_vmt = car_vmt_row.iloc[0]['Value'] / 1_000_000
                            truck_vmt = truck_vmt_row.iloc[0]['Value'] / 1_000_000
                            truck_pct = (truck_vmt / vmt_millions * 100) if vmt_millions > 0 else 0
                            success_info += f"\n🚗 Car VMT: {car_vmt:.1f}M | 🚛 Truck VMT: {truck_vmt:.1f}M ({truck_pct:.1f}%)"
        except Exception:
            # Don't fail notification if we can't read results
            pass
        
        notify_slack(f"✅ Travel Model Two run completed successfully! Go get {reward}", success_info)
    else:
        # Random motivating failure messages
        motivating_messages = [
            "They say failure is part of the process in engineering. If that's true, I must be crushing the process.",
            "Every model run teaches us something new. This one taught us patience.",
            "Debugging is like being the detective in a crime movie where you are also the murderer.",
            "Error messages are just the model's way of asking for help.",
            "Rome wasn't built in a day, and neither was a perfect travel model.",
            "This isn't a failure, it's a learning opportunity with attitude.",
            "Even the best models need a timeout sometimes.",
            "Consider this a feature request from reality.",
            "The model is just taking a creative approach to problem-solving.",
            "Sometimes the journey is more important than the destination... but not today.",
            "Think of it as aggressive testing of error handling systems.",
            "The model is practicing mindfulness by stopping to reflect.",
            "Failure is success in progress... very, very slow progress.",
            "This is just the model's way of saying it needs more coffee.",
            "Error: Task failed successfully (at failing).",
            "The model decided to take the scenic route through Errorville.",
            "It's not a bug, it's an undocumented feature of disappointment.",
            "The model is just expressing its artistic side through creative failure.",
            "Congratulations! You've discovered a new way for things to go wrong.",
            "The model is conducting an impromptu stress test on your patience.",
            "Error messages are like fortune cookies, but less helpful.",
            "The model is just really committed to the whole 'fail fast' philosophy.",
            "This failure brought to you by the department of unexpected plot twists.",
            "The model decided to practice interpretive dance instead of running.",
            "At least the model is consistent... consistently surprising.",
            "The model is taking a mental health day.",
            "This is what happens when models try to think outside the box.",
            "The model is just showing off its extensive vocabulary of error codes.",
            "Failure is the spice of life, and this one is extra spicy.",
            "The model is auditioning for a role in a tragedy instead of a success story."
        ]
        motivating_message = random.choice(motivating_messages)
        
        failure_info = f"🏷️ Scenario: {scenario_name} ({scenario_year})\n⏱️ Runtime: {runtime_str}\n📍 Directory: {current_dir}\n❌ Error: {error_message}\n⏰ Failed: {end_time.strftime('%Y-%m-%d %H:%M:%S')}"
        
        notify_slack(f"❌ Travel Model Two run failed", failure_info + f"\n\n💭 {motivating_message}")
    
    return 0 if run_successful else 1

if __name__ == "__main__":
    # Exit with appropriate code
    sys.exit(main())