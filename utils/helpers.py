import subprocess
from pathlib import Path


def execute_command(cmd):
    """
    Execute a shell command and stream output to logs

    Args:
        cmd: Command to execute (list of strings)

    Returns:
        Tuple of (success, output_lines)
    """
    # Print command for logging
    print(f"Executing: {' '.join(cmd)}")

    # Run command and stream output
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,  # Line buffered for real-time output
    )

    # Stream output to logs
    output_lines = []
    for line in iter(process.stdout.readline, ""):
        print(line, end="")
        output_lines.append(line.rstrip())

    # Wait for process to complete
    process.wait()

    # Return success/failure and output
    return process.returncode == 0, output_lines


def run_dbt_command(command, project_dir=None, profiles_dir=None, target="dev"):
    """
    Simple function to execute a dbt command with automatic deps installation

    Args:
        command: dbt command to run (e.g., 'run', 'test')
        project_dir: dbt project directory (Path or string)
        profiles_dir: dbt profiles directory (Path or string)
        target: dbt target environment

    Returns:
        True if command succeeded, False otherwise
    """
    # Default directories
    project_dir = Path(__file__).parent.parent if not project_dir else Path(project_dir)
    profiles_dir = project_dir if not profiles_dir else Path(profiles_dir)

    # Common command arguments
    base_args = [
        "--no-use-colors",
        "--profiles-dir",
        str(profiles_dir),
        "--project-dir",
        str(project_dir),
        "--target",
        target,
    ]

    # Run dbt deps first (unless the command is already deps)
    if command != "deps":
        print("Installing dbt dependencies...")
        deps_cmd = ["dbt", "deps"] + base_args
        success, _ = execute_command(deps_cmd)

        if not success:
            print("Failed to install dbt dependencies")
            return False

    # Run the requested command
    cmd = ["dbt", command] + base_args
    success, _ = execute_command(cmd)

    return success
