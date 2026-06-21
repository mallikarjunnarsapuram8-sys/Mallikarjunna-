import os
import sys
from subprocess import Popen, PIPE

print("Testing subprocess with Python executable...")
python_exe = r"C:\Program Files\Python312\python.exe"
print(f"Using Python executable: {python_exe}")

# Check if file exists
if os.path.isfile(python_exe):
    print("File exists")
else:
    print("File does NOT exist")
    sys.exit(1)

# Try to run a simple command
try:
    print("Attempting to run subprocess...")
    proc = Popen([python_exe, "--version"], stdout=PIPE, stderr=PIPE, text=True)
    stdout, stderr = proc.communicate(timeout=10)
    print(f"Return code: {proc.returncode}")
    print(f"Stdout: {stdout}")
    print(f"Stderr: {stderr}")
except Exception as e:
    print(f"Failed to run subprocess: {e}")
    import traceback
    traceback.print_exc()