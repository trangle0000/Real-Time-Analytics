
import subprocess
import time

print("Starting Flask server...")
server = subprocess.Popen(["python", "app.py"])
time.sleep(2)
print("✓ Server started on http://localhost:5000/")
