import sys
import subprocess
try:
    import akshare
    print("akshare is installed")
except ImportError:
    print("akshare is not installed")
print("Python executable:", sys.executable)
print("Python version:", sys.version)
