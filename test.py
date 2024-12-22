import threading
import time
import signal
import sys

# Shared flag for stopping threads
stop_threads = False

# Define your functions
def function1():
    while not stop_threads:
        print("Function 1 is running...")
        time.sleep(1)

def function2():
    while not stop_threads:
        print("Function 2 is running...")
        time.sleep(1)

# Signal handler for graceful exit on Ctrl+C
def signal_handler(sig, frame):
    global stop_threads
    print("\nCtrl+C detected! Stopping threads...")
    stop_threads = True
    sys.exit(0)  # Optional, force exit if threads are not responsive

# Attach the signal handler
signal.signal(signal.SIGINT, signal_handler)

# Create and start threads
thread1 = threading.Thread(target=function1)
thread2 = threading.Thread(target=function2)

thread1.start()
thread2.start()

# Keep the main thread alive while threads are running
while thread1.is_alive() or thread2.is_alive():
    time.sleep(0.1)

print("Threads stopped. Exiting program.")
