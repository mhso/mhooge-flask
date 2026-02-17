import sys
import subprocess
import signal
import inspect
from time import sleep

def restartable(func):
    file_to_restart = inspect.getfile(func)

    def wrapper():
        if len(sys.argv) > 1 and "--restartable" in sys.argv:
            # If '--restartable' is given as an argument, run a master process
            # that starts the actual program in a child process and monitors it.
            # If the child process exits with return code 2, restart it.

            try:
                while True:
                    # Start child process that runs the actual program.
                    without_name = sys.argv[1:]
                    without_name.remove("--restartable")
                    process = subprocess.Popen([sys.executable, file_to_restart, *without_name])

                    while process.poll() is None: # Wait for child process to exit.
                        sleep(0.1)

                    if process.returncode == 2: # Process requested a restart.
                        sleep(1) # Sleep for a sec and restart while loop.
                        print(f"++++++ Restarting {file_to_restart} ++++++")
                    else:
                        break # Process exited naturally, terminate program.

            except KeyboardInterrupt:
                # We have to handle interrupt signal differently on Linux vs. Windows.
                if sys.platform == "linux":
                    sig = signal.SIGINT
                else:
                    sig = signal.CTRL_C_EVENT

                # End child process and terminate program.
                process.send_signal(sig)

                # Wait for child process to exit properly.
                try:
                    while process.poll() is None:
                        sleep(0.1)
                except KeyboardInterrupt:
                    return

        else:
            # This runs the actual program. It runs in a subprocess
            # if '--restartable' is given as an argument on the CLI.
            func()

    return wrapper
