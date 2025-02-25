import datetime
import os
import json

# ScriptLogger class to log script runs and errors
# run logs are grouped by script name and month
# error logs are grouped by month
class ScriptLogger:
    def __init__(self, script_name):
        self.script_name = script_name
        self.start_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        current_month = datetime.datetime.now().strftime('%Y_%m')
        
        # Load log_directory from config.json
        base_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = os.path.join(base_directory, 'config.json')
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)
            
        try:
            log_directory = os.path.join(base_directory, config['log_directory'])
        except KeyError:
            raise ValueError("Missing required 'log_directory' setting in config.json")

        # Create log directories if they don't exist
        directories = [
            os.path.join(log_directory, 'run_logs'),
            os.path.join(log_directory, 'error_logs'),
            os.path.join(log_directory, 'run_logs', script_name)
        ]
        
        for directory in directories:
            if not os.path.exists(directory):
                os.makedirs(directory)
        
        self.run_log_file = f'{log_directory}/run_logs/{script_name}/{script_name}_{current_month}_runs.log'
        self.error_log_file = f'{log_directory}/error_logs/{current_month}_errors.log'

        # Create new line in run log file
        with open(self.run_log_file, 'a', newline='\n') as file:
            file.write(f'\nSTART: {self.start_time} - UNKOWN ERROR')
    
    # Update the last line of the run log file
    def update_last_line(self, new_text):
        with open(self.run_log_file, 'rb+', newline='\n') as file:
            file.seek(0, os.SEEK_END)
            file_size = file.tell()
            buffer = bytearray()
            while file_size > 0:
                file_size -= 1
                file.seek(file_size)
                char = file.read(1)
                if char == b'\n':
                    if buffer:
                        break
                buffer.extend(char)
            buffer[::-1].decode()
            file.seek(file_size)
            file.write(new_text.encode())
    
    def error(self, error_message, exception=""):
        error_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.update_last_line(f'START: {self.start_time} - ERROR {error_message}: {error_time}')
        log_message = f'\n{error_time} - {self.script_name} - {error_message}\n'
        if exception:
            log_message += f'\n{exception}\n'
        with open(self.error_log_file, 'a', newline='\n') as file:
            file.write(f'\n{log_message}')
    
    def end(self, message=""):
        end_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_message = f'START: {self.start_time} - SUCCESS: {end_time}'
        if message:
            log_message += f' - {message}'
        self.update_last_line(log_message)

# Example usage
if __name__ == "__main__":
    log = ScriptLogger("example_script")
    try:
        raise Exception("Test Exception")
    except Exception as e:
        log.error("Test Error", e)
        
    log.end()
