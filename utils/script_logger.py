import datetime
import os
import json

class ScriptLogger:
    def __init__(self, script_name):
        self.script_name = script_name
        self.start_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        current_month = datetime.datetime.now().strftime('%Y_%m')
        
        # Load log_directory from config.json
        base_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        print("base dir:", base_directory)
        config_path = os.path.join(base_directory, 'config.json')
        print("config path:", config_path)
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)
            
        try:
            log_directory = os.path.join(base_directory, config['log_directory'])
        except KeyError:
            raise ValueError("Missing required 'log_directory' setting in config.json")
        print("log dir:",log_directory)

        # Create log directories if they don't exist
        directories = [
            os.path.join(log_directory, 'run_logs'),
            os.path.join(log_directory, 'error_logs'),
            os.path.join(log_directory, 'run_logs', script_name),
            os.path.join(log_directory, 'error_logs', script_name)
        ]
        
        for directory in directories:
            print("directory:", directory)
            if not os.path.exists(directory):
                os.makedirs(directory)
        
        self.run_log_file = f'{log_directory}/run_logs/{script_name}/{script_name}_{current_month}_runs.log'
        self.error_log_file = f'{log_directory}/error_logs/{script_name}/{script_name}_{current_month}_errors.log'

        # Create new line in run log file
        with open(self.run_log_file, 'a') as file:
            file.write(f'\nSTART: {self.start_time}')
    
    # Update the last line of the run log file
    def update_last_line(self, new_text):
        with open(self.run_log_file, 'rb+') as file:
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
    
    def error(self, error_name, error_details=""):
        error_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.update_last_line(f'START: {self.start_time} - ERROR {error_name}: {error_time}\n')
    
    def end(self, message=""):
        end_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.update_last_line(f'START: {self.start_time} - SUCCESS: {end_time}')

# Example usage
if __name__ == "__main__":
    log = ScriptLogger("example_script")
    try:
        # Your script logic here
        pass
    except Exception as e:
        log.error(str(e))
        
    log.end()
