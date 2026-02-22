
import time
from pipeline.component_framework.component import Component
from pipeline.core.flow.activity import Service

class TimeDelayService(Service):
    """
    Waits for a specified duration in milliseconds.
    """
    def execute(self, data, parent_data):
        delay_ms = data.get_one_of_inputs('delay_ms')

        print("time_delay: ", delay_ms)
        
        if not delay_ms:
            data.set_outputs('message', 'No delay specified')
            return False

        try:
            delay_sec = int(delay_ms) / 1000.0
            if delay_sec < 0:
                 data.set_outputs('message', 'Delay cannot be negative')
                 return False
                 
            print(f"[TimeDelay] Sleeping for {delay_sec} seconds...")
            time.sleep(delay_sec)
            
            data.set_outputs('result', f'Waited {delay_ms} ms')
            return True
                
        except ValueError:
            data.set_outputs('message', 'Invalid delay value (must be integer)')
            return False
        except Exception as e:
            data.set_outputs('message', str(e))
            return False

    def inputs_format(self):
        return [
            self.InputItem(name='Delay (ms)', key='delay_ms', type='int', required=True)
        ]

    def outputs_format(self):
        return [
            self.OutputItem(name='Result', key='result', type='string')
        ]

class TimeDelayComponent(Component):
    name = 'Time Delay'
    code = 'time_delay'
    bound_service = TimeDelayService
    version = '1.0'
    category = 'Standard'
    icon = 'Clock'
    description = 'Wait for a specified amount of time (ms)'
