import typing
import abc

class DataProcessor(abc.ABC):
    def __init__(self):
        self.counter = 0
        self.values = []
    @abc.abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass
    @abc.abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass
    def output(self) -> tuple[int, str]:
        return self.values.pop(0)

class NumericProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if type(data) in (int,float):
            return True
        if type(data) == list:
            for element in data:
                if type(element) in (int,float):
                    pass
                else:
                    return False
            return True    
        else:
            return False

    def ingest(self, data: typing.Any) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        if type(data) == list:
            for element in data:
                self.values.append((self.counter, str(element)))
                self.counter += 1
        else:
            self.values.append((self.counter, str(data)))
            self.counter += 1

class TextProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if type(data) == (str):
            return True
        if type(data) == list:
            for element in data:
                if type(element) == (str):
                    pass
                else:
                    return False
            return True
                
        else:
            return False
        
    def ingest(self, data: typing.Any) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")
        if type(data) == list:
            for element in data:
                self.values.append((self.counter, element))
                self.counter += 1
        else:
            self.values.append((self.counter, data))
            self.counter += 1

class LogProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if type(data) == dict:
            for key, value in data.items():
                if type(key) == (str) and type (value) == (str):
                    pass
                else:
                    return False
            return True
        
        if type(data) == list:
            for element in data:
                if type(element) == dict:
                    for key, value in element.items():
                        if type(key) == (str) and type (value) == (str):
                            pass
                        else:
                            return False
                else:
                    return False
            return True      
        else:
            return False
    def ingest(self, data: typing.Any) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")
        if type(data) == list:
            for element in data:
                log = f"{element['log_level']}: {element['log_message']}"
                self.values.append((self.counter, log))
                self.counter += 1
        else:
            log = f"{data['log_level']}: {data['log_message']}"
            self.values.append((self.counter, log))
            self.counter += 1


class DataStream():
    def __init__(self):
        self.processor = []
    def register_processor(self, proc: DataProcessor) -> None:
        self.processor.append(proc)
    def process_stream(self, stream: list[typing.Any]) -> None:
        for element in stream:
            flag = False
            for process in self.processor:

                if process.validate(element) == True:
                    process.ingest(element)
                    flag = True
                    break
            if flag == False:
                print(f"DataStream error - Can't process element in stream: {element}")
    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self.processor:
            print("No processor found, no data")
            return
        for element in self.processor:
            name = element.__class__.__name__
            number = element.counter
            remainder = len(element.values)
            print(f"{name}: total {number} items processed, remaining {remainder} on processor")


def main():
    print("=== Code Nexus - Data Stream ===")
    print()
    print("Initialize Data Stream...")
    stream = DataStream()
    stream.print_processors_stats()
    print()
    print("Registering Numeric Processor")
    numeric = NumericProcessor()
    stream.register_processor(numeric)
    test = ['Hello world', [3.14, -1, 2.71], [{'log_level': 'WARNING', 'log_message': 'Telnet access! Use ssh instead'},{'log_level': 'INFO', 'log_message': 'User wil isconnected'}], 42, ['Hi', 'five']]
    print()
    print(f"Send first batch of data on stream: {test}")
    stream.process_stream(test)
    stream.print_processors_stats()
    