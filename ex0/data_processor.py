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


def main():
    print("=== Code Nexus - Data Processor ===")
    print()
    print("Testing Numeric Processor...")
    num_proc = NumericProcessor()
    test = [42, "Hello"]
    for element in test:
        result = num_proc.validate(element)
        print(f"Trying to validate input '{element}': {result}")
    try:
        print(f"Test invalid ingestion of string 'foo' without prior validation:")
        num_proc.ingest("foo")
    except ValueError as e:
        print(f"Got exception: {e}")
    test3 = [1, 2, 3, 4, 5]
    num_proc.ingest(test3)
    print(f"Processing data: {test3}")
    print("Extracting 3 values...")
    i = 0
    while i < 3:
        result =num_proc.output()[1]
        print(f"Numeric value {i}: {result}")
        i += 1
    print()
    print("Testing Text Processor...")
    text_proc = TextProcessor()
    result = text_proc.validate(42)
    print(f"Trying to validate input '42': {result}")
    string_list = ["Hello", "Nexus", "World"]
    text_proc.ingest(string_list)
    print(f"Processing data: {string_list}")
    print("Extracting 1 value...")
    result = text_proc.output()[1]
    print(f"Text value 0: {result}")
    print()
    print("Testing Log Processor...")
    log_proc = LogProcessor()
    result = log_proc.validate("Hello")
    print(f"Trying to validate input 'Hello': {result}")
    log_test =  [{'log_level': 'NOTICE', 'log_message': 'Connection to server'},
                {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}]
    log_proc.ingest(log_test)
    print(f"Processing data: {log_test}")
    j = 0
    while j < 2:
        result = log_proc.output()[1]
        print(f"Log entry {j}: {result}")
        j += 1


if __name__ == "__main__":
    main()