import typing
import abc


class DataProcessor(abc.ABC):
    def __init__(self) -> None:
        self.counter = 0
        self.values: list[tuple[int, str]] = []

    @abc.abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abc.abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        return self.values.pop(0)


class NumericProcessor(DataProcessor):
    def validate(self, data: int | float | list[int | float]) -> bool:
        if type(data) in (int, float):
            return True
        if type(data) is list:
            for element in data:
                if type(element) in (int, float):
                    pass
                else:
                    return False
            return True
        else:
            return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        if type(data) is list:
            for element in data:
                self.values.append((self.counter, str(element)))
                self.counter += 1
        else:
            self.values.append((self.counter, str(data)))
            self.counter += 1


class TextProcessor(DataProcessor):
    def validate(self, data: str | list[str]) -> bool:
        if type(data) is (str):
            return True
        if type(data) is list:
            for element in data:
                if element and type(element) is (str):
                    pass
                else:
                    return False
            return True

        else:
            return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")
        if type(data) is list:
            for element in data:
                self.values.append((self.counter, element))
                self.counter += 1
        else:
            self.values.append((self.counter, str(data)))
            self.counter += 1


class LogProcessor(DataProcessor):
    def validate(self, data: dict[str, str] | list[dict[str, str]]) -> bool:
        if type(data) is dict:
            for key, value in data.items():
                if (
                    isinstance(key, str)
                    and isinstance(value, str)
                    and key.strip() and value.strip()
                ):
                    pass
                else:
                    return False
            return True

        if type(data) is list:
            for element in data:
                if type(element) is dict:
                    for key, value in element.items():
                        if (
                            isinstance(key, str)
                            and isinstance(value, str)
                            and key.strip() and value.strip()
                        ):
                            pass
                        else:
                            return False
                else:
                    return False
            return True
        else:
            return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")
        if isinstance(data, list):
            for element in data:
                if isinstance(element, dict):
                    log = f"{element['log_level']}: {element['log_message']}"
                    self.values.append((self.counter, log))
                    self.counter += 1
        elif isinstance(data, dict):
            log = f"{data['log_level']}: {data['log_message']}"
            self.values.append((self.counter, log))
            self.counter += 1


class ExportPlugin(typing.Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass


class CSV:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        if not data:
            return
        print("CSV Output:")
        length = len(data)
        i = 0
        text = ""
        for element in data:
            i += 1
            text += element[1]
            if i != length:
                text += ","
        print(text)


class JSON:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        if not data:
            return
        print("JSON Output:")
        length = len(data)
        i = 0
        text = ""
        for element in data:
            i += 1
            text += (f'"item_{element[0]}": "{element[1]}"')
            if i != length:
                text += ", "
        print("{", end="")
        print(f"{text}", end="")
        print("}")


class DataStream():
    def __init__(self) -> None:
        self.processor: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processor.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
        for element in stream:
            flag = False
            for process in self.processor:
                if process.validate(element) is True:
                    process.ingest(element)
                    flag = True
                    break
            if flag is False:
                print(f"DataStream error - Can't process element in stream: "
                      f"{element}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self.processor:
            print("No processor found, no data")
            return
        for element in self.processor:
            name = element.__class__.__name__
            if name == "NumericProcessor":
                name = "Numeric Processor"
            elif name == "TextProcessor":
                name = "Text Processor"
            elif name == "LogProcessor":
                name = "Log Processor"
            number = element.counter
            remainder = len(element.values)
            print(f"{name}: total {number} items processed, remaining "
                  f"{remainder} on processor")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:

        for element in self.processor:
            data_output = []
            i = 0
            length = len(element.values)
            while i < nb and length > 0:
                processor_value = element.output()
                data_output.append(processor_value)
                i += 1
                length -= 1
            if data_output != []:
                plugin.process_output(data_output)


def main() -> None:
    print("=== Code Nexus - Data Pipeline ===")
    print()
    print("Initialize Data Stream...")
    print()
    example = DataStream()
    example.print_processors_stats()
    print()
    print("Registering Processors")
    print()
    example.register_processor(NumericProcessor())
    example.register_processor(TextProcessor())
    example.register_processor(LogProcessor())
    try:
        text1 = ['Hello world', [3.14, -1, 2.71],
                 [{'log_level': 'WARNING', 'log_message': 'Telnet access! Use'
                  ' ssh instead'}, {'log_level': 'INFO', 'log_message': 'User'
                  ' will is connected'}], 42, ['Hi', 'five']]
        print(f"Send first batch of data on stream: {text1}")
        print()
        example.process_stream(text1)
        example.print_processors_stats()
        print()
        print("Send 3 processed data from each processor to a CSV plugin:")
        csv = CSV()
        example.output_pipeline(3, csv)
        print()
        example.print_processors_stats()
        print()
        text2 = [21, ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
                 [{'log_level': 'ERROR', 'log_message': '500 server crash'},
                 {'log_level': 'NOTICE', 'log_message': 'Certificate expires'
                  ' in 10 days'}], [32, 42, 64, 84, 128, 168], 'World hello']
        print(f"Send another batch of data: {text2}")
        print()
        example.process_stream(text2)
        example.print_processors_stats()
        print()
        print("Send 5 processed data from each processor to a JSON plugin:")
        json = JSON()
        example.output_pipeline(5, json)
        print()
        example.print_processors_stats()
    except (ValueError, IndexError, KeyError) as e:
        print(f"Got exception: {e}")


if __name__ == "__main__":
    main()
