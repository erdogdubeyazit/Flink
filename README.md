# Flink Text Parser

Simple Text Parser Application with Flink implementation

## Installation

```bash
mvn clean install
```

## Usage
This application can be used either as individual java executable or docker contained. Executable can be used as follows : 

```bash
java -jar SimpleTextParser-0.0.1-SNAPSHOT-jar-with-dependencies.jar --input=<file_to_parse> --output=<output_folder>
```
Docker
```bash
JOBMANAGER_CONTAINER=$(docker ps --filter name=jobmanager --format={{.ID}})
docker cp target/SimpleTextParser-0.0.1-SNAPSHOT-jar "$JOBMANAGER_CONTAINER":/app/text-parser.jar
docker cp case.csv "$JOBMANAGER_CONTAINER":/app/case.csv
docker exec -t -i "$JOBMANAGER_CONTAINER" flink run /app/text-parser.jar --input=<file_to_parse> --output=<output_folder>
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)
