## Synopsis

This is a simple project that shows an implementation of ETL with spring batch. The ETL is done from csv file to H2 in-memory database.
The csv represent watchlist with entries that should be loaded to DB with timestamp for current job and randomly chosen watchlist type(PEP, AM, DENY).
Basic error handling for missing columns or empty file is performed.

## Requirements
* Java 8
* Maven
* JUnit 4.12+

## Installation and run example

1. `git clone https://github.com/karolgle/spring-batch-demo.git`
2. `cd spring-batch-demo`
3. `mvn clean install spring-boot:repackage`
4. `java -jar target/spring-batch-demo-0.0.1-SNAPSHOT-spring-boot.jar`


## Tests
Install maven or if using IntelliJ set environment path to `{INSTALLATION_BASE}/plugins/maven/lib/maven3/bin`. 

Run following command on cloned repository:

1. Run `mvn test`

## Contributors

For now only me :).

## License

A short snippet describing the license (MIT, Apache, etc.)