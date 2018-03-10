# topKdocuments
Used to find the top K documents which have the most occurences of the words in the query text using Spark Map Reduce.

### Execution instructions
Run `mvn clean install` to generate the app package
Run `spark-submit --class <name of your main class> --master local target/<name of your jar>.jar` to run spark locally

### Pre-Requisites
1. java
2. Scala
3. Maven
4. Spark 
