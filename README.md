Java-Apache-Spark---Crime-Fighter

In order to add Apache Spark libraries you need to add these dependencies in your xml file:

	https://www.mvnrepository.com/artifact/org.apache.spark/spark-core

    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-core_2.11</artifactId>
        <version>2.2.1</version>
    </dependency>

	https://www.mvnrepository.com/artifact/org.apache.spark/spark-sql

    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-sql_2.11</artifactId>
        <version>2.2.1</version>
    </dependency>