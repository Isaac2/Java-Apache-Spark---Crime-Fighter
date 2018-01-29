/**
 * Created by Miku on 26/01/2018.
 */

//SQL
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;

//spark config
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

//Test
import java.util.Arrays;
import java.util.List;

import org.apache.spark.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;

public class crimeAnalyzer {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local").appName("crymeAnalyzer").getOrCreate();
        System.out.println("Session created");

        // A crimes dataset is pointed to by path.
        // The path can be either a single text file or a directory storing text files
        //Dataset<Row> crimes = spark.read().csv("Crime_dataset");
        Dataset<Row> crimes = spark
                .read()
                .format("com.databricks.spark.csv")
                .option("header", true)
                .load("Crime_dataset_headers");

        // The inferred schema can be visualized using the printSchema() method
        crimes.printSchema();
        //root
        // |-- id: string (nullable = true)
        // |-- Case_Number: string (nullable = true)
        // |-- Date: string (nullable = true)
        // |-- Block: string (nullable = true)
        // |-- IUCR: string (nullable = true)
        // |-- Primary_type: string (nullable = true)
        // |-- Description: string (nullable = true)
        // |-- Location_description: string (nullable = true)
        // |-- Arrest: string (nullable = true)
        // |-- Domestic: string (nullable = true)
        // |-- Beat: string (nullable = true)
        // |-- District: string (nullable = true)
        // |-- Ward: string (nullable = true)
        // |-- community: string (nullable = true)
        // |-- Fbicode: string (nullable = true)
        // |-- X: string (nullable = true)
        // |-- Y: string (nullable = true)
        // |-- Year: string (nullable = true)
        // |-- Updated: string (nullable = true)
        // |-- lattitude: string (nullable = true)
        // |-- longititude: string (nullable = true)
        // |-- location: string (nullable = true)

		// Creates a temporary view using the DataFrame
		crimes.createOrReplaceTempView("crimes");

		// SQL statements can be run by using the sql methods provided by spark
		Dataset<Row> namesDF = spark.sql("select fbicode,count(fbicode) as count from Crimes group by fbicode");
		namesDF.show();
		/*


		// Alternatively, a DataFrame can be created for a JSON dataset represented by
		// a Dataset<String> storing one JSON object per string.
		List<String> jsonData = Arrays.asList(
				"{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
		Dataset<String> anotherPeopleDataset = spark.createDataset(jsonData, Encoders.STRING());
		Dataset<Row> anotherPeople = spark.read().json(anotherPeopleDataset);
		anotherPeople.show();
		// +---------------+----+
		// |        address|name|
		// +---------------+----+
		// |[Columbus,Ohio]| Yin|
		// +---------------+----+

		*/
    }
}
