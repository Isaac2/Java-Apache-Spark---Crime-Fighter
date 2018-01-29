/**
 * Created by Miku on 26/01/2018.
 */

//SQL
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

//spark config
import org.apache.spark.sql.SparkSession;

public class datasetManager {

    private SparkSession spark;
    private Dataset<Row> dataset;

    private datasetManager(String tableName, String pathToFile) {
        this.spark = SparkSession.builder().master("local").appName("Dataset Analyzer").getOrCreate();
        generateDataSet(pathToFile);
        setTableName(tableName);
    }

    // Creates a temporary view using the DataFrame
    private void setTableName(String tableName){
        this.dataset.createOrReplaceTempView(tableName);
    }

    private void generateDataSet(String pathToFile) {
        this.dataset = this.spark
                .read()
                .format("csv")
                .option("header", true)
                .load(pathToFile);
        //newDataSet.printSchema();
    }

    private Dataset filterDataSet(String sqlQuery){
        // SQL statements can be run by using the sql methods provided by spark
        Dataset<Row> filteredDataSet = this.spark.sql(sqlQuery);
        //filteredDataSet.show();
        return filteredDataSet;
    }

    public static void main(String[] args) {
        String pathToFile = "Crime_dataset_headers";
        String tableName = "crimes";
        datasetManager dM = new datasetManager(tableName, pathToFile);

        String query = "select fbicode,count(fbicode) as count from Crimes group by fbicode";
        Dataset result = dM.filterDataSet(query);
        System.out.println("----FBIcode count----");
        result.show();

        query = "select count(*) as count from Crimes where Primary_type ='NARCOTICS' and year = 2015";
        result = dM.filterDataSet(query);
        System.out.println("----‘NARCOTICS’ cases filed in the year 2015----");
        result.show();

        query = "SELECT District, count(*) as count FROM Crimes WHERE Primary_type == 'THEFT' AND arrest == 'true' group by District ";
        result = dM.filterDataSet(query);
        System.out.println("----Number of theft related arrests that happened in each district----");
        result.show();
    }
}
