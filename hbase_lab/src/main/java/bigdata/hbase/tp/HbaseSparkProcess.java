package bigdata.hbase.tp;

// Import des classes nécessaires pour la configuration HBase
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;

// Import des classes Spark
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;

public class HbaseSparkProcess {

    // Méthode pour créer une connexion à HBase et lire les données via Spark
    public void createHbaseTable() {

        // Création de la configuration HBase par défaut
        Configuration config = HBaseConfiguration.create();

        // Configuration de Spark
        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkHBaseTest")  // Nom de l'application Spark
                .setMaster("local[4]");        // Exécution locale avec 4 threads

        // try-with-resources ← ferme automatiquement jsc
        try (JavaSparkContext jsc = new JavaSparkContext(sparkConf)) {

            // Définition de la table HBase à lire
            config.set(TableInputFormat.INPUT_TABLE, "products");

            // Création d'un RDD à partir de la table HBase
            JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = jsc.newAPIHadoopRDD(
                    config,
                    TableInputFormat.class,
                    ImmutableBytesWritable.class,
                    Result.class
            );

            // Affichage du nombre d'enregistrements présents dans la table HBase
            System.out.println("Nombre d'enregistrements: " + hBaseRDD.count());
        }
    }

    // Méthode principale pour exécuter le programme
    public static void main(String[] args){
        HbaseSparkProcess admin = new HbaseSparkProcess();
        admin.createHbaseTable();
    }
}
