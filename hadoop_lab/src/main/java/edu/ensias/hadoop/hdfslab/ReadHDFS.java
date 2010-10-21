package edu.ensias.hadoop.hdfslab;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class ReadHDFS {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: ReadHDFSFull <chemin_fichier_HDFS>");
            System.exit(1);
        }

        String cheminFichier = args[0];

        Configuration conf = new Configuration();
        // Charger explicitement la configuration Hadoop
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));

        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            Path fichier = new Path(cheminFichier);

            if (!fs.exists(fichier)) {
                System.out.println("Le fichier n'existe pas : " + fichier);
                System.exit(1);
            }

            // Lire tout le fichier ligne par ligne
            try (FSDataInputStream inStream = fs.open(fichier);
                 BufferedReader br = new BufferedReader(new InputStreamReader(inStream))) {

                String line;
                while ((line = br.readLine()) != null) {
                    System.out.println(line);
                }
            }

        } catch (IOException e) {
            System.err.println("Erreur lors de la lecture du fichier : " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Fermer le système de fichiers
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
