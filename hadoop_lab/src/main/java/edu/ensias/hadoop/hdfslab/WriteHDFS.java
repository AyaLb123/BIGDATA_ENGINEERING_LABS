package edu.ensias.hadoop.hdfslab;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class WriteHDFS {
    public static void main(String[] args) throws IOException {
        // Vérification des arguments
        if (args.length < 2) {
            System.out.println("Usage: HDFSWrite <chemin_fichier_HDFS> <texte>");
            System.exit(1);
        }

        String cheminFichier = args[0];
        String texte = args[1];

        // Configuration Hadoop
        Configuration conf = new Configuration();
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));

        FileSystem fs = FileSystem.get(conf);
        Path fichier = new Path(cheminFichier);

        // Créer le fichier (écrase si existe déjà)
        try (FSDataOutputStream outStream = fs.create(fichier, true)) {
            // Écriture lisible du texte dans HDFS
            outStream.writeBytes("Bonjour tout le monde !\n");
            outStream.writeBytes(texte + "\n");
        } catch (IOException e) {
            System.err.println("Erreur lors de l'écriture dans le fichier : " + e.getMessage());
            e.printStackTrace();
        } finally {
            fs.close();
        }

        System.out.println("Fichier écrit avec succès dans HDFS : " + cheminFichier);
    }
}
