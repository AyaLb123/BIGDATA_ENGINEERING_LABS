package edu.ensias.hadoop.hdfslab;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HadoopFileStatus {
    public static void main(String[] args) {
        // Vérifier que les 3 arguments sont fournis
        if (args.length != 3) {
            System.out.println("Usage: HadoopFileStatus <chemin_dossier> <nom_fichier> <nouveau_nom_fichier>");
            System.exit(1);
        }

        String cheminDossier = args[0];  // ex: /user/root/input
        String nomFichier = args[1];     // ex: achats.txt
        String nouveauNom = args[2];     // ex: nouveau_nom.txt

        Configuration conf = new Configuration();

        // Charger explicitement la configuration Hadoop
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));

        try {
            FileSystem fs = FileSystem.get(conf);
            Path filepath = new Path(cheminDossier, nomFichier);

            // Vérifier si le fichier existe
            if (!fs.exists(filepath)) {
                System.out.println("File does not exist: " + filepath);
                System.exit(1);
            }

            // Lire les métadonnées du fichier
            FileStatus infos = fs.getFileStatus(filepath);
            System.out.println("File Name: " + filepath.getName());
            System.out.println("File Size: " + infos.getLen());
            System.out.println("File owner: " + infos.getOwner());
            System.out.println("File permission: " + infos.getPermission());
            System.out.println("File Replication: " + infos.getReplication());
            System.out.println("File Block Size: " + infos.getBlockSize());

            BlockLocation[] blockLocations = fs.getFileBlockLocations(infos, 0, infos.getLen());
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                System.out.println("Block offset: " + blockLocation.getOffset());
                System.out.println("Block length: " + blockLocation.getLength());
                System.out.print("Block hosts: ");
                for (String host : hosts) {
                    System.out.print(host + " ");
                }
                System.out.println();
            }

            // Renommer le fichier
            Path newFilePath = new Path(cheminDossier, nouveauNom);
            if (fs.rename(filepath, newFilePath)) {
                System.out.println("File renamed successfully to: " + nouveauNom);
            } else {
                System.out.println("Failed to rename file.");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
