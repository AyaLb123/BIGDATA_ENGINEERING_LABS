-- ============================================
-- Script 2: Loading.hql
-- Chargement des données dans les tables
-- ============================================

USE hotel_booking;

-- Activer les propriétés pour les partitions dynamiques
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=20000;
SET hive.exec.max.dynamic.partitions.pernode=20000;
SET hive.enforce.bucketing=true;

-- Charger les données dans la table clients
LOAD DATA LOCAL INPATH '/shared_volume/clients.txt' 
OVERWRITE INTO TABLE clients;

-- Charger les données dans la table hotels
LOAD DATA LOCAL INPATH '/shared_volume/hotels.txt' 
OVERWRITE INTO TABLE hotels;

-- Charger les données dans la table reservations avec partition dynamique
-- Note: Pour les partitions dynamiques, il faut d'abord créer une table temporaire
CREATE TEMPORARY TABLE IF NOT EXISTS temp_reservations (
    reservation_id INT,
    client_id INT,
    hotel_id INT,
    date_debut DATE,
    date_fin DATE,
    prix_total DECIMAL(10,2)
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/shared_volume/reservations.txt' 
INTO TABLE temp_reservations;

INSERT OVERWRITE TABLE reservations PARTITION(date_debut)
SELECT 
    reservation_id,
    client_id,
    hotel_id,
    date_fin,
    prix_total,
    date_debut
FROM temp_reservations;

-- Charger les données dans hotels_partitioned
INSERT OVERWRITE TABLE hotels_partitioned PARTITION(ville)
SELECT 
    hotel_id,
    nom,
    etoiles,
    ville
FROM hotels;

-- Charger les données dans reservations_bucketed
INSERT OVERWRITE TABLE reservations_bucketed
SELECT 
    r.reservation_id,
    r.client_id,
    r.hotel_id,
    r.date_debut,
    r.date_fin,
    r.prix_total
FROM reservations r;

-- Vérifier le chargement des données
SELECT 'Nombre de clients:' AS info, COUNT(*) AS total FROM clients
UNION ALL
SELECT 'Nombre d\'hotels:', COUNT(*) FROM hotels
UNION ALL
SELECT 'Nombre de réservations:', COUNT(*) FROM reservations;