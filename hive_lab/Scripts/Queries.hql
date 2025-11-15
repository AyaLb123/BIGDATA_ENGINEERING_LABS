-- ============================================
-- Script 3: Queries.hql
-- Toutes les requêtes d'analyse
-- ============================================

USE hotel_booking;

-- ============================================
-- 5. Utilisation de requêtes simples
-- ============================================

-- Lister tous les clients
SELECT * FROM clients;

-- Lister tous les hôtels à Paris
SELECT * FROM hotels 
WHERE ville = 'Paris';

-- Lister toutes les réservations avec les informations sur les hôtels et les clients
SELECT 
    r.reservation_id,
    c.client_id,
    c.nom AS nom_client,
    c.email,
    h.hotel_id,
    h.nom AS nom_hotel,
    h.ville,
    h.etoiles,
    r.date_debut,
    r.date_fin,
    r.prix_total
FROM reservations r
JOIN clients c ON r.client_id = c.client_id
JOIN hotels h ON r.hotel_id = h.hotel_id;


-- ============================================
-- 6. Requêtes avec jointures
-- ============================================

-- Afficher le nombre de réservations par client
SELECT 
    c.client_id,
    c.nom,
    COUNT(r.reservation_id) AS nombre_reservations
FROM clients c
LEFT JOIN reservations r ON c.client_id = r.client_id
GROUP BY c.client_id, c.nom
ORDER BY nombre_reservations DESC;

-- Afficher les clients qui ont réservé plus que 2 nuitées
SELECT 
    c.client_id,
    c.nom,
    r.reservation_id,
    DATEDIFF(r.date_fin, r.date_debut) AS nombre_nuitees
FROM clients c
JOIN reservations r ON c.client_id = r.client_id
WHERE DATEDIFF(r.date_fin, r.date_debut) > 2;

-- Afficher les Hôtels réservés par chaque client
SELECT 
    c.client_id,
    c.nom AS nom_client,
    h.hotel_id,
    h.nom AS nom_hotel,
    h.ville,
    COUNT(r.reservation_id) AS nombre_reservations
FROM clients c
JOIN reservations r ON c.client_id = r.client_id
JOIN hotels h ON r.hotel_id = h.hotel_id
GROUP BY c.client_id, c.nom, h.hotel_id, h.nom, h.ville
ORDER BY c.client_id, nombre_reservations DESC;

-- Afficher les noms des hôtels dans lesquels il y a plus qu'une réservation
SELECT 
    h.hotel_id,
    h.nom,
    h.ville,
    COUNT(r.reservation_id) AS nombre_reservations
FROM hotels h
JOIN reservations r ON h.hotel_id = r.hotel_id
GROUP BY h.hotel_id, h.nom, h.ville
HAVING COUNT(r.reservation_id) > 1
ORDER BY nombre_reservations DESC;

-- Afficher les noms des hôtels dans lesquels il y a pas de réservation
SELECT 
    h.hotel_id,
    h.nom,
    h.ville,
    h.etoiles
FROM hotels h
LEFT JOIN reservations r ON h.hotel_id = r.hotel_id
WHERE r.reservation_id IS NULL;


-- ============================================
-- 7. Requêtes imbriquées
-- ============================================

-- Afficher les clients ayant réservé un hôtel avec plus de 4 étoiles
SELECT DISTINCT
    c.client_id,
    c.nom,
    c.email
FROM clients c
WHERE c.client_id IN (
    SELECT r.client_id
    FROM reservations r
    JOIN hotels h ON r.hotel_id = h.hotel_id
    WHERE h.etoiles > 4
);

-- Afficher le Total des revenus générés par chaque hôtel
SELECT 
    h.hotel_id,
    h.nom,
    h.ville,
    h.etoiles,
    COALESCE(SUM(r.prix_total), 0) AS revenus_totaux
FROM hotels h
LEFT JOIN reservations r ON h.hotel_id = r.hotel_id
GROUP BY h.hotel_id, h.nom, h.ville, h.etoiles
ORDER BY revenus_totaux DESC;


-- ============================================
-- 8. Utilisation de fonctions d'agrégation 
--    avec partitions et buckets
-- ============================================

-- Revenus totaux par ville (partitionnée)
SELECT 
    hp.ville,
    COUNT(DISTINCT hp.hotel_id) AS nombre_hotels,
    COALESCE(SUM(r.prix_total), 0) AS revenus_totaux
FROM hotels_partitioned hp
LEFT JOIN reservations r ON hp.hotel_id = r.hotel_id
GROUP BY hp.ville
ORDER BY revenus_totaux DESC;

-- Nombre total de réservations par client (bucketed)
SELECT 
    rb.client_id,
    c.nom,
    COUNT(rb.reservation_id) AS nombre_reservations,
    SUM(rb.prix_total) AS total_depense
FROM reservations_bucketed rb
JOIN clients c ON rb.client_id = c.client_id
GROUP BY rb.client_id, c.nom
ORDER BY nombre_reservations DESC;


-- ============================================
-- Requêtes supplémentaires d'analyse
-- ============================================

-- Top 5 des clients par dépenses totales
SELECT 
    c.client_id,
    c.nom,
    SUM(r.prix_total) AS depense_totale,
    COUNT(r.reservation_id) AS nombre_reservations
FROM clients c
JOIN reservations r ON c.client_id = r.client_id
GROUP BY c.client_id, c.nom
ORDER BY depense_totale DESC
LIMIT 5;

-- Durée moyenne de séjour par hôtel
SELECT 
    h.hotel_id,
    h.nom,
    h.ville,
    AVG(DATEDIFF(r.date_fin, r.date_debut)) AS duree_moyenne_sejour,
    COUNT(r.reservation_id) AS nombre_reservations
FROM hotels h
JOIN reservations r ON h.hotel_id = r.hotel_id
GROUP BY h.hotel_id, h.nom, h.ville
ORDER BY duree_moyenne_sejour DESC;

-- Prix moyen par nuitée par ville
SELECT 
    h.ville,
    AVG(r.prix_total / DATEDIFF(r.date_fin, r.date_debut)) AS prix_moyen_nuitee,
    COUNT(r.reservation_id) AS nombre_reservations
FROM hotels h
JOIN reservations r ON h.hotel_id = r.hotel_id
WHERE DATEDIFF(r.date_fin, r.date_debut) > 0
GROUP BY h.ville
ORDER BY prix_moyen_nuitee DESC;