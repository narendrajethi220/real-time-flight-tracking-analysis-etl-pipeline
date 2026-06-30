-- Flight Tracking System - RDS Database Schema
-- Database: Amazon RDS --> MySQL

-- DATABASE NAME
-- flight_tracking


-- Create country_flight_stats table

CREATE TABLE IF NOT EXISTS country_flight_stats(
    id INT AUTO_INCREMENT PRIMARY KEY,
    window_start TIMESTAMP NULL,
    window_end TIMESTAMP NULL,
    origin_country VARCHAR(100),
    flight_count INT,
    avg_altitude DOUBLE,
    avg_speed DOUBLE,
    UNIQUE KEY unique_window_country (window_start, window_end, origin_country)
);

-- CONNECTION DETAILS
mysql -h <RDS-ENDPOINT> -P 3306 -u <USERNAME> -p

mysql -h <flight-tracking-db.cxyz123abc.us-east-1.rds.amazonaws.com> -P 3306 -u admin -p