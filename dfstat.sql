DROP VIEW  IF EXISTS v_recent_measurements;
DROP VIEW  IF EXISTS v_hosts;
DROP INDEX IF EXISTS timestamp_index;
DROP TABLE IF EXISTS df_stat;
DROP TABLE IF EXISTS hosts;

CREATE TABLE hosts( host varchar(32), partition varchar(32), PRIMARY KEY (host, partition) );

CREATE TABLE df_stat( host varchar(32), partition varchar(32) DEFAULT 'ERROR', partition_size int DEFAULT 0, avail_disk int DEFAULT 0, error_code int DEFAULT 0, measured_at datetime default current_timestamp, FOREIGN KEY (host, partition) REFERENCES hosts(host, partition) ON DELETE CASCADE ON UPDATE CASCADE);

CREATE INDEX timestamp_idx on df_stat(measured_at);

CREATE VIEW v_hosts as SELECT * FROM hosts ORDER BY host, partition;

CREATE VIEW v_recent_measurements as SELECT * FROM df_stat ORDER BY measured_at DESC;


