DROP VIEW  IF EXISTS v_recent_measurements;
DROP VIEW  IF EXISTS v_hosts;
DROP INDEX IF EXISTS timestamp_index;
DROP TABLE IF EXISTS df_stat;
DROP TABLE IF EXISTS hosts;

CREATE TABLE hosts( 
    host varchar(32), 
    partition varchar(32), 
    PRIMARY KEY (host, partition) 
    );

CREATE TABLE df_stat( 
    host varchar(32), 
    partition varchar(32) DEFAULT 'ERROR', 
    partition_size int DEFAULT 0, 
    avail_disk int DEFAULT 0, 
    error_code int DEFAULT 0, 
    measured_at datetime default current_timestamp, 
    FOREIGN KEY (host, partition) REFERENCES hosts(host, partition) ON DELETE CASCADE ON UPDATE CASCADE);

CREATE INDEX timestamp_idx on df_stat(measured_at);

CREATE VIEW v_hosts as SELECT * FROM hosts ORDER BY host, partition;

CREATE VIEW v_recent_measurements as SELECT * FROM df_stat ORDER BY measured_at DESC;

insert into hosts (host, partition) values ('alexis', 'ERROR');
insert into hosts (host, partition) values ('alexis', '/home');
insert into hosts (host, partition) values ('alexis', '/usr/local');
insert into hosts (host, partition) values ('adam', 'ERROR');
insert into hosts (host, partition) values ('adam', '/home');
insert into hosts (host, partition) values ('adam', '/');
insert into hosts (host, partition) values ('spydur', 'ERROR');
insert into hosts (host, partition) values ('spydur', '/home');
insert into hosts (host, partition) values ('spydur', '/usr/local');
insert into hosts (host, partition) values ('spydur', '/scratch');
insert into hosts (host, partition) values ('billieholiday', 'ERROR');
insert into hosts (host, partition) values ('billieholiday', '/home');
insert into hosts (host, partition) values ('billieholiday', '/');
insert into hosts (host, partition) values ('justin', 'ERROR');
insert into hosts (host, partition) values ('justin', '/home');
insert into hosts (host, partition) values ('justin', '/');
insert into hosts (host, partition) values ('justin', '/scr');
insert into hosts (host, partition) values ('justin', '/data');
insert into hosts (host, partition) values ('boyi', '/data');
insert into hosts (host, partition) values ('boyi', '/scr');
insert into hosts (host, partition) values ('boyi', '/');
insert into hosts (host, partition) values ('boyi', 'ERROR');
insert into hosts (host, partition) values ('camryn', '/');
insert into hosts (host, partition) values ('camryn', '/home');
insert into hosts (host, partition) values ('camryn', 'ERROR');
insert into hosts (host, partition) values ('cooper', '/');
insert into hosts (host, partition) values ('cooper', '/home');
insert into hosts (host, partition) values ('cooper', 'ERROR');
insert into hosts (host, partition) values ('erica', '/home');
insert into hosts (host, partition) values ('erica', '/');
insert into hosts (host, partition) values ('erica', '/scratch');
insert into hosts (host, partition) values ('erica', 'ERROR');
insert into hosts (host, partition) values ('evan', '/');
insert into hosts (host, partition) values ('evan', '/home');
insert into hosts (host, partition) values ('evan', 'ERROR');
insert into hosts (host, partition) values ('hamilton', '/home');
insert into hosts (host, partition) values ('hamilton', '/');
insert into hosts (host, partition) values ('hamilton', 'ERROR');
insert into hosts (host, partition) values ('irene', '/home');
insert into hosts (host, partition) values ('irene', '/');
insert into hosts (host, partition) values ('irene', 'ERROR');
insert into hosts (host, partition) values ('kevin', '/');
insert into hosts (host, partition) values ('kevin', '/home');
insert into hosts (host, partition) values ('kevin', 'ERROR');
insert into hosts (host, partition) values ('mayer', '/home');
insert into hosts (host, partition) values ('mayer', '/');
insert into hosts (host, partition) values ('mayer', 'ERROR');
insert into hosts (host, partition) values ('michael', '/');
insert into hosts (host, partition) values ('michael', '/home');
insert into hosts (host, partition) values ('michael', 'ERROR');
insert into hosts (host, partition) values ('sarah', '/');
insert into hosts (host, partition) values ('sarah', 'ERROR');
insert into hosts (host, partition) values ('thais', '/');
insert into hosts (host, partition) values ('thais', '/home');
insert into hosts (host, partition) values ('thais', 'ERROR');
insert into hosts (host, partition) values ('spiderweb', '/');
insert into hosts (host, partition) values ('spiderweb', '/var');
insert into hosts (host, partition) values ('spiderweb', '/opt');
insert into hosts (host, partition) values ('spiderweb', '/home');
insert into hosts (host, partition) values ('spiderweb', '/usr/local');
insert into hosts (host, partition) values ('enterprise', '/');
insert into hosts (host, partition) values ('enterprise', '/home');
insert into hosts (host, partition) values ('enterprise', 'ERROR');
insert into hosts (host, partition) values ('trueuser', '/');
insert into hosts (host, partition) values ('trueuser', '/mnt/usrlocal');
insert into hosts (host, partition) values ('trueuser', '/var');
insert into hosts (host, partition) values ('trueuser', 'ERROR');
insert into hosts (host, partition) values ('truenas', '/mnt/Parish_backup');
insert into hosts (host, partition) values ('truenas', '/');
insert into hosts (host, partition) values ('truenas', '/var');
insert into hosts (host, partition) values ('truenas', 'ERROR');
insert into hosts (host, partition) values ('newnas', '/var');
insert into hosts (host, partition) values ('newnas', '/');
insert into hosts (host, partition) values ('newnas', 'ERROR');
insert into hosts (host, partition) values ('newnas', '/mnt/chem1');

