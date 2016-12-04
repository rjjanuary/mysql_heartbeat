use dbatools;

drop trigger if exists before_update_heartbeat;
drop table if exists before_update_heartbeat;
create table if not exists heartbeat (
	source_db int primary key,
    script_time timestamp null default null,
	source_time timestamp default CURRENT_TIMESTAMP,
	dest_time timestamp null default null,
    replication_diff float
);
delimiter // 
CREATE TRIGGER before_update_heartbeat BEFORE update ON heartbeat
FOR EACH ROW
BEGIN
	IF NEW.source_db != @@server_id THEN
		SET NEW.DEST_TIME = sysdate(),
		NEW.replication_diff = timestampdiff( SECOND, NEW.SOURCE_TIME, sysdate());
    END IF;
END;
//
DROP EVENT IF EXISTS update_heartbeat //
CREATE EVENT IF NOT EXISTS update_heartbeat
ON SCHEDULE EVERY 10 SECOND STARTS now()
DO BEGIN 
    SET SESSION binlog_format = 'STATEMENT'; -- have to use statement based replication to allow triggers to fire on the slave side 
	SET @source_db := @@GLOBAL.server_id; -- store current db in session variable, forcing it to be resolved on local db, rather than replicating reference to global variable
	insert into heartbeat (source_db, source_time) -- perform 'upsert'
		values (@source_db,current_timestamp)
		on duplicate key update
			source_db = values(source_db),
			source_time = values(source_time)
	;
END //
delimiter ;
drop table if exists lag_alert_threshold_norep;
create table lag_alert_threshold_norep (
	-- name is based on the assumption that replicate-wild-ignore-table param configured to ignore tablenames matching %.%_norep 
	source_db int,
    max_lag_seconds int,
    enabled char(1),
    ignore_until datetime,
    comment varchar(50)
	);
insert into lag_alert_threshold_norep values (0,30,'y',null,'default acceptable lag')
;
drop view if exists lag_current;
create view lag_current as (
	select hb.source_db, hb.source_time, hb.dest_time, hb.replication_diff, timestampdiff(SECOND, source_time, sysdate()) as seconds_since_heartbeat, coalesce(ilat.source_db,0) as lag_alert_mapping 
	from heartbeat hb
	left outer join lag_alert_threshold_norep ilat on hb.source_db = ilat.source_db
)
;
drop view if exists lag_alert_view;
create view lag_alert_view as (
	select lc.*, lat.max_lag_seconds, lat.enabled, lat.ignore_until, lat.comment
    from lag_current lc
	join lag_alert_threshold_norep lat on lc.lag_alert_mapping = lat.source_db
	where ((lc.seconds_since_heartbeat > lat.max_lag_seconds) or (lc.replication_diff > lat.max_lag_seconds))
    and lat.enabled = 'y'
    and coalesce(lat.ignore_until, sysdate()) <= sysdate()
)
;
SET GLOBAL event_scheduler = ON;
