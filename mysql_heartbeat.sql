use dbatools;

drop trigger if exists before_update_heartbeat;
drop table if exists before_update_heartbeat;
create table if not exists heartbeat (
	source_db int primary key						comment 'server id of database.  Used to potentially differentiate different sources of replication',
	source_time timestamp default CURRENT_TIMESTAMP comment 'current_timestamp is deterministic, when inserted the current timestamp of master will transmit as a part of the transaction.', 
	dest_time timestamp null default null 			comment 'column containing time when row was placed into table on slave',
    replication_diff float							comment 'column containing difference in seconds between source and dest time'
) comment 'Table which is updated by the \"update_heartbeat\" event to create binary log markers which are replicated to slaves';
delimiter // 

CREATE TRIGGER before_update_heartbeat BEFORE update ON heartbeat
-- sysdate() call functions exactly like Oracle's sysdate.  Being non-deterministic it will be the actual time the trigger fires
-- this gives the actual time the row gets replicated to the slave.  
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
-- This event inserts/updates a row into the local table, for replication to the slave.  It has to use statement based replication to allow triggers to fire on the slave side, completing
-- the other half of the replication timestamp.
ON SCHEDULE EVERY 10 SECOND STARTS now()
DO BEGIN 
    SET SESSION binlog_format = 'STATEMENT';		
	SET @source_db := @@GLOBAL.server_id;    		-- store current db in session variable, forcing it to be resolved on local db, rather than replicating reference to global variable
	insert into heartbeat (source_db, source_time)	-- perform 'upsert'.  current_timestamp is deterministic and included with the transaction within the binlog.
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
    -- must manually create on the slave if %_norep objects are not replicated
	source_db int				comment 'server id of source database.  source_db of 0 is the default behavior unless otherwise overridden',
    max_lag_seconds int			comment 'maximum lag before alerting',
    enabled char(1)				comment 'y/n field which indicates if the threshold should alert',
    ignore_until datetime		comment 'time at which alerts should begin. (Good for silencing lag for a known lag condition)',
    comment varchar(50)			comment 'comment for threshold'
	) comment 'table used for configuration of maximum allowable replication lag';
insert into lag_alert_threshold_norep values (0,30,'y',null,'default acceptable lag')
;

drop view if exists lag_current;
create view lag_current 
-- view for monitoring lag conditions, regardless of if they are currently alerting.
as (
	select hb.source_db, hb.source_time, hb.dest_time, hb.replication_diff, timestampdiff(SECOND, source_time, sysdate()) as seconds_since_heartbeat, coalesce(ilat.source_db,0) as lag_alert_mapping 
	from heartbeat hb
	left outer join lag_alert_threshold_norep ilat on hb.source_db = ilat.source_db
)
;

drop view if exists lag_alert_view;
create view lag_alert_view 
-- building upon lag_current, this view only contains records for thresholds with greater than maximum allowed replication lag
as (
	select lc.*, lat.max_lag_seconds, lat.enabled, lat.ignore_until, lat.comment
    from lag_current lc
	join lag_alert_threshold_norep lat on lc.lag_alert_mapping = lat.source_db
	where ((lc.seconds_since_heartbeat > lat.max_lag_seconds) or (lc.replication_diff > lat.max_lag_seconds))
    and lat.enabled = 'y'
    and coalesce(lat.ignore_until, sysdate()) <= sysdate()
)
;
SET GLOBAL event_scheduler = ON;
