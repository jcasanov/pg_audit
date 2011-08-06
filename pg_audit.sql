-- Table to keep auditing information
CREATE TABLE audit_log (
    log_id 				serial NOT NULL PRIMARY KEY,
	log_relid			oid		NOT NULL,
    log_session_user	text	NOT NULL DEFAULT SESSION_USER,
    log_when 			timestamp with time zone	NOT NULL DEFAULT now(),
	log_client_addr		inet,
	log_operation		text,
    log_old_values 		hstore,
    log_new_values 		hstore
);


-- Trigger to use on all the tables we want to track
/*
 * To use create a trigger in this way:
 * CREATE TRIGGER trigger_name AFTER INSERT OR UPDATE OR DELETE
 *        ON table_name FOR EACH ROW EXECUTE PROCEDURE logger();
 *
 * To track TRUNCATE events you can also create a trigger as:
 * CREATE TRIGGER trigger_name AFTER TRUNCATE
 *        ON table_name FOR EACH STATEMENT EXECUTE PROCEDURE logger();
 */
CREATE OR REPLACE FUNCTION logger() RETURNS trigger AS $$
DECLARE 
	hs_new hstore = NULL;
	hs_old hstore = NULL;
BEGIN
	-- Check that the trigger for the logger should be AFTER and FOR EACH ROW
	IF TG_WHEN = 'BEFORE' THEN
		RAISE EXCEPTION 'Trigger for logger should be AFTER';
	END IF; 

	IF TG_LEVEL = 'STATEMENT' AND TG_OP <> 'TRUNCATE' THEN
		RAISE EXCEPTION 'Trigger for logger should be FOR EACH ROW';
	END IF;

	-- Obtain the hstore versions of NEW and OLD, when appropiate
	IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
		SELECT hstore(new.*) INTO hs_new;
	END IF;

	IF TG_OP = 'DELETE' OR TG_OP = 'UPDATE' THEN
		SELECT hstore(old.*) INTO hs_old;
	END IF;

	INSERT INTO audit_log.audit_log(log_relid, log_client_addr, log_operation, log_old_values, log_new_values) 
	WITH t_old(key, value) as (SELECT * FROM each(hs_old)),
	     t_new(key, value) as (SELECT * FROM each(hs_new))
	SELECT TG_RELID, inet_client_addr(), TG_OP, 
		(SELECT hstore(array_agg(key), array_agg(value)) FROM t_old WHERE (key, value) NOT IN (SELECT key, value FROM t_new)),
		(SELECT hstore(array_agg(key), array_agg(value)) FROM t_new WHERE (key, value) NOT IN (SELECT key, value FROM t_old));

	RETURN NULL;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


CREATE OR REPLACE FUNCTION install_logger(schema_name text, table_name text, log_truncate boolean default false) RETURNS boolean AS $$
DECLARE
	fq_table_name text = NULL;
BEGIN
	SELECT schema_name || '.' || table_name INTO fq_table_name; 

	-- check if the table exists and if it doesn't get an error
	EXECUTE 'SELECT ' || quote_literal(fq_table_name) || '::regclass';

	-- drop the trigger if it  already exists and re-create it
	-- this is easier than checking pg_triggers to see if the trigger exists
	EXECUTE 'DROP TRIGGER IF EXISTS auditing_mod_actions ON ' || fq_table_name;
	EXECUTE 'CREATE TRIGGER auditing_mod_actions AFTER INSERT OR UPDATE OR DELETE ' ||
			' ON ' || fq_table_name || ' FOR EACH ROW EXECUTE PROCEDURE logger();';
	
	IF (log_truncate) THEN
		EXECUTE 'DROP TRIGGER IF EXISTS auditing_truncate_actions ON ' || fq_table_name;
		EXECUTE 'CREATE TRIGGER auditing_truncate_actions AFTER TRUNCATE ' ||
				' ON ' || fq_table_name || ' FOR EACH STATEMENT EXECUTE PROCEDURE logger();';
	END IF;

	RETURN TRUE;
END;
$$ LANGUAGE plpgsql SECURITY INVOKER STRICT;
