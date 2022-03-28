CREATE EXTENSION IF NOT EXISTS dblink;

DO 
$$
DECLARE database_name TEXT;
DECLARE conn_template TEXT;
DECLARE conn_string TEXT;
DECLARE table_exists Boolean;
BEGIN
    conn_template = 'user=myuser password=mypass dbname=';

    FOR database_name IN
        SELECT datname FROM pg_database
        WHERE datistemplate = false
    LOOP
        conn_string = conn_template || database_name;

        table_exists = (select table_exists_ from dblink(conn_string, '(select Count(*) > 0 from information_schema.tables where table_name = ''databasechangeloglock'')') as (table_exists_ Boolean));
        IF table_exists THEN
            perform dblink_exec(conn_string, 'delete from databasechangeloglock');
        END IF;     
    END LOOP;

END
$$
