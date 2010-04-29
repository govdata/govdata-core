#!/usr/bin/env bash
POSTGIS_SQL_PATH=`/usr/lib/postgresql/8.4/bin/pg_config --sharedir`/contrib/postgis-1.5
/usr/lib/postgresql/8.4/bin/createdb -E UTF8 template_postgis # Create the template spatial database.
/usr/lib/postgresql/8.4/bin/createlang -d template_postgis plpgsql # Adding PLPGSQL language support.
/usr/lib/postgresql/8.4/bin/psql -d postgres -c "UPDATE pg_database SET datistemplate='true' WHERE datname='template_postgis';"
/usr/lib/postgresql/8.4/bin/psql -d template_postgis -f $POSTGIS_SQL_PATH/postgis.sql # Loading the PostGIS SQL routines
/usr/lib/postgresql/8.4/bin/psql -d template_postgis -f $POSTGIS_SQL_PATH/spatial_ref_sys.sql
/usr/lib/postgresql/8.4/bin/psql -d template_postgis -c "GRANT ALL ON geometry_columns TO PUBLIC;" # Enabling users to alter spatial tables.
/usr/lib/postgresql/8.4/bin/psql -d template_postgis -c "GRANT ALL ON geography_columns TO PUBLIC;"
/usr/lib/postgresql/8.4/bin/psql -d template_postgis -c "GRANT ALL ON spatial_ref_sys TO PUBLIC;"
