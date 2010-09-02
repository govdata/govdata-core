#!/usr/bin/env bash
POSTGIS_SQL_PATH=`/usr/bin/pg_config --sharedir`/contrib/postgis-1.5
/usr/bin/createdb -E UTF8 template_postgis_centos # Create the template spatial database.
/usr/bin/createlang -d template_postgis_centos plpgsql # Adding PLPGSQL language support.
/usr/bin/psql -d postgres -c "UPDATE pg_database SET datistemplate='true' WHERE datname='template_postgis_centos';"
/usr/bin/psql -d template_postgis_centos -f $POSTGIS_SQL_PATH/postgis.sql # Loading the PostGIS SQL routines
/usr/bin/psql -d template_postgis_centos -f $POSTGIS_SQL_PATH/spatial_ref_sys.sql
/usr/bin/psql -d template_postgis_centos -c "GRANT ALL ON geometry_columns TO PUBLIC;" # Enabling users to alter spatial tables.
/usr/bin/psql -d template_postgis_centos -c "GRANT ALL ON geography_columns TO PUBLIC;"
/usr/bin/psql -d template_postgis_centos -c "GRANT ALL ON spatial_ref_sys TO PUBLIC;"
