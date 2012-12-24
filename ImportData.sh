#!/bin/bash

source fips.sh

TMPDIR="/home/gjurman/gisdata/temp"
TIGERDATA="/data/TIGER2012/TIGER2012"
STATE=VT
FIPS=${FIPS_A[$STATE]}
UNZIPTOOL=`which unzip`
export PGDATABASE=geocoder
PSQL=`which psql`
SHP2PGSQL=`which shp2pgsql`

function cleanup_tmp {
	rm -f ${TMPDIR}/*.*
	${PSQL} -c "DROP SCHEMA tiger_staging CASCADE;"
	${PSQL} -c "CREATE SCHEMA tiger_staging;"
}


function import_state {
	cd $TIGERDATA/STATE

	cleanup_tmp

	for z in tl_*state.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
	for z in */tl_*state.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
	cd $TMPDIR;

	${PSQL} -c \
		"CREATE TABLE tiger_data.state_all(CONSTRAINT pk_state_all PRIMARY KEY (statefp),
		 CONSTRAINT uidx_state_all_stusps  UNIQUE (stusps), CONSTRAINT uidx_state_all_gid UNIQUE (gid) ) INHERITS(state);"
	${SHP2PGSQL} -c -s 4269 -g the_geom -W "latin1" tl_2012_us_state.dbf tiger_staging.state | ${PSQL}
	${PSQL} -c \
		"SELECT loader_load_staged_data(lower('state'), lower('state_all')); "
	${PSQL} -c \
		"CREATE INDEX tiger_data_state_all_the_geom_gist ON tiger_data.state_all USING gist(the_geom);"
	${PSQL} -c \
		"VACUUM ANALYZE tiger_data.state_all"
}


function import_county {
	cd $TIGERDATA/COUNTY

	cleanup_tmp

	for z in tl_*county.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
	for z in */tl_*county.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
	cd $TMPDIR;

	${PSQL} -c \
		"CREATE TABLE tiger_data.county_all(CONSTRAINT pk_tiger_data_county_all PRIMARY KEY (cntyidfp),
		 CONSTRAINT uidx_tiger_data_county_all_gid UNIQUE (gid)) INHERITS(county); "
	${SHP2PGSQL} -c -s 4269 -g the_geom -W "latin1" tl_2012_us_county.dbf tiger_staging.county | ${PSQL}
	${PSQL} -c \
		"ALTER TABLE tiger_staging.county RENAME geoid TO cntyidfp;
		 SELECT loader_load_staged_data(lower('county'), lower('county_all'));"
	${PSQL} -c \
		"CREATE INDEX tiger_data_county_the_geom_gist ON tiger_data.county_all USING gist(the_geom);"
	${PSQL} -c \
		"CREATE UNIQUE INDEX uidx_tiger_data_county_all_statefp_countyfp ON tiger_data.county_all 
		 USING btree(statefp,countyfp);"
	${PSQL} -c \
		"CREATE TABLE tiger_data.county_all_lookup ( CONSTRAINT pk_county_all_lookup 
		  PRIMARY KEY (st_code, co_code)) INHERITS (county_lookup);"
	${PSQL} -c \
		"VACUUM ANALYZE tiger_data.county_all;"
	${PSQL} -c \
		"INSERT INTO tiger_data.county_all_lookup(st_code, state, co_code, name)
		 SELECT CAST(s.statefp as integer), s.abbrev, CAST(c.countyfp as integer), c.name
		 FROM tiger_data.county_all As c INNER JOIN state_lookup As s ON s.statefp = c.statefp;"
	${PSQL} -c \
		"VACUUM ANALYZE tiger_data.county_all_lookup;"
}

function import_place {
	cd $TIGERDATA/PLACE

	cleanup_tmp

	for z in tl_*_${FIPS}*_place.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
	for z in */tl_*_${FIPS}*_place.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
	cd $TMPDIR;

	${PSQL} -c \
		"CREATE TABLE tiger_data.${STATE}_place(CONSTRAINT pk_${STATE}_place PRIMARY KEY (plcidfp) ) INHERITS(place);" 
	${SHP2PGSQL} -c -s 4269 -g the_geom -W "latin1" tl_2012_${FIPS}_place.dbf tiger_staging.${STATE}_place | ${PSQL}
	${PSQL} -c \
		"ALTER TABLE tiger_staging.${STATE}_place RENAME geoid TO plcidfp;
		 SELECT loader_load_staged_data(lower('${STATE}_place'), lower('${STATE}_place'));
		 ALTER TABLE tiger_data.${STATE}_place ADD CONSTRAINT uidx_${STATE}_place_gid UNIQUE (gid);"
	${PSQL} -c \
		"CREATE INDEX idx_${STATE}_place_soundex_name ON tiger_data.${STATE}_place USING btree (soundex(name));" 
	${PSQL} -c \
		"CREATE INDEX tiger_data_${STATE}_place_the_geom_gist ON tiger_data.${STATE}_place USING gist(the_geom);"
	${PSQL} -c \
		"ALTER TABLE tiger_data.${STATE}_place ADD CONSTRAINT chk_statefp CHECK (statefp = '${FIPS}');"
}


function import_cousub {
	cd $TIGERDATA/COUSUB

	cleanup_tmp

	for z in tl_*_${FIPS}*_cousub.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
	for z in */tl_*_${FIPS}*_cousub.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
	cd $TMPDIR;

	${PSQL} -c \
		"CREATE TABLE tiger_data.${STATE}_cousub(CONSTRAINT pk_${STATE}_cousub PRIMARY KEY (cosbidfp), 
		 	CONSTRAINT uidx_${STATE}_cousub_gid UNIQUE (gid)) INHERITS(cousub);"
	${SHP2PGSQL} -c -s 4269 -g the_geom -W "latin1" tl_2012_${FIPS}_cousub.dbf tiger_staging.${STATE}_cousub | ${PSQL}
	${PSQL} -c \
		"ALTER TABLE tiger_staging.${STATE}_cousub RENAME geoid TO cosbidfp;
		 SELECT loader_load_staged_data(lower('${STATE}_cousub'), lower('${STATE}_cousub'));
		 ALTER TABLE tiger_data.${STATE}_cousub ADD CONSTRAINT chk_statefp CHECK (statefp = '${FIPS}');"
	${PSQL} -c \
		"CREATE INDEX tiger_data_${STATE}_cousub_the_geom_gist ON tiger_data.${STATE}_cousub USING gist(the_geom);"
	${PSQL} -c \
		"CREATE INDEX idx_tiger_data_${STATE}_cousub_countyfp ON tiger_data.${STATE}_cousub USING btree(countyfp);"
}


function import_tract {
	cd $TIGERDATA/TRACT

	cleanup_tmp

	for z in tl_*_${FIPS}*_tract.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
	for z in */tl_*_${FIPS}*_tract.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
	cd $TMPDIR;

	${PSQL} -c \
		"CREATE TABLE tiger_data.${STATE}_tract(CONSTRAINT pk_${STATE}_tract PRIMARY KEY (tract_id) ) INHERITS(tiger.tract); " 
	${SHP2PGSQL} -c -s 4269 -g the_geom -W "latin1" tl_2012_${FIPS}_tract.dbf tiger_staging.${STATE}_tract | ${PSQL}
	${PSQL} -c \
		"ALTER TABLE tiger_staging.${STATE}_tract RENAME geoid TO tract_id;
		 SELECT loader_load_staged_data(lower('${STATE}_tract'), lower('${STATE}_tract'));"
	${PSQL} -c \
		"CREATE INDEX tiger_data_${STATE}_tract_the_geom_gist ON tiger_data.${STATE}_tract USING gist(the_geom);"
	${PSQL} -c \
		"VACUUM ANALYZE tiger_data.${STATE}_tract;"
	${PSQL} -c \
		"ALTER TABLE tiger_data.${STATE}_tract ADD CONSTRAINT chk_statefp CHECK (statefp = '${FIPS}');"
}

function import_tabblock {
	cd $TIGERDATA/TABBLOCK

	cleanup_tmp

	for z in tl_*_${FIPS}*_tabblock.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
	for z in */tl_*_${FIPS}*_tabblock.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
	cd $TMPDIR;

	${PSQL} -c \
		"CREATE TABLE tiger_data.${STATE}_tabblock(CONSTRAINT pk_${STATE}_tabblock
		 PRIMARY KEY (tabblock_id)) INHERITS(tiger.tabblock);" 
	${SHP2PGSQL} -c -s 4269 -g the_geom -W "latin1" tl_2012_${FIPS}_tabblock.dbf tiger_staging.${STATE}_tabblock | ${PSQL}
	${PSQL} -c \
		"ALTER TABLE tiger_staging.${STATE}_tabblock RENAME geoid TO tabblock_id;
		 SELECT loader_load_staged_data(lower('${STATE}_tabblock'), lower('${STATE}_tabblock'), 
			'{gid, statefp10, countyfp10, tractce10, blockce10,suffix1ce,blockce,tractce}'::text[]); "
	${PSQL} -c \
		"ALTER TABLE tiger_data.${STATE}_tabblock ADD CONSTRAINT chk_statefp CHECK (statefp = '${FIPS}');"
	${PSQL} -c \
		"CREATE INDEX tiger_data_${STATE}_tabblock_the_geom_gist ON tiger_data.${STATE}_tabblock USING gist(the_geom);"
	${PSQL} -c \
		"vacuum analyze tiger_data.${STATE}_tabblock;"
}

function import_bg {
	cd $TIGERDATA/BG

	cleanup_tmp

	for z in tl_*_${FIPS}*_bg.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
	for z in */tl_*_${FIPS}*_bg.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
	cd $TMPDIR;

	${PSQL} -c \
		"CREATE TABLE tiger_data.${STATE}_bg(CONSTRAINT pk_${STATE}_bg PRIMARY KEY (bg_id)) INHERITS(tiger.bg);" 
	${SHP2PGSQL} -c -s 4269 -g the_geom -W "latin1" tl_2012_${FIPS}_bg.dbf tiger_staging.${STATE}_bg | ${PSQL}
	${PSQL} -c \
		"ALTER TABLE tiger_staging.${STATE}_bg RENAME geoid TO bg_id;
		 SELECT loader_load_staged_data(lower('${STATE}_bg'), lower('${STATE}_bg'));"
	${PSQL} -c \
		"ALTER TABLE tiger_data.${STATE}_bg ADD CONSTRAINT chk_statefp CHECK (statefp = '${FIPS}');"
	${PSQL} -c \
		"CREATE INDEX tiger_data_${STATE}_bg_the_geom_gist ON tiger_data.${STATE}_bg USING gist(the_geom);"
	${PSQL} -c \
		"vacuum analyze tiger_data.${STATE}_bg;"
}

function import_zcta5 {
	cd $TIGERDATA/2010

	cleanup_tmp

	for z in tl_*_${FIPS}*_zcta510.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
	for z in */tl_*_${FIPS}*_zcta510.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
	cd $TMPDIR;

	${PSQL} -c \
		"CREATE TABLE tiger_data.${STATE}_zcta5(CONSTRAINT 
		 pk_${STATE}_zcta5 PRIMARY KEY (zcta5ce,statefp), CONSTRAINT uidx_${STATE}_zcta5_gid UNIQUE (gid)) INHERITS(zcta5);"

	for z in *zcta510.dbf; do
		${SHP2PGSQL} -s 4269 -g the_geom -W "latin1" $z tiger_staging.${STATE}_zcta510 | ${PSQL}
		${PSQL} -c "SELECT loader_load_staged_data(lower('${STATE}_zcta510'), lower('${STATE}_zcta5'));"
	done

	${PSQL} -c "ALTER TABLE tiger_data.${STATE}_zcta5 ADD CONSTRAINT chk_statefp CHECK (statefp = '${FIPS}');"
	${PSQL} -c "CREATE INDEX tiger_data_${STATE}_zcta5_the_geom_gist ON tiger_data.${STATE}_zcta5 USING gist(the_geom);"
}

function import_faces {
	cd $TIGERDATA/FACES/

	cleanup_tmp

	for z in tl_*_${FIPS}*_faces.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
	for z in */tl_*_${FIPS}*_faces.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
	cd $TMPDIR;

	${PSQL} -c \
		"CREATE TABLE tiger_data.${STATE}_faces(CONSTRAINT pk_${STATE}_faces PRIMARY KEY (gid)) INHERITS(faces);" 

	for z in *faces.dbf; do 
		${SHP2PGSQL} -s 4269 -g the_geom -W "latin1" $z tiger_staging.${STATE}_faces | ${PSQL} 
		${PSQL} -c \
			"SELECT loader_load_staged_data(lower('${STATE}_faces'), lower('${STATE}_faces'));"
	done

	${PSQL} -c \
		"CREATE INDEX tiger_data_${STATE}_faces_the_geom_gist ON tiger_data.${STATE}_faces USING gist(the_geom);"
	${PSQL} -c \
		"CREATE INDEX idx_tiger_data_${STATE}_faces_tfid ON tiger_data.${STATE}_faces USING btree (tfid);"
	${PSQL} -c \
		"CREATE INDEX idx_tiger_data_${STATE}_faces_countyfp ON tiger_data.${STATE}_faces USING btree (countyfp);"
	${PSQL} -c \
		"ALTER TABLE tiger_data.${STATE}_faces ADD CONSTRAINT chk_statefp CHECK (statefp = '${FIPS}');"
	${PSQL} -c \
		"vacuum analyze tiger_data.${STATE}_faces;"

}


function import_featnames {
	cd $TIGERDATA/FEATNAMES/

	cleanup_tmp

	for z in tl_*_${FIPS}*_featnames.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
	for z in */tl_*_${FIPS}*_featnames.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
	cd $TMPDIR;

	${PSQL} -c \
		"CREATE TABLE tiger_data.${STATE}_featnames(CONSTRAINT pk_${STATE}_featnames PRIMARY KEY (gid)) INHERITS(featnames);
		 ALTER TABLE tiger_data.${STATE}_featnames ALTER COLUMN statefp SET DEFAULT '${FIPS}';" 

	for z in *featnames.dbf; do
		${SHP2PGSQL} -s 4269 -g the_geom -W "latin1" $z tiger_staging.${STATE}_featnames | ${PSQL} 
		${PSQL} -c \
			"SELECT loader_load_staged_data(lower('${STATE}_featnames'), lower('${STATE}_featnames'));"
	done

	${PSQL} -c \
		"CREATE INDEX idx_tiger_data_${STATE}_featnames_snd_name ON tiger_data.${STATE}_featnames USING btree (soundex(name));"
	${PSQL} -c \
		"CREATE INDEX idx_tiger_data_${STATE}_featnames_lname ON tiger_data.${STATE}_featnames USING btree (lower(name));"
	${PSQL} -c \
		"CREATE INDEX idx_tiger_data_${STATE}_featnames_tlid_statefp ON tiger_data.${STATE}_featnames USING btree (tlid,statefp);"
	${PSQL} -c \
		"ALTER TABLE tiger_data.${STATE}_featnames ADD CONSTRAINT chk_statefp CHECK (statefp = '${FIPS}');"
	${PSQL} -c \
		"vacuum analyze tiger_data.${STATE}_featnames;"
}

function import_edges {
	cd $TIGERDATA/EDGES/

	cleanup_tmp

	for z in tl_*_${FIPS}*_edges.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
	for z in */tl_*_${FIPS}*_edges.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
	cd $TMPDIR;

	${PSQL} -c \
		"CREATE TABLE tiger_data.${STATE}_edges(CONSTRAINT pk_${STATE}_edges PRIMARY KEY (gid)) INHERITS(edges);" 

	for z in *edges.dbf; do 
		${SHP2PGSQL} -s 4269 -g the_geom -W "latin1" $z tiger_staging.${STATE}_edges | ${PSQL} 
		${PSQL} -c \
			"SELECT loader_load_staged_data(lower('${STATE}_edges'), lower('${STATE}_edges'));"
	done

	${PSQL} -c \
		"ALTER TABLE tiger_data.${STATE}_edges ADD CONSTRAINT chk_statefp CHECK (statefp = '${FIPS}');"
	${PSQL} -c \
		"CREATE INDEX idx_tiger_data_${STATE}_edges_tlid ON tiger_data.${STATE}_edges USING btree (tlid);"
	${PSQL} -c \
		"CREATE INDEX idx_tiger_data_${STATE}_edgestfidr ON tiger_data.${STATE}_edges USING btree (tfidr);"
	${PSQL} -c \
		"CREATE INDEX idx_tiger_data_${STATE}_edges_tfidl ON tiger_data.${STATE}_edges USING btree (tfidl);"
	${PSQL} -c \
		"CREATE INDEX idx_tiger_data_${STATE}_edges_countyfp ON tiger_data.${STATE}_edges USING btree (countyfp);"
	${PSQL} -c \
		"CREATE INDEX tiger_data_${STATE}_edges_the_geom_gist ON tiger_data.${STATE}_edges USING gist(the_geom);"
	${PSQL} -c \
		"CREATE INDEX idx_tiger_data_${STATE}_edges_zipl ON tiger_data.${STATE}_edges USING btree (zipl);"
	${PSQL} -c \
		"CREATE TABLE tiger_data.${STATE}_zip_state_loc(CONSTRAINT pk_${STATE}_zip_state_loc PRIMARY KEY(zip,stusps,place))
		 INHERITS(zip_state_loc);"
	${PSQL} -c \
		"INSERT INTO tiger_data.${STATE}_zip_state_loc(zip,stusps,statefp,place)
		 SELECT DISTINCT e.zipl, '${STATE}', '${FIPS}', p.name
		 FROM tiger_data.${STATE}_edges AS e INNER JOIN tiger_data.${STATE}_faces AS f ON (e.tfidl = f.tfid OR e.tfidr = f.tfid)
		 INNER JOIN tiger_data.${STATE}_place As p ON(f.statefp = p.statefp AND f.placefp = p.placefp ) WHERE e.zipl IS NOT NULL;"
	${PSQL} -c \
		"CREATE INDEX idx_tiger_data_${STATE}_zip_state_loc_place ON tiger_data.${STATE}_zip_state_loc
		 USING btree(soundex(place));"
	${PSQL} -c \
		"ALTER TABLE tiger_data.${STATE}_zip_state_loc ADD CONSTRAINT chk_statefp CHECK (statefp = '${FIPS}');"
	${PSQL} -c \
		"vacuum analyze tiger_data.${STATE}_edges;"
	${PSQL} -c \
		"vacuum analyze tiger_data.${STATE}_zip_state_loc;"
	${PSQL} -c \
		"CREATE TABLE tiger_data.${STATE}_zip_lookup_base(CONSTRAINT pk_${STATE}_zip_state_loc_city 
		 PRIMARY KEY(zip,state, county, city, statefp)) INHERITS(zip_lookup_base);"
	${PSQL} -c \
		"INSERT INTO tiger_data.${STATE}_zip_lookup_base(zip,state,county,city,statefp) 
		 SELECT DISTINCT e.zipl, '${STATE}', c.name,p.name,'${FIPS}' FROM tiger_data.${STATE}_edges AS e 
		 INNER JOIN tiger.county AS c ON (e.countyfp = c.countyfp AND e.statefp = c.statefp AND e.statefp = '${FIPS}')
		 INNER JOIN tiger_data.${STATE}_faces AS f ON (e.tfidl = f.tfid OR e.tfidr = f.tfid)
		 INNER JOIN tiger_data.${STATE}_place As p ON(f.statefp = p.statefp AND f.placefp = p.placefp ) WHERE e.zipl IS NOT NULL;"
	${PSQL} -c \
		"ALTER TABLE tiger_data.${STATE}_zip_lookup_base ADD CONSTRAINT chk_statefp CHECK (statefp = '${FIPS}');"
	${PSQL} -c \
		"CREATE INDEX idx_tiger_data_${STATE}_zip_lookup_base_citysnd ON tiger_data.${STATE}_zip_lookup_base
		 USING btree(soundex(city));" 
}

function import_addr {
	cd $TIGERDATA/ADDR/

	cleanup_tmp

	for z in tl_*_${FIPS}*_addr.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
	for z in */tl_*_${FIPS}*_addr.zip ; do $UNZIPTOOL -o -d $TMPDIR $z; done
	cd $TMPDIR;

	${PSQL} -c \
		"CREATE TABLE tiger_data.${STATE}_addr(CONSTRAINT pk_${STATE}_addr PRIMARY KEY (gid)) INHERITS(addr);
		 ALTER TABLE tiger_data.${STATE}_addr ALTER COLUMN statefp SET DEFAULT '${FIPS}';" 
	for z in *addr.dbf; do 
		${SHP2PGSQL} -s 4269 -g the_geom -W "latin1" $z tiger_staging.${STATE}_addr | ${PSQL} 
		${PSQL} -c \
			"SELECT loader_load_staged_data(lower('${STATE}_addr'), lower('${STATE}_addr'));"
	done

	${PSQL} -c \
		"ALTER TABLE tiger_data.${STATE}_addr ADD CONSTRAINT chk_statefp CHECK (statefp = '${FIPS}');"
	${PSQL} -c \
		"CREATE INDEX idx_tiger_data_${STATE}_addr_least_address ON tiger_data.${STATE}_addr
		 USING btree (least_hn(fromhn,tohn) );"
	${PSQL} -c \
		"CREATE INDEX idx_tiger_data_${STATE}_addr_tlid_statefp ON tiger_data.${STATE}_addr USING btree (tlid, statefp);"
	${PSQL} -c \
		"CREATE INDEX idx_tiger_data_${STATE}_addr_zip ON tiger_data.${STATE}_addr USING btree (zip);"
	${PSQL} -c \
		"CREATE TABLE tiger_data.${STATE}_zip_state(CONSTRAINT pk_${STATE}_zip_state PRIMARY KEY(zip,stusps))
		 INHERITS(zip_state);"
	${PSQL} -c \
		"INSERT INTO tiger_data.${STATE}_zip_state(zip,stusps,statefp) SELECT DISTINCT zip, '${STATE}', '${FIPS}'
		 FROM tiger_data.${STATE}_addr WHERE zip is not null;"
	${PSQL} -c \
		"ALTER TABLE tiger_data.${STATE}_zip_state ADD CONSTRAINT chk_statefp CHECK (statefp = '${FIPS}');"
	${PSQL} -c \
		"vacuum analyze tiger_data.${STATE}_addr;"
}


cleanup_tmp

#import_state
#import_county

import_place
import_cousub
import_tract
import_tabblock
import_bg
import_zcta5
import_faces
import_featnames
import_edges
import_addr
