import psycopg2.extras
import arcpy
from time import time
from terreno import Terreno
from cronometro import Cronometro


arcpy.env.workspace = r"Database Connections\My Database Connection.sde"
data = "BD.DBO.TABLE"

print("Eliminando tabla ...")
conn = psycopg2.connect("dbname='xxxxxx' user='xxxxxx' host='xxx.xx.xx.xxx' password='xxxxxxxx'")
conn.autocommit = True
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
cur.execute("DROP TABLE IF EXISTS parcels CASCADE;"
            "CREATE TABLE parcels("
            "gid serial, "
            "cod_pr character varying(14), "
			"direccion character varying(255), "
            "cod_act character varying(8), "
            "manz character varying(50), "
            "lado character varying(10), "
            "shape_leng numeric, "
            "shape_area numeric,"
            "the_geom geometry,"
            "updated_at date,"
            "CONSTRAINT parcels_pkey PRIMARY KEY (gid),"
            "CONSTRAINT enforce_dims_the_geom CHECK (st_ndims(the_geom) = 2),"
            "CONSTRAINT enforce_geotype_the_geom CHECK (geometrytype(the_geom) = 'MULTIPOLYGON'::text OR the_geom IS NULL),"
            "CONSTRAINT enforce_srid_the_geom CHECK (st_srid(the_geom) = SRID)"
            ")"
            "WITH ("
            "OIDS = FALSE"
            "); "
            "CREATE UNIQUE INDEX ind_parcels ON parcels USING btree(gid);"
            "CREATE INDEX indg_parcels ON parcels USING gist(the_geom);"
            )
print("Tabla parcels actualizada")
start = time()
with arcpy.da.SearchCursor(data, ["field1", "field2", "field3", "field4", "SHAPE@LENGTH", "SHAPE@AREA", "SHAPE@WKT"]) as cursor:
    for row in cursor:
        terreno = Terreno(row[0], row[1], row[2], row[3], row[4], row[5], row[6])
        terreno.save_terreno_to_db(cur)
        print ("Guardando {} en la base de datos".format(row[0]))

end = time()

cr = Cronometro(start, end)
cr.calculate_elapsed_time()

conn.close()
