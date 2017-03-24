import psycopg2
import psycopg2.extras
import arcpy
import os
from time import time
from terreno import Terreno
from cronometro import Cronometro


arcpy.env.workspace = r"Database Connections\My Database Connection.sde"
data = "DATABASE.DBO.MY_TABLE"
##cod_predio, direccion, cod_act, cod_manzana, lado_manz, perimetro, area, geometria
print("Eliminando tabla geo_parcels")
conn = psycopg2.connect("dbname='xxxxx' user='xxxxx' host='111.11.11.111' password='xxxxxxx'")
conn.autocommit = True
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
cur.execute("DROP TABLE IF EXISTS geo_parcels CASCADE;"
            "CREATE TABLE geo_parcels("
            "gid serial, "
            "cod_pr character varying(14), "
            "cod_act character varying(8), "
            "cod_man character varying(50), "
            "lado_man character varying(10), "
            "shape_leng numeric, "
            "shape_area numeric,"
            "the_geom geometry,"
            "updated_at date,"
            "CONSTRAINT parcels_pk PRIMARY KEY (gid),"
            "CONSTRAINT enforce_dims_the_geom CHECK (st_ndims(the_geom) = 2),"
            "CONSTRAINT enforce_geotype_the_geom CHECK (geometrytype(the_geom) = 'MULTIPOLYGON'::text OR the_geom IS NULL),"
            "CONSTRAINT enforce_srid_the_geom CHECK (st_srid(the_geom) = 6249)"
            ")"
            "WITH ("
            "OIDS = TRUE"
            "); "
            "CREATE UNIQUE INDEX ind_parcels ON geo_parcels USING btree(gid);"
            "CREATE INDEX indg_parcels ON geo_parcels USING gist(the_geom);"
            )
print("Tabla geo_parcels eliminada")

start = time()
with arcpy.da.SearchCursor(data, ["field1", "field2", "field3", "SHAPE@LENGTH", "SHAPE@AREA", "SHAPE@WKT"]) as cursor:
    for row in cursor:
        terreno = Terreno(row[0], row[1], row[2], row[3], row[4], row[5])
        terreno.save_terreno_to_db(cur)
        print ("Guardando terreno {} en la base de datos".format(row[0]))

end = time()

cr = Cronometro(start, end)
cr.calculate_elapsed_time()
start = time()
print("Ejecutando...")
cur.execute("select actualizar_tablas()")
print("Actualizacion terminada")
end = time()

cr = Cronometro(start, end)
cr.calculate_elapsed_time()

conn.close()