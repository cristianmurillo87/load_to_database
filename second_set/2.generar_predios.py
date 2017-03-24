import psycopg2
import psycopg2.extras
import arcpy
import os
from time import time
from terreno import Predio
from cronometro import Cronometro


arcpy.env.workspace = r"Database Connections\My Database Connection.sde"
data = "BD_ESTRATIFICACION_URBANA.DBO.BASE_CATASTRAL"

print("Eliminando tabla predios")
conn = psycopg2.connect("dbname='xxxxxx' user='xxxxxx' host='xxx.xx.xx.xxx' password='xxxxxxxx'")
conn.autocommit = True
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
cur.execute("DROP TABLE IF EXISTS predios CASCADE;"
            "CREATE TABLE preds("
            "gid serial, "
			"com character varying(2), "
			"barr character varying(2), "
			"manz character varying(4), "
			"lado character varying(1), "
            "cod_pr character varying(14), "
			"direccion character varying(255), "
            "cod_act character varying(8), "
            "cod_pred_n character varying(30), "
			"numnal_old character varying(30), "
            "num_pr character varying(14), "
			"cod_man character varying(8), "
			"lado_m character varying(9), "
            "updated_at date,"
            "CONSTRAINT predios_pkey PRIMARY KEY (gid)"
            ")"
            "WITH ("
            "OIDS = FALSE"
            "); "
            "CREATE UNIQUE INDEX ind_preds ON preds USING btree(gid);"
            )
print("Tabla predios actualizada")
start = time()
with arcpy.da.SearchCursor(data, ["field", "field", "field", "field", "field", "field", "field", "field", "field", "field", "field", "field"]) as cursor:
    for row in cursor:
        predio = Predio(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11])
        predio.save_predio_to_db(cur)
        print ("Guardando {} en la base de datos".format(row[7]))

end = time()

cr = Cronometro(start, end)
cr.calculate_elapsed_time()

conn.close()

	
