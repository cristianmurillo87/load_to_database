import psycopg2.extras
import os
from time import time
from cronometro import Cronometro

es = r"path_to_file"
delimiter = ";"

conn = psycopg2.connect("dbname='xxxxxx' user='xxxxx' host='xxxxxx' password='xxxxxxx'")
conn.autocommit = True
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

start = time()
print("Iniciando actualizacion de tablas y capas auxiliares")


cur.execute("SELECT actualizar(%s, %s);", (es, delimiter))

cur.execute("SELECT vista_est();")


cur.execute("DROP TABLE IF EXISTS xxxxxxx;")
cur.execute("CREATE TABLE xxxxxx "
            "AS SELECT lado, st_union(the_geom) AS the_geom FROM parcels  "
            "GROUP BY lado;")
cur.execute("ALTER TABLE yyyy ADD COLUMN id SERIAL;")
cur.execute("ALTER TABLE yyyy ADD PRIMARY KEY (id);")


print("Actualizacion terminada")
end = time()

cr = Cronometro(start, end)
cr.calculate_elapsed_time()

conn.close()
