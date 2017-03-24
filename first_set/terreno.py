
class Terreno:

    def __init__(self, cod_pr, cod_man, lado_manz, perimetro, area, geometria):
        self.cod_pr = cod_pr
        self.cod_man = cod_man
        self.lado = lado_manz
        self.perimetro = perimetro
        self.area = area
        self.geometria = geometria


    def __repr__(self):
        print (" Predio con Codigo {} y ubicado en {}".format(self.cod_pr, self.lado))

    def save_terreno_to_db(self, cursor):
        cursor.execute('INSERT INTO geo_parcels(cod_pr, cod_man, lado, shape_leng, shape_area, the_geom, updated_at) '
                       'VALUES (%s, %s, %s, %s, %s, st_geomfromtext(%s, SRID), now())',
                       (self.cod_pr, self.cod_man, self.lado, self.perimetro, self.area, self.geometria))