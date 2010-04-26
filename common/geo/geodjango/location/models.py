###########../Data/ShapeFiles/Counties/co99_d00.shp:

# This is an auto-generated Django model module created by ogrinspect.
from django.contrib.gis.db import models
from django.contrib.gis.utils import LayerMapping
import os

class USCounties(models.Model):
    area = models.FloatField()
    perimeter = models.FloatField()
    co99_d00_field = models.FloatField()
    co99_d00_i = models.FloatField()
    state_code = models.CharField(max_length=2)
    county_code = models.CharField(max_length=3)
    county_name = models.CharField(max_length=90)
    lsad = models.CharField(max_length=2)
    lsad_trans = models.CharField(max_length=50)
    geom = models.MultiPolygonField(srid=4326)
    objects = models.GeoManager()

# Auto-generated `LayerMapping` dictionary for Counties model
USCounties_mapping = {
    'area' : 'AREA',
    'perimeter' : 'PERIMETER',
    'co99_d00_field' : 'CO99_D00_',
    'co99_d00_i' : 'CO99_D00_I',
    'state_code' : 'STATE',
    'county_code' : 'COUNTY',
    'county_name' : 'NAME',
    'lsad' : 'LSAD',
    'lsad_trans' : 'LSAD_TRANS',
    'geom' : 'MULTIPOLYGON',
}


USCounties_shp = os.path.abspath(os.path.join(os.path.dirname(__file__),'../../../../backbackend/Data/ShapeFiles/Counties/co99_d00.shp'))

###########../Data/ShapeFiles/States/st99_d00.shp:

class USStates(models.Model):
    area = models.FloatField()
    perimeter = models.FloatField()
    st99_d00_field = models.FloatField()
    st99_d00_i = models.FloatField()
    state_code = models.CharField(max_length=2)
    state_name = models.CharField(max_length=90)
    lsad = models.CharField(max_length=2)
    region = models.CharField(max_length=1)
    division = models.CharField(max_length=1)
    lsad_trans = models.CharField(max_length=50)
    geom = models.MultiPolygonField(srid=4326)
    objects = models.GeoManager()

# Auto-generated `LayerMapping` dictionary for States model
USStates_mapping = {
    'area' : 'AREA',
    'perimeter' : 'PERIMETER',
    'st99_d00_field' : 'ST99_D00_',
    'st99_d00_i' : 'ST99_D00_I',
    'state_code' : 'STATE',
    'state_name' : 'NAME',
    'lsad' : 'LSAD',
    'region' : 'REGION',
    'division' : 'DIVISION',
    'lsad_trans' : 'LSAD_TRANS',
    'geom' : 'MULTIPOLYGON',
}


USStates_shp = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../backbackend/Data/ShapeFiles/States/st99_d00.shp'))

###########../Data/ShapeFiles/FiveDigitZCTAs/zt01_d00.shp:

# This is an auto-generated Django model module created by ogrinspect.
from django.contrib.gis.db import models

class FiveDigitZCTAs(models.Model):
    area = models.FloatField()
    perimeter = models.FloatField()
    zt_field = models.FloatField()
    zt_i = models.FloatField()
    zcta5_code = models.CharField(max_length=5)
    zcta5_name = models.CharField(max_length=90)
    lsad = models.CharField(max_length=2)
    lsad_trans = models.CharField(max_length=50)
    geom = models.MultiPolygonField(srid=4326)
    objects = models.GeoManager()

# Auto-generated `LayerMapping` dictionary for FiveDigitZCTAs model
FiveDigitZCTAs_mapping = {
    'area' : 'AREA',
    'perimeter' : 'PERIMETER',
    'zcta5_code' : 'ZCTA',
    'zcta5_name' : 'NAME',
    'lsad' : 'LSAD',
    'lsad_trans' : 'LSAD_TRANS',
    'geom' : 'MULTIPOLYGON',
}

FiveDigitZCTAspath = os.path.abspath(os.path.join(os.path.dirname(__file__),'../../../../backbackend/Data/ShapeFiles/FiveDigitZCTAs/'))

FiveDigitZCTAs_shp = [FiveDigitZCTAspath + '/' + x for x in os.listdir(FiveDigitZCTAspath) if x.endswith('.shp')]

###########../Data/ShapeFiles/MSA/cb99_03c.shp:

# This is an auto-generated Django model module created by ogrinspect.
from django.contrib.gis.db import models

class CBSA(models.Model):
    cbsa_code = models.CharField(max_length=9)
    cbsa_name = models.CharField(max_length=56)
    type = models.CharField(max_length=29)
    status = models.CharField(max_length=11)
    geocode = models.CharField(max_length=12)
    geom = models.MultiPolygonField(srid=4326)
    objects = models.GeoManager()

# Auto-generated `LayerMapping` dictionary for MSA model
CBSA_mapping = {
    'cbsa_code' : 'CBSA',
    'cbsa_name' : 'CBSA_NAME',
    'type' : 'TYPE',
    'status' : 'STATUS',
    'geocode' : 'GEOCODE',
    'geom' : 'MULTIPOLYGON',
}

CBSA_shp = os.path.abspath(os.path.join(os.path.dirname(__file__),'../../../../backbackend/Data/ShapeFiles/MSA/cb99_03c.shp'))


###########../Data/ShapeFiles/MSA/cs99_03c.shp:

# This is an auto-generated Django model module created by ogrinspect.
from django.contrib.gis.db import models

class CSA(models.Model):
    csa_code = models.CharField(max_length=9)
    status = models.CharField(max_length=11)
    csa_name = models.CharField(max_length=54)
    geocode = models.CharField(max_length=12)
    geom = models.MultiPolygonField(srid=4326)
    objects = models.GeoManager()

# Auto-generated `LayerMapping` dictionary for MSA model
CSA_mapping = {
    'csa_code' : 'CSA',
    'status' : 'STATUS',
    'csa_name' : 'CSA_NAME',
    'geocode' : 'GEOCODE',
    'geom' : 'MULTIPOLYGON',
}

CSA_shp = os.path.abspath(os.path.join(os.path.dirname(__file__),'../../../../backbackend/Data/ShapeFiles/MSA/cs99_03c.shp'))



###########../Data/ShapeFiles/MSA/md99_03c.shp:

# This is an auto-generated Django model module created by ogrinspect.
from django.contrib.gis.db import models

class METDIV(models.Model):
    metdiv_code = models.CharField(max_length=12)
    status = models.CharField(max_length=11)
    metdiv_name = models.CharField(max_length=55)
    geocode = models.CharField(max_length=12)
    geom = models.MultiPolygonField(srid=4326)
    objects = models.GeoManager()

# Auto-generated `LayerMapping` dictionary for MSA model
METDIV_mapping = {
    'metdiv_code' : 'METDIV',
    'status' : 'STATUS',
    'metdiv_name' : 'METDIV_NAM',
    'geocode' : 'GEOCODE',
    'geom' : 'MULTIPOLYGON',
}

METDIV_shp = os.path.abspath(os.path.join(os.path.dirname(__file__),'../../../../backbackend/Data/ShapeFiles/MSA/md99_03c.shp'))


class CensusTracts(models.Model):
    area = models.FloatField()
    perimeter = models.FloatField()
    state_code = models.CharField(max_length=2)
    county_code = models.CharField(max_length=3)
    tract_code = models.CharField(max_length=6)
    tract_name = models.CharField(max_length=90)
    lsad = models.CharField(max_length=2)
    lsad_trans = models.CharField(max_length=50)
    geom = models.MultiPolygonField(srid=4326)
    objects = models.GeoManager()

# Auto-generated `LayerMapping` dictionary for CensusTracts model
CensusTracts_mapping = {
    'area' : 'AREA',
    'perimeter' : 'PERIMETER',
    'state_code' : 'STATE',
    'county_code' : 'COUNTY',
    'tract_code' : 'TRACT',
    'tract_name' : 'NAME',
    'lsad' : 'LSAD',
    'lsad_trans' : 'LSAD_TRANS',
    'geom' : 'MULTIPOLYGON',
}


CensusTractspath = os.path.abspath(os.path.join(os.path.dirname(__file__),'../../../../backbackend/Data/ShapeFiles/CensusTracts/'))

CensusTracts_shp = [CensusTractspath + '/' + x for x in os.listdir(CensusTractspath) if x.endswith('.shp')]
