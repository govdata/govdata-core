import pymongo.son as son
from common.utils import is_string_like, ListUnion, uniqify, Flatten

#see http://www.census.gov/population/www/metroareas/metrodef.html
#see http://www.census.gov/geo/www/ansi/ansi.html for FIPS
#see http://www.census.gov/geo/www/cob/bdy_files.html for boundary files
#see http://www2.census.gov/cgi-bin/shapefiles2009/national-files for more boundary files
#see http://wiki.openstreetmap.org/wiki/Karlsruhe_Schema
#see http://wiki.openstreetmap.org/wiki/Map_Features#Places

SPACE_CODE_MAP = [('a','Address',),
('A','Area Code'),
('c','County'),
('C','country'),
('d','Public Use Microdata Area -- 1%'),
('e','Public Use Microdata Area -- 5%'),
('g':'Congressional District'),
('I','Incoporated Places'),
('i','Island'),
('j':'County Subdivisions'),
('k','School District -- Unified'),
('l','State Legislative District -- Lower')
('m','Metropolitan/Micropolitan Statistical Area')
('n','New England City and Town Areas'),
('O','Continent')
('p','Postal Code')
('q','School District -- Elementary'),
('Q','School District -- Secondary'),
('r','Region')
('S','State Code'),
('s','State')
('t','Town'),
('T','Census Tract')
('u','Urban Area'),
('v','Voting District'),
('V','Village'),
('W','City'),
('X','Undefined'),
('Z','5-Digit ZCTA'),
('z','3-Digit ZCTA'),
]

(SPACE_CODES,SPACE_DIVISIONS) = zip(*SPACE_CODE_MAP)
SPACE_DIVISIONS = dict(zip(SPACE_CODES,SPACE_DIVISIONS))

