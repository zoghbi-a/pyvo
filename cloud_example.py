
import sys
import os

#sys.path.insert(0, os.path.dirname(__file__) + '/build/lib/')

import pyvo
import astropy.coordinates as coord
from astropy.io import votable

pos = coord.SkyCoord.from_name("ngc 4151")
query_url = 'https://heasarc.gsfc.nasa.gov/xamin_aws/vo/sia?table=chanmaster&resultmax=2&'

res = pyvo.dal.sia.search(query_url, pos=pos, size=0.0)

print('\n\n++++ Extracting cloud information from the original query result ++++')
r = res[0]
r.access_points.summary()

# inject s3 column by hand
from astropy.table import Column
tab = res.to_table()
s3 = [x.replace('https://heasarc.gsfc.nasa.gov', 's3://nasa-heasarc') for x in tab['access_url']]
tab.add_column(Column(s3, name='aws', meta={'ucd':'meta.ref.aws'}))
res = pyvo.dal.SIAResults(votable.from_table(tab))
# end injection 

print('\n\n++++ Extracting cloud information after injecting direct s3 address ++++')
r = res[0]
r.access_points.summary()

#from IPython import embed; embed()