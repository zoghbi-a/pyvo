
import sys
import os

#sys.path.insert(0, os.path.dirname(__file__) + '/build/lib/')

import pyvo
import astropy.coordinates as coord
from astropy.io import votable

pos = coord.SkyCoord.from_name("ngc 4151")
query_url = 'https://heasarc.gsfc.nasa.gov/xamin_aws/vo/sia?table=chanmaster&resultmax=2&'

res = pyvo.dal.sia.search(query_url, pos=pos, size=0.0)

# requesting a row, triggers the cloud information processing
r = res[0]

# this a summary of the access point
r.access_points.summary()
