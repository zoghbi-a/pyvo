
import sys
import os

#sys.path.insert(0, os.path.dirname(__file__) + '/build/lib/')

import pyvo
import astropy.coordinates as coord
from astropy.io import votable

pos = coord.SkyCoord.from_name("ngc 4151")
query_url = 'https://heasarc.gsfc.nasa.gov/xamin_aws/vo/sia?table=chanmaster&resultmax=2&'

res = pyvo.dal.sia.search(query_url, pos=pos, size=0.0)


## ----------------------------------------- ##
## Use case 1: Working with a single Record. ##
# Calling enable_cloud triggers processing of cloud information
r = res[0]
r.enable_cloud()

# this is a summary of the access point
r.access_points.summary()

# we can call the download method on the record
print('\n-- download from prem --')
path = r.download('prem')
print(path)

print('\n-- download from aws --')
path = r.download('aws')
print(path)


## ----------------------------------------- ##
## Use case 2: Working with the whole Result ##

# again, calling enable_cloud will process all the
# cloud information in the result product
res.enable_cloud()

# access point for individual rows are now available
res.access_points[0].summary()

# we can donwload all records
print('\n-- download from prem --')
paths = res.download('prem')

print('\n-- download from aws --')
paths = res.download('aws')