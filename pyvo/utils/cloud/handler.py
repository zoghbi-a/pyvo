# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""
Cloud-related utils
"""
import json

from astropy.table import Table, unique
from astropy.io import votable
import pyvo


from .ap import APContainer, AccessPoint, AWSAccessPoint

# useful global variables
ACCESS_POINTS = [
    AccessPoint,
    AWSAccessPoint
]
class_mapper = {ap.name: ap for ap in ACCESS_POINTS}


def process_json_column(record, colname='cloud_access'):
    """Look for and process any cloud information in a json column
    
    record: dal.Record
    """
    # if no cloud_access column, there is nothing to do
    try:
        jsontxt = record[colname]
    except KeyError:
        return []

    
    desc = json.loads(jsontxt)

    # search for the known access types in desc
    access_points = []
    for ap_name, APclass in class_mapper.items():

        if ap_name not in desc:
            continue

        # TEMPORARY
        if 'access' in desc[ap_name]:
            del desc[ap_name]['access']

        # access point parameters
        ap_params = desc[ap_name]
        # add access meta data, if any
        #ap_params.update(self.access_meta[ap_name])
        new_ap = APclass(**ap_params)
        access_points.append(new_ap)
    
    return access_points


def process_datalinks(record):
    """Look for and process access point in datalinks

    record: dal.Record

    """

    # do we have datalinks?
    try:
        dlinks = record._results.get_adhocservice_by_ivoid(
            'ivo://ivoa.net/std/datalink'
        )
    except (pyvo.DALServiceError, AttributeError) as err:
        return []

    # input parameters for the datalink call
    input_params = pyvo.dal.adhoc._get_input_params_from_resource(dlinks)
    dl_col_id = [p.ref for p in input_params.values() if p.ref is not None]
    #from IPython import embed; embed();exit(0)

    
    access_points = []
    
    # proceed only if we have a PARAM named source, 
    if 'source' in input_params.keys():
        # we have a 'source' element, process it
        source_elem  = input_params['source']

        # list the available options in the `source` element:
        access_options = source_elem.values.options
        for description,option in access_options:

            #log.debug(f'-- datalink option: {option}: {description}')

            # TEMPORARY
            option = option.replace('main-server', 'prem')
            if option == 'prem': continue

            soption = option.split(':')                
            query = pyvo.dal.adhoc.DatalinkQuery.from_resource(
                record, dlinks, 
                source=option
            )

            dl_result = query.execute()
            dl_table = dl_result.to_table()

            ap_type = option.split(':')[0]
            if ap_type in class_mapper.keys():
                ApClass = class_mapper[ap_type]
                #ap_meta = self.access_meta[ap_type]
                dl_res = dl_table[dl_table['ID'] == record[dl_col_id[0]]]
                for dl_row in dl_res:
                    # TODO: ensure that all ApClass's accept uri
                    ap = ApClass(uri=dl_row['access_url'])
                    access_points.append(ap)
    return access_points

def process_ucds(record):
    """look for columns with 'meta.ref.aws', meta.ref.gc etc
        that simply have cloud uri's

    record: dal.Record

    """
    
    access_points = []
    for ap_type, ApClass in class_mapper.items():
        uri = record.getbyucd(f'meta.ref.{ap_type}')
        if uri is not None:
            ap = ApClass(uri=uri)
            access_points.append(ap)
    return access_points

                    
class CloudRecordMixin:
    """
    Mixin for cloud access functionality
    """
    
    def _process_cloud_record(self):
        """prepare cloud information"""
        
        # add a default on-prem access point
        access_points = APContainer()
        url = self.getdataurl()
        if url is not None:
            access_points.add_access_point(AccessPoint(url))
        
        ## ----------------------- ##
        ## add cloud access points ##
        
        # process the json column if it exists
        json_ap = process_json_column(self)
        if len(json_ap) == 0: 
            print('--- No json column ---')
        else:
            print('--- Processed json column ---')
        access_points.add_access_point(json_ap)
        
        # process datalinks if they exist
        dl_ap = process_datalinks(self)
        if len(dl_ap) == 0: 
            print('--- No datalinks ---')
        else:
            print('--- Processed datalinks ---')
        access_points.add_access_point(dl_ap)
        
        # look for columns with 'meta.ref.aws', meta.ref.gc etc
        # that simply have cloud uri's
        uri_ap = process_ucds(self)
        if len(uri_ap) == 0: 
            print('--- No direct uri ---')
        else:
            print('--- Processed direct uri ---')
        access_points.add_access_point(uri_ap)
        
        self.access_points = access_points
        
    
    
    def get_cloud_uri(self, provider='aws'):
        """
        Retrun the cloud uri for the dataset which can be used to retrieve 
        the dataset in this record. None is returne if no cloud information
        is available
        
        provier: aws, azure, gc etc
        """
        
        # do we have a access_points?
        try:
            access_points = self.access_points
        except AttributeError:
            self._process_cloud_record()
        
        for fieldname in self._results.fieldnames:
            field = self._results.getdesc(fieldname)
            if ( field.ucd 
                and "meta.dataset" in field.ucd 
                and f"meta.ref.{provider}" in field.ucd ):
                
                out = self[fieldname]
                if isinstance(out, bytes):
                    out = out.decode('utf-8')
                return out
        return None