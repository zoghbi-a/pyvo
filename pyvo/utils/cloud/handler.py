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


def process_json_column(dalProduct, colname='cloud_access', **access_meta):
    """Look for and process any cloud information in a json column
    
    dalProduct: dal.Record or dal.DALResults
    """
    isRecord = isinstance(dalProduct, pyvo.dal.Record)
    
    # if no cloud_access column, there is nothing to do    
    try:
        jsontxt = dalProduct[colname]
    except KeyError:
        # no json column, return
        return None
    
    if isRecord:
        # case of dal.Record
        jsontxt_list = [jsontxt]
    else:
        # case of dal.DALResult
        jsontxt_list = [_ for _ in jsontxt]
    
    
    access_points_list = []
    for jsontxt in jsontxt_list:
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
            ap_params.update(access_meta[ap_name])
            new_ap = APclass(**ap_params)
            access_points.append(new_ap)
        access_points_list.append(access_points)
    
    # if isRecord, the list has length 1, so we don't return the full list
    if isRecord:
        access_points_list = access_points_list[0]
        
    return access_points_list


def process_datalinks(dalProduct, **access_meta):
    """Look for and process access point in datalinks

    dalProduct: dal.Record or dal.DALResults

    """
    isRecord = isinstance(dalProduct, pyvo.dal.Record)
    
    # do we have datalinks?
    try:
        result = dalProduct._results if isRecord else dalProduct
        _datalink = result.get_adhocservice_by_ivoid(
            'ivo://ivoa.net/std/datalink'
        )
    except (pyvo.DALServiceError, AttributeError):
        # No datalinks; return
        return None
    

    records = [dalProduct] if isRecord else [_ for _ in dalProduct]
    nrec = len(records)

    # input parameters for the datalink call
    input_params = pyvo.dal.adhoc._get_input_params_from_resource(_datalink)
    dl_col_id = [p.ref for p in input_params.values() if p.ref is not None]

    
    
    # proceed only if we have a PARAM named source, 
    access_points_list = None
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

            # TODO: consider including batch_size simialr to 
            # DatalinkResultsMixin.iter_datalinks
            query = pyvo.dal.adhoc.DatalinkQuery.from_resource(
                records, _datalink, 
                source=option
            )
            
            dl_result = query.execute()
            dl_table = dl_result.to_table()
            
            if access_points_list is None:
                access_points_list = [[] for _ in range(nrec)]
                
            ap_type = option.split(':')[0]
            access_points = []
            if ap_type in class_mapper.keys():
                ApClass = class_mapper[ap_type]
                for irow in range(nrec):
                    dl_res = dl_table[dl_table['ID'] == records[irow][dl_col_id[0]]]
                    for dl_row in dl_res:
                        ap = ApClass(uri=dl_row['access_url'], **access_meta[ap_type])
                        access_points_list[irow].append(ap)
    # if isRecord, the list has length 1, so we don't return the full list
    if isRecord and access_points_list is not None:
        access_points_list = access_points_list[0]
        
    return access_points_list


def process_ucds(dalProduct, **access_meta):
    """look for columns with 'meta.ref.aws', meta.ref.gc etc
        that simply have cloud uri's

    dalProduct: dal.Record or dal.DALResults

    """
    isRecord = isinstance(dalProduct, pyvo.dal.Record)
    
    records = [dalProduct] if isRecord else [_ for _ in dalProduct]
    nrec = len(records)
    
    
    access_points_list = None
    for irow in range(nrec):
        access_points = []
        for ap_type, ApClass in class_mapper.items():
            uri = records[irow].getbyucd(f'meta.ref.{ap_type}')
            if uri is not None:
                ap = ApClass(uri=uri, **access_meta[ap_type])
                access_points.append(ap)
        if access_points_list is None:
            access_points_list = []
        access_points_list.append(access_points)
    
    # if isRecord, the list has length 1, so we don't return the full list
    if isRecord and access_points_list is not None:
        access_points_list = access_points_list[0]
        
    return access_points_list

                    
class CloudRecordMixin:
    """
    Mixin for cloud access functionality
    """
    
    def enable_cloud(self, refresh=False, **kwargs):
        """prepare cloud information
        
        Parameters
        ----------
        refresh: bool
            If True, re-work out the access points. May be needed
            when changing credentials for example.
        
        Keywords:
        ---------
        meta data needed to download the data, such as authentication profile
        which will be used to create access points. 

        prem:
            No keywords needed
        aws:
            aws_profile : str
                name of the user's profile for credentials in ~/.aws/config
                or ~/.aws/credentials. Use to authenticate the AWS user with boto3.
        
        
        """
                
        
        if (
            hasattr(self._results, 'access_points') and 
            self._results.access_points[self._index] is not None
            and not refresh
           ):
            self.access_points = self._results.access_points[self._index]
        else:
            
            # extract any access meta information from kwargs
            access_meta = {}
            for ap_name,apClass in class_mapper.items():
                access_meta[ap_name] = apClass.access_meta(**kwargs)
            
            # add a default on-prem access point
            access_points = APContainer()
            url = self.getdataurl()
            if url is not None:
                access_points.add_access_point(AccessPoint(url))

            ## ----------------------- ##
            ## add cloud access points ##

            # process the json column if it exists
            json_ap = process_json_column(self, **access_meta)
            if len(json_ap) == 0: 
                print('--- No json column ---')
            else:
                print('--- Processed json column ---')
            access_points.add_access_point(json_ap)

            # process datalinks if they exist
            dl_ap = process_datalinks(self, **access_meta)
            if len(dl_ap) == 0: 
                print('--- No datalinks ---')
            else:
                print('--- Processed datalinks ---')
            access_points.add_access_point(dl_ap)

            # look for columns with 'meta.ref.aws', meta.ref.gc etc
            # that simply have cloud uri's
            uri_ap = process_ucds(self, **access_meta)
            if len(uri_ap) == 0: 
                print('--- No direct uri ---')
            else:
                print('--- Processed direct uri ---')
            access_points.add_access_point(uri_ap)

            self.access_points = access_points
            
            if not hasattr(self._results, 'access_points'):
                self._results.access_points = [None for _ in range(len(self._results))]
            self._results.access_points[self._index] = access_points
        
    
    
    def get_cloud_uris(self, provider='aws'):
        """
        Retrun the cloud uri for the dataset which can be used to retrieve 
        the dataset in this record. None is returne if no cloud information
        is available
        
        provider: prem, aws, azure, gc etc
        """
        
        # do we have a access_points?
        try:
            access_points = self.access_points
        except AttributeError:
            self.enable_cloud()
            access_points = self.access_points
        
        return access_points.uris(provider)
    
    
    def download(self, provider='aws'):
        """
        Download the data from the given provider
        
        Parameters:
        ----------
        provider: str
            prem, aws, azure, gc etc
        """
        
        # do we have a access_points?
        try:
            access_points = self.access_points
        except AttributeError:
            self.enable_cloud()
            access_points = self.access_points
        if provider not in access_points.access_points.keys():
            raise ValueError(f'No access point available for provider {provider}.')
        aps = access_points[provider]
        path = None
        msgs = []
        # return the first access point that is accessible.
        # if none, print the returned message
        # TODO: we can make this more sophisticated by selecting
        # by region etc.
        for ap in aps:
            accessible, msg = ap.is_accessible()
            if accessible:
                path = ap.download()
                break
            else:
                msgs.append(msg)
        if path is None:
            for ap,msg in zip(aps, msgs):
                print(f'\n{ap}:\n\t*** {msg} ***')
        return path
        

class CloudResultMixin:
    """
    Mixin for cloud access functionality to go along with DALResults
    """
    
        
    def enable_cloud(self, refresh=False, **kwargs):
        """prepare cloud information
        
        Parameters
        ----------
        refresh: bool
            If True, re-work out the access points. May be needed
            when changing credentials for example.
        
        Keywords:
        ---------
        meta data needed to download the data, such as authentication profile
        which will be used to create access points. 

        prem:
            No keywords needed
        aws:
            aws_profile : str
                name of the user's profile for credentials in ~/.aws/config
                or ~/.aws/credentials. Use to authenticate the AWS user with boto3.
        
        """
        
        if (
            hasattr(self, 'access_points') and 
            all([ap is not None for ap in self.access_points])
            and not refresh
        ):
            return
        
        # extract any access meta information from kwargs
        access_meta = {}
        for ap_name,apClass in class_mapper.items():
            access_meta[ap_name] = apClass.access_meta(**kwargs)
        
        # add a default on-prem access point
        nrec = len(self)
        access_points_list = []
        for irec in range(nrec):
            aps = APContainer()
            url = self[irec].getdataurl()
            if url is not None:
                aps.add_access_point(AccessPoint(url))
            access_points_list.append(aps)

        ## ----------------------- ##
        ## add cloud access points ##

        # process the json column if it exists
        json_ap = process_json_column(self, **access_meta)
        if json_ap is None or len(json_ap) == 0: 
            print('--- No json column ---')
        else:
            print('--- Processed json column ---')
            for irec in range(nrec):
                access_points_list[irec].add_access_point(json_ap[irec])

        # process datalinks if they exist
        dl_ap = process_datalinks(self, **access_meta)
        if dl_ap is None or len(dl_ap) == 0: 
            print('--- No datalinks ---')
        else:
            print('--- Processed datalinks ---')
            for irec in range(nrec):
                access_points_list[irec].add_access_point(dl_ap[irec])

        # look for columns with 'meta.ref.aws', meta.ref.gc etc
        # that simply have cloud uri's
        uri_ap = process_ucds(self, **access_meta)
        if uri_ap is None or len(uri_ap) == 0: 
            print('--- No direct uri ---')
        else:
            print('--- Processed direct uri ---')
            for irec in range(nrec):
                access_points_list[irec].add_access_point(uri_ap[irec])

        self.access_points = access_points_list
        
        
    def download(self, provider='aws'):
        """
        Download data for *all* rows from the given provider
        
        Parameters:
        ----------
        provider: str
            prem, aws, azure, gc etc
        """
        paths = [rec.download(provider) for rec in self]
        return paths