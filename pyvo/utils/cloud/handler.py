# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""
Cloud-related utils
"""


import json
import itertools

from astropy.table import Table, Row
import pyvo
from pyvo.utils import prototype

from .access_points import ACCESS_MAP, AccessPointContainer


prototype.features['cloud'] = prototype.Feature('cloud',
                              'https://wiki.ivoa.net/twiki/bin/view/IVOA/Cloud-access',
                              False)

@prototype.prototype_feature('cloud')
def generate_access_points(product, mode='all', **kwargs):
    """Process cloud-related information in a data product
    
    Parameters
    ----------
    product: pyvo.dal.Record, pyvo.dal.DALResults, astropy Table or Row
        The data product.
    mode: str
        The mode to use. Options include: json, datalink, ucd, or all.
        Note that the datalink and ucd modes work only with product being a
        pyvo.dal.Record or pyvo.dal.DALResults
        
    Keywords
    --------
    meta data needed to download the data, such as authentication profile
    which will be used to create access points. 

    prem:
        No keywords needed
    aws:
        aws_profile : str
            name of the user's profile for credentials in ~/.aws/config
            or ~/.aws/credentials. Use to authenticate the AWS user with boto3.
            
    Return
    ------
    A list of AccessPoint instances for every row in products if products
    is a DALResults or Table, otherwise a single list
    
    """
    Record = pyvo.dal.Record
    DALResults = pyvo.dal.DALResults
    if not isinstance(product, (Record, DALResults, Table, Row)):
        raise ValueError((
            f'product has the wrong type. Expecting dal.Record, '
            f'dal.DALResults, Table or Row. Found {type(product)}'
        ))
    
    if mode not in ['json', 'datalink', 'ucd', 'all']:
        raise ValueError((
            'mode has to be one of json, datalink, ucd or all'
        ))
    
    
    # get the product as a list of rows
    if isinstance(product, (Record, Row)):
        rows = [product]
    else:
        rows = [_ for _ in product]
    
    
    json_ap, ucd_ap, dl_ap = [[] for _ in rows], [[] for _ in rows], [[] for _ in rows]
    
    if mode in ['json', 'all']:
        json_ap = process_cloud_json(rows, **kwargs)
    
    # proceed to ucd or datalinks only when dealing with pyvo Record.
    if isinstance(rows[0], Record):
        
        if mode in ['ucd', 'all']:
            ucd_ap = process_cloud_ucd(rows, **kwargs)
    
        if mode in ['datalink', 'all']:
            query_result = rows[0]._results
            dl_ap = process_cloud_datalinks(rows, query_result)
    
    # put them in one list of nrow lists of access points
    ap_list = [list(itertools.chain(*z)) for z in zip(json_ap, ucd_ap, dl_ap)]
    
    if isinstance(product, (Record, Row)):
        ap_list = ap_list[0]
    
    return ap_list
    

@prototype.prototype_feature('cloud')
def process_cloud_json(products, colname='cloud_access', **kwargs):
    """Look for and process any cloud information in a json column
    
    Parameters
    ----------
    products: list
        A list of dal.Record or astropy Row
        
    
    Keywords
    --------
    meta data needed to download the data, such as authentication profile
    which will be used to create access points. 
       
    
    Return
    ------
    A list of AccessPoint instances for every row in products
    
    """
    
    rows_access_points = []
    for row in products:
        # if no colname column, there is nothing to do    
        try:
            jsontxt  = row[colname]
        except KeyError:
            # no json column, continue
            rows_access_points.append([])
            continue
        
        jsonDict = json.loads(jsontxt)
        
        apoints = []
        for provider, APClass in ACCESS_MAP.items():
            
            # skip provider if not in jsonDict
            if provider not in jsonDict:
                continue
            
            # access point parameters
            ap_params = jsonDict[provider]
            ap_params.update(**kwargs)
            new_ap = APClass(**ap_params)
            apoints.append(new_ap)
            
        rows_access_points.append(apoints)
        
    return rows_access_points


@prototype.prototype_feature('cloud')
def process_cloud_ucd(products, **kwargs):
    """Look for and process any cloud information in columns
    with ucd of the form: 'meta.ref.{provider}', where provider
    is: aws, gc, azure etc.
    
    Note that products needs to be a pyvo.dal.Record. astropy
    table Row objects do not handle UCDs.
    
    Parameters
    ----------
    products: list
        A list of dal.Record
        
    
    Keywords
    --------
    meta data needed to download the data, such as authentication profile
    which will be used to create access points. 
        
        
    Return
    ------
    A list of AccessPoint instances for every row in products
    
    """
    
    if not isinstance(products[0], pyvo.dal.Record):
        raise ValueError((
            f'products has the wrong type. Expecting a list of '
            f'dal.Record. Found {type(products[0])}'
        ))
    
    rows_access_points = []
    for row in products:
        
        apoints = []
        for provider, APClass in ACCESS_MAP.items():
            
            uri = row.getbyucd(f'meta.ref.{provider}')
            if uri is not None:
                new_ap = APClass(uid=uri, **kwargs)
                apoints.append(new_ap)
        
        rows_access_points.append(apoints)

    return rows_access_points


@prototype.prototype_feature('cloud')
def process_cloud_datalinks(products, query_result, provider_par='provider', **kwargs):
    """Look for and process any cloud information in datalinks
    
    Note that products needs to be a pyvo.dal.Record. astropy
    table Row objects do not handle datalinks.
    
    Parameters
    ----------
    products: list
        A list of dal.Record
    query_result: pyvo.dal.DALResults
        The original query results that will be used to find
        datalinks and call them.
    provider_par: str
        The name of the parameter that will passed with the
        datalink call to specify the provider
        
    
    Keywords
    --------
    meta data needed to download the data, such as authentication profile
    which will be used to create access points. 
        
        
    Return
    ------
    A list of AccessPoint instances for every row in products. 
    
    """
    if not isinstance(products[0], pyvo.dal.Record):
        raise ValueError((
            f'products has the wrong type. Expecting a list of '
            f'dal.Record. Found {type(products[0])}'
        ))
    
    if not isinstance(query_result, pyvo.dal.DALResults):
        raise ValueError((
            f'query_result has the wrong type. Expecting '
            f'dal.DALResults. Found {type(query_result)}'
        ))
    
    rows_access_points = [[] for _ in products]
    
    # get datalink service
    try:
        _datalink = query_result.get_adhocservice_by_id('cloudlinks')
    except (pyvo.DALServiceError, AttributeError):
        # No datalinks; return
        return rows_access_points
    
    nrows = len(products)
    
    # input parameters for the datalink call
    in_params = pyvo.dal.adhoc._get_input_params_from_resource(_datalink)
    dl_col_id = [p.ref for p in in_params.values() if p.ref is not None]
    
    # proceed only if we have a PARAM named provider_par, 
    if provider_par in in_params.keys():
        # we have a 'source' element, process it
        provider_elem  = in_params[provider_par]
        
        # list the available providers in the `provider_par` element:
        provider_options = provider_elem.values.options
        
        
        for description,option in provider_options:


            # TODO: consider including batch_size simialr to 
            # DatalinkResultsMixin.iter_datalinks
            query = pyvo.dal.adhoc.DatalinkQuery.from_resource(
                products, _datalink, 
                **{provider_par:option}
            )
            
            dl_result = query.execute()
            dl_table = dl_result.to_table()
            
            if len(rows_access_points) == 0:
                rows_access_points = [[] for _ in products]
                
            provider = option.split(':')[0]
            if provider in ACCESS_MAP.keys():
                APClass = ACCESS_MAP[provider]
                for irow in range(nrows):
                    dl_res = dl_table[dl_table['ID'] == products[irow][dl_col_id[0]]]
                    for dl_row in dl_res:
                        new_ap = APClass(uid=dl_row['access_url'], **kwargs)
                        rows_access_points[irow].append(new_ap)
    
    return rows_access_points


@prototype.prototype_feature('cloud')
class CloudHandler:
    """
    Handles and processes cloud access functionality
    """   
    
    def __init__(self, product, mode='all', urlcolumn='auto', **kwargs):
        """Process cloud-related information in a data product
        
        Creates an AccessPointContainer for the Record/Row.        
    
    
        Parameters
        ----------
        product: pyvo.dal.Record, pyvo.dal.DALResults, astropy Table or Row
            The data product.
        mode: str
            The mode to use. Options include: json, datalink, ucd, or all.
        urlcolumn: str
            The name of the column that contains the url link to on-prem data.
            If 'auto', try to find the url by:
                - use getdataurl if product is either pyvo.dal.Record or DALResults
                - Use any column that contain http(s) links if product is Table or Row.

        Keywords
        --------
        meta data needed to download the data, such as authentication profile
        which will be used to create access points. 

        prem:
            No keywords needed
        aws:
            aws_profile : str
                name of the user's profile for credentials in ~/.aws/config
                or ~/.aws/credentials. Use to authenticate the AWS user with boto3.
        
        """
        
        # generate access points; this will fail if product is of the wrong type
        generated_ap = generate_access_points(product=product, mode=mode, **kwargs)
        
        product_is_list = isinstance(generated_ap[0], list)
        if not product_is_list:
            generated_ap = [generated_ap]
            product = [product]
        
        
        # add a default on-prem access point
        nrec = len(generated_ap)
        access_points = []
        for irec in range(nrec):
            access = AccessPointContainer()

            # add on-prem url if found
            url = self._getdataurl(product[irec], urlcolumn)
            if url is not None:
                access.add_access_point(ACCESS_MAP['prem'](uid=url))
            
            # add other access points
            access.add_access_point(generated_ap[irec])
            access_points.append(access)
        
        # if we initially got a single row, retrun a single container
        if not product_is_list:
            access_points = access_points[0]
        
        self.product_is_list = product_is_list
        self.access_points = access_points
    
    
    def _getdataurl(self, product, urlcolumn='auto'):
        """Work out the prem data url
        
        Parameters
        ----------
        product: Record or Row
        urlcolumn: str
            The name of the column that contains the url link to on-prem data.
            If 'auto', try to find the url by:
                - use getdataurl if product is either pyvo.dal.Record
                - Use any column that contain http(s) links if product is Row.
                
        Return
        ------
        url (as str) if found or None
        
        """
        # column names
        if hasattr(product, 'fieldnames'):
            # DALResults
            colnames = product.fieldnames
        elif hasattr(product, '_results'):
            # dal.Record
            colnames = product._results.fieldnames
        else:
            colnames = product.colnames
        
        url = None
        
        if urlcolumn == 'auto':
            if isinstance(product, pyvo.dal.Record):
                url = product.getdataurl()
            else:
                # try to find it
                for col in colnames:
                    if isinstance(product[col], str) and 'http' in product[col]:
                        url = product[col]
                        break
        else:
            if urlcolumn not in colnames:
                raise ValueError(f'colname {colname} not available in data product')
            url = product[urlcolumn]
        
        return url
        
        
    
    def get_cloud_uris(self, provider=None):
        """Retrun the cloud uri for the dataset, which can be used to retrieve 
        the dataset in this record. None is returne if no cloud information
        is available
        
        Parameters
        ----------
        provider: str or None
            Which provider to use: prem, aws, gc, azure.
            If None, get uris from all providers
            
            
        """
        
        if self.product_is_list:
            uris = [ap.uids(provider) for ap in self.access_points]
        else:
            uris = self.access_points.uids(provider)
                
        return uris
    
    
    def download(self, provider='aws', cache=True):
        """
        Download the data from the given provider
        
        Parameters:
        ----------
        provider: str
            A short name of the data provider: prem, aws, azure, gc etc
        cache : bool
            If True (default), use file in cache if present.
            
        """
        
        
        access_point_list = self.access_points
        if not self.product_is_list:
            access_point_list = [access_point_list]
        
        # loop through the rows:
        paths, messages = [], []
        for access in access_point_list:  
            
            path, msgs = None, []
            
            if provider in access.providers:

                access_points = access[provider]
                # return the first access point that is accessible.
                # if none, print the returned message
                # TODO: we can make this more sophisticated by selecting
                # by region etc.
                for ap in access_points:
                    accessible, msg = ap.accessible
                    if accessible:
                        path = ap.download(cache)
                        break
                    else:
                        msgs.append(msg)
            else:
                msgs.append(f'No data from {provider}')

            if path is None:
                for ap,msg in zip(access, msgs):
                    print(f'\n** {ap}: ***\n{msg}\n')
                    
            messages.append(msgs)
            paths.append(path)
        
        if not self.product_is_list:
            paths = paths[0]
            messages = messages[0]
        
        self.messages = messages
                
        return paths
    

@prototype.prototype_feature('cloud')
def enable_cloud(product, mode='all', urlcolumn='auto', **kwargs):
    """Process cloud-related information in a data product.
    This is a wrapper around CloudHandler

    Parameters
    ----------
    product: pyvo.dal.Record, pyvo.dal.DALResults, astropy Table or Row
        The data product.
    mode: str
        The mode to use. Options include: json, datalink, ucd, or all.
    urlcolumn: str
            The name of the column that contains the url link to on-prem data.
            If 'auto', try to find the url by:
                - use getdataurl if product is either pyvo.dal.Record
                - Use any column that contain http(s) links if product is Row.

    Keywords
    --------
    meta data needed to download the data, such as authentication profile
    which will be used to create access points. 

    prem:
        No keywords needed
    aws:
        aws_profile : str
            name of the user's profile for credentials in ~/.aws/config
            or ~/.aws/credentials. Use to authenticate the AWS user with boto3.
            
    Return
    ------
    CloudHandler instance, which has a download method.
    
    """
    
    handler = CloudHandler(product, mode, urlcolumn, **kwargs)
    return handler