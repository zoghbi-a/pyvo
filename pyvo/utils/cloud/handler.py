# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""
Cloud-related utils
"""


import json
import itertools

from astropy.table import Table, Row
import pyvo
from pyvo.dal import Record, DALResults


from .access_points import ACCESS_MAP, AccessPointContainer


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
    
    if not isinstance(products[0], Record):
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


def process_cloud_datalinks(products, query_result, provider_par='source', **kwargs):
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
    if not isinstance(products[0], Record):
        raise ValueError((
            f'products has the wrong type. Expecting a list of '
            f'dal.Record. Found {type(products[0])}'
        ))
    
    if not isinstance(query_result, DALResults):
        raise ValueError((
            f'query_result has the wrong type. Expecting '
            f'dal.DALResults. Found {type(query_result)}'
        ))
    
    rows_access_points = [[] for _ in products]
    
    # get datalink service
    try:
        _datalink = query_result.get_adhocservice_by_ivoid(
            'ivo://ivoa.net/std/datalink'
        )
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