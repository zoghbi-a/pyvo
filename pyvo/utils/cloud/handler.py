# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""
Cloud-related utils
"""


import json

from astropy.table import Table, Row
from pyvo.dal import Record, DALResults


from .access_points import ACCESS_MAP


def generate_cloud_access_points(product, mode='all', **kwargs):
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
    
    """
    
    if not isinstance(product, (Record, DALResults, Table, Row)):
        raise ValueError((
            f'product has the wrong type. Execting dal.Record, '
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
    
    
    if mode in ['json', 'all']:
        json_ap = process_cloud_json(rows, **kwargs)
        print(json_ap)
    
    # proceed to ucd or datalinks only when dealing with pyvo Record.
    if isinstance(rows[0], Record):
        
        if mode in ['ucd', 'all']:
            ucd_ap = process_cloud_ucd(rows, **kwargs)
            print(ucd_ap)
    
        #if mode in ['datalink', 'all']:
        #    dl_ap = process_cloud_dlinks(rows, product)
        
    
    
        

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
        # if no cloud_access column, there is nothing to do    
        try:
            jsontxt  = row[colname]
        except KeyError:
            # no json column, continue
            aplist.append(None)
            continue
        
        jsonDict = json.loads(jsontxt)
        
        apoints = []
        for provider, APClass in ACCESS_MAP.items():
            
            # skip provider if not in jsonDict
            if provider not in jsonDict:
                continue
            
            # access point parameters
            ap_params = jsonDict[provider]
            new_ap = APClass(**ap_params)
            apoints.append(new_ap)
            
        rows_access_points.append(apoints)
    return rows_access_points


def process_cloud_ucd(products, colname='cloud_access', **kwargs):
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
    
    rows_access_points = []
    for row in products:
        
        apoints = []
        for provider, APClass in ACCESS_MAP.items():
            
            uri = row.getbyucd(f'meta.ref.{provider}')
            if uri is not None:
                new_ap = APClass(uri=uri, **kwargs)
                apoints.append(new_ap)
        
        rows_access_points.append(apoints)

    return rows_access_points