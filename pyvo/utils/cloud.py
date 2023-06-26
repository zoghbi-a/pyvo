# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""
Cloud-related utils
"""

import json
from collections import UserDict

from astropy.table import Table, Row
from ..dal import Record, DALResults, adhoc, DALServiceError
from .download import http_download, aws_download



# global variables
JSON_COLUMN = 'cloud_access'



def find_product_access(product, mode='all', urlcolumn='auto', verbose=False, **kwargs):
    """Search for data product access information in some data product.
    
    This finds all available access information from prem, aws etc.

    Parameters
    ----------
    product: Record, DALResults, astropy Table or Row
        The data product.
    mode: str
        The mode to use. Options include: json, datalink, ucd, or all.
    urlcolumn: str
            The name of the column that contains the url link to on-prem data.
            If 'auto', try to find the url by:
                - use getdataurl if product is either Record or DALResults
                - Use any column that contain http links if product is Row or Table.
    verbose: bool
        If True, print progress and debug text

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
    ...
    
    """
    
    # check product
    if not isinstance(product, (Record, DALResults, Table, Row)):
        raise ValueError((
            f'product has the wrong type. Expecting dal.Record, '
            f'dal.DALResults, Table or Row. Found {type(product)}'
        ))
    
    # check mode
    if mode not in ['json', 'datalink', 'ucd', 'all']:
        raise ValueError((
            'mode has to be one of json, datalink, ucd or all'
        ))
    
    # convert product as a list of rows
    if isinstance(product, (Record, Row)):
        rows = [product]
    else:
        rows = [_ for _ in product]
    
    json_ap = [Provider() for _ in rows]
    if mode in ['json', 'all']:
        json_ap = _process_json_column(rows, verbose=verbose)
    
    ucd_ap = [Provider() for _ in rows]
    if mode in ['ucd', 'all']:
        ucd_ap = _process_ucd_column(rows, verbose=verbose)
    
    dl_ap = [Provider() for _ in rows]
    if mode in ['datalink', 'all']:
        dl_ap = _process_cloud_datalinks(rows, verbose=verbose)
    
    # put them in one list of nrow lists of access points
    ap_list = [json_ap[irow] + ucd_ap[irow] + dl_ap[irow]
                  for irow in range(len(rows))]
    
    if isinstance(product, (Record, Row)):
        ap_list = ap_list[0]
        
    return ap_list


def _process_json_column(products, colname=JSON_COLUMN, verbose=False):
    """Look for and process any cloud information in a json column
    
    Parameters
    ----------
    products: list of Record or Row
        A list of product rows
    colname: str
        The name for the column that contain the cloud json information
    verbose: bool
        If True, print progress and debug text
       
    
    Return
    ------
    A list of Provider instances. One for every row in products
    
    """
    if not isinstance(products, list):
        raise ValueError('products is expected to be a list')
    
    if verbose:
        print(f'searching for and processing json column {colname}')
    
    rows_access_points = []
    for irow,row in enumerate(products):
        
        providers = Provider()
        
        # if no colname column, there is nothing to do    
        try:
            jsontxt  = row[colname]
        except KeyError:
            # no json column, continue
            if verbose:
                print(f'No column {colname} found for row {irow}')
            rows_access_points.append(providers)
            continue
        
        jsonDict = json.loads(jsontxt)
        
        for provider, params in Provider.PROVIDERS.items():
            
            if provider not in jsonDict:
                continue
            
            p_params = jsonDict[provider]
            if not isinstance(p_params, list):
                p_params = [p_params]
            
            for ppar in p_params:
                providers.add_provider(provider, verbose=verbose, **ppar)

        rows_access_points.append(providers)
        
    return rows_access_points


def _process_ucd_column(products, verbose=False):
    """Look for and process any cloud information in columns
    with ucd of the form: 'meta.ref.{provider}', where provider
    is: aws, gc, azure etc.
    
    Note that products needs to be a Record. astropy
    table Row objects do not handle UCDs.
    
    Parameters
    ----------
    products: list
        A list of Record
    verbose: bool
        If True, print progress and debug text
       
    
    Return
    ------
    A list of Provider instances. One for every row in products
    
    """
    if not isinstance(products, list):
        raise ValueError('products is expected to be a list')
    
    if not isinstance(products[0], Record):
        raise ValueError((
            f'products has the wrong type. Expecting a list of '
            f'Record. Found {type(products[0])}'
        ))
    
    if verbose:
        print(f'searching for and processing cloud ucd column(s)')
    
    rows_access_points = []
    for row in products:
        
        providers = Provider()
        
        for provider, params in Provider.PROVIDERS.items():
            
            uri = row.getbyucd(f'meta.ref.{provider}')
            if uri is not None:
                providers.add_provider(provider, uri, verbose=verbose)
        
        rows_access_points.append(providers)

    return rows_access_points


def _process_cloud_datalinks(products, verbose=False):
    """Look for and process any cloud information in datalinks
    
    Note that products needs to be a Record. astropy
    table Row objects do not handle datalinks.
    
    Parameters
    ----------
    products: list
        A list of dal.Record
    verbose: bool
        If True, print progress and debug text
        
    Return
    ------
    A list of Provider instances. One for every row in products
    
    """
    if not isinstance(products, list):
        raise ValueError('products is expected to be a list')
    
    if not isinstance(products[0], Record):
        raise ValueError((
            f'products has the wrong type. Expecting a list of '
            f'Record. Found {type(products[0])}'
        ))
    
    if verbose:
        print(f'searching for and processing datalinks')
    
    dalResult = products[0]._results
    
    rows_access_points = [Provider() for _ in products]
    
    # get datalink service
    try:
        _datalink = dalResult.get_adhocservice_by_id('cloudlinks')
    except (DALServiceError, AttributeError):
        # No datalinks; return
        return rows_access_points
    
    nrows = len(products)
    
    # input parameters for the datalink call
    in_params = adhoc._get_input_params_from_resource(_datalink)
    dl_col_id = [p.ref for p in in_params.values() if p.ref is not None]
    
    # name of parameter to specify the provider; we initially used source
    provider_par = 'provider'
    
    # proceed only if we have a PARAM named provider_par, 
    if provider_par in in_params.keys():
        # we have a 'provider' element, process it
        provider_elem  = in_params[provider_par]
        
        # list the available providers in the `provider_par` element:
        provider_options = provider_elem.values.options
        
        
        for description,option in provider_options:
            
            provider = option.split(':')[0]
            if provider not in Provider.PROVIDERS:
                continue

            # TODO: consider including batch_size simialr to 
            # DatalinkResultsMixin.iter_datalinks
            query = adhoc.DatalinkQuery.from_resource(
                products, _datalink, 
                **{provider_par:option}
            )
            
            dl_result = query.execute()
            dl_table = dl_result.to_table()
                
                
            for irow in range(nrows):
                dl_res = dl_table[dl_table['ID'] == products[irow][dl_col_id[0]]]
                for dl_row in dl_res:
                    rows_access_points[irow].add_provider(provider, dl_row['access_url'], 
                                                          verbose=verbose)
    
    return rows_access_points


class Provider(UserDict):
    """Container for a list of providers as dict"""

    # supported providers & their parameters
    PROVIDERS = {
        'prem': ['url'],
        'aws' : ['uri', 'bucket_name', 'key']
    }

    def __init__(self):
        super(Provider, self).__init__()
        for provider, params in self.PROVIDERS.items():
            self.data[provider] = []

    def __repr__(self):
        return json.dumps(self.data, indent=1) 
    
    def __add__(self, val2):
        """Combine two Provider instances"""
        newp = Provider()
        for cval in [self, val2]:
            for key,val in cval.items():
                newp.data[key] += val
        return newp
    
    def __setitem__(self, key, value):
        """Not allowed directly; use add_provider
        """
        raise NotImplemented(f'use add_provider to add providers')

    
    def add_provider(self, provider, uri=None, **kwargs):
        """Add a data provider
        
        Parameters
        ----------
        provider: str
            Provider name: prem, aws, etc. 
            The list is in the keys of Provider.PROVIDERS
        uri: str
            direct uri for the data. If given, it overrides
            the value of the first parameter PROVIDERS[provider]
            in kwargs, if given.
        
        Keywords
        --------
        verbose: bool
            If True, print progress and debug text
        
        Other parameters needed for each provider.
        The list is in the values of Provider.PROVIDERS
        
        """
        if not provider in self.PROVIDERS:
            raise ValueError(f'provider: {provider} is not supported')
        
        verbose = kwargs.pop('verbose', False)
        
        if uri is not None:
            if not isinstance(uri, str):
                raise ValueError('uri has to be a str')
            kwargs[self.PROVIDERS[provider][0]] = uri
        
        # if uri already exists; skip
        uri = kwargs.get(self.PROVIDERS[provider][0], None)
        if uri is not None:
            for link in self[provider]:
                if uri == link[0]:
                    if verbose:
                        print(f'uri {uri} already exists. skipping ...')
                    return
        
        
        params = [kwargs.get(par, None) for par in self.PROVIDERS[provider]]
        if all([_ is None for _ in params]):
            require_p = ', '.join(self.PROVIDERS[provider])
            raise ValueError(f'Wrong parameter. Parameters for {provider} are: {require_p}')
        else:
            self[provider].append(params)

    
    def download(self,
                 provider, 
                 local_filepath=None,
                 cache=False,
                 timeout=None,
                 verbose=False,
                 **kwargs):
        """Download data from provider to local_filepath
        
        Parameters
        ----------
        provider: str
            options are prem, aws etc.
        local_filepath: str
        Local path, including filename, where the file is to be downloaded.
        cache : bool
            If True, check if a cached file exists before download
        timeout: int
            Time to attempt download before failing
        verbose: bool
            If True, print progress and debug text
        
        Keywords
        --------
        Other parameters to be passed to http_download, aws_download etc
        
        """
        
        download = {
            'prem': http_download,
            'aws' : aws_download
        }
        if provider not in download:
            raise ValueError(f'Unsupported provider {provider}')
        download_func  = download[provider]
        download_links = self[provider]
        func_keys = self.PROVIDERS[provider]
        
        errors = ''
        for link in download_links:
            kpars = {k:v for k,v in zip(func_keys, link)}
            kpars.update(local_filepath=local_filepath, cache=cache, 
                         timeout=timeout, verbose=verbose)
            kpars.update(**kwargs)
            try:
                if verbose:
                    print(f'Downloading from {provider} ...')
                download_func(**kpars)
                return
            except Exception as e:
                err_msg = f'Downloading from {provider} failed: {str(e)}'
                if verbose:
                    print(err_msg)
                if link != download_links[-1]:
                    msg2 = 'Trying other available links.'
                    if verbose:
                        print(msg2)
                    err_msg += f'\n{msg2}'
                errors += f'\n{err_msg}'
        # if we are here, then download has failed. Report the errors
        raise RuntimeError(errors)
        