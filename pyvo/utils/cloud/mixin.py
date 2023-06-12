
from pyvo.utils import prototype


from .handler import generate_access_points, enable_cloud
from .access_points import AccessPointContainer, PREMAccessPoint



@prototype.prototype_feature('cloud')
class CloudRecordMixin:
    """
    Mixin to add cloud access functionality to pyvo.dal.Record
    """
    
    
    def enable_cloud(self, mode='all', urlcolumn='auto', refresh=False, **kwargs):
        """Process cloud-related information in a data product
        
        Adds a list of AccessPointContainer's (called access_points) 
        to the dal.DALResults in self._results. 
        Each AccessPointContainer is also available
        to the individual dal.Record elements (called access).
        
    
    
        Parameters
        ----------
        mode: str
            The mode to use. Options include: json, datalink, ucd, or all.
        urlcolumn: str
            The name of the column that contains the url link to on-prem data.
            If 'auto', try to find the url by:
                - use getdataurl if product is either pyvo.dal.Record
                - Use any column that contain http(s) links if product is Row.
            If None, do not use url for on-prem access
        refresh: bool
            If True, re-work out the access points. May be needed
            when changing credentials for example.

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
        
        # check if we have already worked out the access points for this Record
        if (
            not refresh and
            hasattr(self._results, 'access_points') and 
            self._results.access_points[self._index] is not None
           ):
            # nothing to do; it is already enabled
            return

        else:
            
            # create a cloudHandler
            cloudHandler = enable_cloud(self, mode, urlcolumn, **kwargs)
            
            # Initialize self._results.access_points if needed
            if not hasattr(self._results, 'access_points'):
                self._results.access_points = [None for _ in self._results]
            
            self._results.access_points[self._index] = cloudHandler.access_points
            self._cloudHanlder = cloudHandler
    
    @property
    def access(self):
        """Return AccessPointContainer that has a list of access point
        
        """
        
        if (
            hasattr(self._results, 'access_points') and 
            self._results.access_points[self._index] is not None
           ):
            return self._results.access_points[self._index]
        else:
            raise ValueError('No access points; call enable_cloud first')
    
    
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
        
        return self._cloudHanlder.get_cloud_uris(provider)
    
    
    def download(self, provider='aws', cache=True, destination=None):
        """
        Download the data from the given provider
        
        Parameters:
        ----------
        provider: str
            A short name of the data provider: prem, aws, azure, gc etc
        cache : bool
            If True (default), use file in cache if present.
        destination: str
            The destination path to save the downloaded file (default: None).
        """
        
        return self._cloudHanlder.download(provider, cache, destination)
        
    
    
@prototype.prototype_feature('cloud')
class CloudResultsMixin:
    """
    Mixin to add cloud access functionality to pyvo.dal.DALResults
    """
    
    
    def enable_cloud(self, mode='all', urlcolumn='auto', refresh=False, **kwargs):
        """Process cloud-related information in a data product
        
        Adds a list of AccessPointContainer's (called access_points) 
        to dal.DALResults. Each AccessPointContainer is also available
        to the individual dal.Record elements (through .access).
    
    
        Parameters
        ----------
        mode: str
            The mode to use. Options include: json, datalink, ucd, or all.
        urlcolumn: str
            The name of the column that contains the url link to on-prem data.
            If 'auto', try to find the url by:
                - use getdataurl if product is either pyvo.dal.Record
                - Use any column that contain http(s) links if product is Row.
            If None, do not use url for on-prem access
        refresh: bool
            If True, re-work out the access points. May be needed
            when changing credentials for example.

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
        # check if we have already worked out the access points for this Record
        if (
            not refresh and
            hasattr(self, 'access_points') and 
            all([ap is not None for ap in self.access_points])
           ):
            # nothing to do; we have already done this
            return

        # create a cloudHandler
        cloudHandler = enable_cloud(self, mode, urlcolumn, **kwargs)
        
        # add 'access_points' to the DALResults
        self.access_points = cloudHandler.access_points
        self._cloudHanlder = cloudHandler

    
    def download(self, provider='aws', cache=True, destination=None):
        """
        Download data for *all* rows from the given provider
        
        Parameters:
        ----------
        provider: str
            prem, aws, azure, gc etc
        cache : bool
            If True (default), use file in cache if present.
        destination: str
            The destination path to save the downloaded file (default: None).
        
        """
        return self._cloudHanlder.download(provider, cache, destination)