
from pyvo.utils import prototype


from .handler import generate_access_points
from .access_points import AccessPointContainer, PREMAccessPoint


prototype.features['cloud-mixin'] = prototype.Feature('cloud-mixin',
                              'https://wiki.ivoa.net/twiki/bin/view/IVOA/Cloud-access',
                              False)

@prototype.prototype_feature('cloud-mixin')
class CloudRecordMixin:
    """
    Mixin to add cloud access functionality to pyvo.dal.Record
    """
    
    
    def enable_cloud(self, mode='all', refresh=False, **kwargs):
        """Process cloud-related information in a data product
        
        Adds a list of AccessPointContainer's (called access_points) 
        to the dal.DALResults in self._results. 
        Each AccessPointContainer is also available
        to the individual dal.Record elements (called access).
        
    
    
        Parameters
        ----------
        mode: str
            The mode to use. Options include: json, datalink, ucd, or all.
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
            
            # add a default on-prem access point
            access = AccessPointContainer()
            url = self.getdataurl()
            if url is not None:
                access.add_access_point(PREMAccessPoint(uid=url))
                
            # generate access points
            generated_ap = generate_access_points(product=self, mode=mode, **kwargs)
            access.add_access_point(generated_ap)

            
            # Initialize self._results.access_points if needed
            if not hasattr(self._results, 'access_points'):
                self._results.access_points = [None for _ in self._results]
            
            self._results.access_points[self._index] = access
    
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
        
        access = self.access
                
        return access.uids(provider)
    
    
    def download(self, provider='aws'):
        """
        Download the data from the given provider
        
        Parameters:
        ----------
        provider: str
            A short name of the data provider: prem, aws, azure, gc etc
        """
        
        
        access = self.access
        if provider not in access.providers:
            raise ValueError(f'No access point available for provider {provider}.')
            
        access_points = access[provider]
        path = None
        msgs = []
        # return the first access point that is accessible.
        # if none, print the returned message
        # TODO: we can make this more sophisticated by selecting
        # by region etc.
        for ap in access_points:
            accessible, msg = ap.accessible
            if accessible:
                path = ap.download()
                break
            else:
                msgs.append(msg)
        if path is None:
            for ap,msg in zip(access_points, msgs):
                print(f'\n** {ap}: ***\n{msg}\n')
        return path
    
    
@prototype.prototype_feature('cloud-mixin')
class CloudResultsMixin:
    """
    Mixin to add cloud access functionality to pyvo.dal.DALResults
    """
    
    
    def enable_cloud(self, mode='all', refresh=False, **kwargs):
        """Process cloud-related information in a data product
        
        Adds a list of AccessPointContainer's (called access_points) 
        to dal.DALResults. Each AccessPointContainer is also available
        to the individual dal.Record elements (through .access).
    
    
        Parameters
        ----------
        mode: str
            The mode to use. Options include: json, datalink, ucd, or all.
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

        # add a default on-prem access point
        nrec = len(self)
        access_points = []
        for irec in range(nrec):
            access = AccessPointContainer()
            url = self[irec].getdataurl()
            if url is not None:
                access.add_access_point(PREMAccessPoint(uid=url))
            access_points.append(access)
            
        # generate access points
        generated_ap = generate_access_points(product=self, mode=mode, **kwargs)
        for irec in range(nrec):
            access_points[irec].add_access_point(generated_ap[irec])

        # add 'access_points' to the DALResults
        self.access_points = access_points

    
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