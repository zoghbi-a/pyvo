

import requests
from astropy.utils.data import download_file

import boto3
import botocore
from pathlib import Path
import os
from astropy.utils.console import ProgressBarOrSpinner


__all__ = ['ACCESS_MAP', 'AccessPointContainer', 'PREMAccessPoint', 'AWSAccessPoint']



class AccessPointContainer(dict):
    """A container and manager for AccessPoint(s)
    
    This is a simple dict that holds the access points for some
    data product
    
    
    """
    
    def __init__(self, *args):
        """Initilize a conatiner with some access point(s).
        
        Parameters
        ----------
        *args: AccessPoint or a subclass
            A single or multiple access points
            
        """
        self.access_points = dict()
        for ap in args:
            if isinstance(ap, AccessPoint):
                self.add_access_point(ap)
            else:
                raise ValueError(f'Expected a subclass of AccessPoint, not {type(ap)}')
                
                
    def add_access_point(self, access_point):
        """Add a new AccessPoint (or subclass) to the conatiner
        
        Parameters:
        -----------
        access_point: a subclass of AccessPoint, or a list of them.
            the access point to be added to the manager
                
        """
        
        # if a list, loop through the elements
        if isinstance(access_point, list):
        
            for ap in access_point:
                self.add_access_point(ap)
        
        else:
            
            # we have a single instance
            # First check we have the correct type
            if not isinstance(access_point, AccessPoint):
                raise ValueError(
                    f'Expected an AccessApoint instance, '
                    f'a subclass or a list not {type(access_point)}'
                )

            provider = access_point.provider
            
            # first time
            if not provider in self.access_points.keys():
                self.access_points[provider] = []
            
            # adding an access point if it has not been added already
            if not access_point.uid in self.uids(provider):
                self.access_points[provider].append(access_point)
                
    
    def uids(self, provider=None):
        """Return a list of id's from the access points. 
        
        Parameters:
        -----------
        provider: str, a list of str or None
            provider name (prem, aws, etc). If None, return all providers.
        
        """
        
        if provider is None:
            provider = [k for k in self.access_points.keys()]
        
        if isinstance(provider, str):
            provider = [provider]
            
        if not isinstance(provider, list):
            raise ValueError('provider has to be a str, a list of str or None')
            
        uids = [ap.uid for prov in provider for ap in self.access_points[prov]]
        
        return uids
    
    
    def __repr__(self):
        summary = ', '.join([f'{k}:{len(g)}' for k,g in self.access_points.items()])
        return f'<Access: {summary}>'
    
    
    def __getitem__(self, item):
        """Enable access to the access_points list directly"""
        return self.access_points[item]


class AccessPoint:
    """A base class to handle a single data access point
    
    Access points using different prem or cloud services
    are subclasses
    
    """
    
    provider = None
    
    
    def __init__(self, uid=None, **kwargs):
        """Initialize a basic access point with some id
        
        Parameters
        ----------
        uid : str
            a unique id for this access point. Typically, the url.
            
        Keywords
        --------
        place holder for keyword argument that may be needed such
        auth authentication.
        
        """
        
        self.uid = uid
        self._accessible = None
    
    
    def __repr__(self):
        return f'|{str(self.provider).ljust(5)}| {self.uid}'
    
    
    @property
    def accessible(self):
        """Check if the AccessPoint is accessible
        
        
        Return
        ------
        (accessible, msg) where accessible is a bool and msg is the failure 
            message or an empty string
        
        
        """
        if self._accessible is None:
            raise NotImplementedError(
                ('This is not meant to be called directly. '
                 'Subclasses need to overrride')
            )
        
        return self._accessible
        
    
    def download(self):
        """Download data from this access point
            
        Return
        ------
        path : str
            Returns the local path that the file was download to.
            
        """
        
        raise NotImplementedError(
            ('This is not meant to be called directly. '
             'Subclasses need to overrride')
        )


class PREMAccessPoint(AccessPoint):
    """Handle an http(s) access point from on-prem servers"""
    
    
    provider = 'prem'
    
    
    def __init__(self, uid, **kwargs):
        """Initialize an http access point from on-prem data center
        
        Parameters
        ----------
        uid : str
            the unique id in the form of the url to access the data
            
        Keywords
        --------
        None is expected
        
        """
        super().__init__(uid=uid)
        self.url = uid

        
    @property
    def accessible(self):
        """Check if the data is accessible
        
        
        Return
        ------
        (accessible, msg) where accessible is a bool and msg is the failure 
            message or an empty string
        
        
        """
        
        if self._accessible is None:
            response = requests.head(self.url)
            accessible = response.status_code == 200
            msg = '' if accessible else response.reason
            self._accessible = (accessible, msg)
        
        return self._accessible
        
    
    def download(self, cache=True):
        """Download data from this access point
        
        Parameters
        ----------
        cache : bool
            If True (default), use file in cache if present.
        
        Return
        ------
        path : str
            Returns the local path that the file was download to.
            
        """
        
        if self.url is None:
            raise ValueError(f'No on-prem url has been defined.')
            
        path = download_file(self.url, cache=cache)
        return path
    

class AWSAccessPoint(AccessPoint):
    """Handles a single access point on AWS"""
    
    
    provider = 'aws'
    
    
    def __init__(self, *, 
                 bucket_name = None, 
                 key = None,
                 uid = None,
                 **kwargs
                ):
        """Define an access point for aws.
        Either uid (uri) or both bucket_name/key need to be given
        
        Parameters
        ----------
        bucket_name: str
            name of the s3 bucket
        key: str
            the key or 'path' to the file or directory
        uid : str
            a unique id in the form of uri for the data of the form s3://bucket/key. 
            Either this should be given, or the combination of bucket_name/key.
            
        Keywords
        --------
        aws_profile : str
            name of the user's profile for credentials in ~/.aws/config
            or ~/.aws/credentials. Use to authenticate the AWS user with boto3.
        
        """
        
        aws_profile = kwargs.get('aws_profile', None)
        
        
        if uid is None:
            
            # when uri is None, we need both bucket_name and key
            if bucket_name is None or key is None:
                raise ValueError('either uid or both bucket_name and key are required')
                
            # both bucket_name and key need to be str
            assert(isinstance(bucket_name, str))
            assert(isinstance(key, str))
            
            if key[0] == '/':
                key = key[1:]
            
            uid = f's3://{bucket_name}/{key}'
            
        else:
            if not uid.startswith('s3://'):
                raise ValueError('uid needs to be of the form "s3://bucket/key"')
                
            # TODO: handle case of region in the uid/uri
            _uri = uid.split('/')
            bucket_name = _uri[2]
            key = '/'.join(_uri[3:])
            
        uri = uid
        super().__init__(uid=uid)
        
        self.s3_uri = uri
        self.s3_bucket_name = bucket_name
        self.s3_key = key
        
        
        # prepare the s3 resource
        # TODO: Allow for the user to pass s3_resource
        self.s3_resource = self._s3_resource(aws_profile)
        
    
    def _s3_resource(self, profile):
        """Construct a boto3 s3 resource
        
        Parameters
        ----------
        profile: str
            name of the user's profile for credentials in ~/.aws/config
            or ~/.aws/credentials. Use to authenticate the AWS user with boto3.
        
        """
        
        
        # if profile is give, use it.
        if profile is not None:
            session = boto3.session.Session(profile_name=profile)
            s3_resource = session.resource(service_name='s3')
        else:
            # access anonymously
            config = botocore.client.Config(signature_version=botocore.UNSIGNED)
            s3_resource = boto3.resource(service_name='s3', config=config)
        
        return s3_resource
    
    
    @property
    def accessible(self):
        """Check if the data is accessible
        Do a head_object call to test access
        
        
        Return
        ------
        (accessible, msg) where accessible is a bool and msg is the failure 
            message or an empty string
        
        
        """
        
        if self._accessible is None:
        
            s3_client = self.s3_resource.meta.client
            try:
                header_info = s3_client.head_object(Bucket=self.s3_bucket_name, Key=self.s3_key)
                accessible, msg = True, ''
            except Exception as e:
                accessible = False
                msg = str(e)
            self._accessible = (accessible, msg)
                
        return self._accessible
    
    
    # adapted from astroquery.mast.
    def download(self, cache=True):
        """
        downloads the product used in inializing this object into
        the given directory.
        
        
        Parameters
        ----------
        cache : bool
            If True (default) and the file is found on disc it will not be downloaded again.
            
        """
        
        s3 = self.s3_resource
        s3_client = s3.meta.client

        key = self.s3_key
        bucket_name = self.s3_bucket_name
        
        bkt = s3.Bucket(bucket_name)
        if not key:
            raise Exception(f"Unable to locate file {key}.")

        local_path = Path(key).name

        # Ask the webserver what the expected content length is and use that.
        info_lookup = s3_client.head_object(Bucket=bucket_name, Key=key)
        length = info_lookup["ContentLength"]

        # if we have cache, use it and return, otherwise download data
        if cache and os.path.exists(local_path):
            if length is not None:
                statinfo = os.stat(local_path)
                if statinfo.st_size == length:
                    # found cached file with expected size. Stop
                    return

        with ProgressBarOrSpinner(length, (f'Downloading {self.s3_uri} to {local_path} ...')) as pb:

            # Bytes read tracks how much data has been received so far
            # This variable will be updated in multiple threads below
            global bytes_read
            bytes_read = 0

            progress_lock = threading.Lock()

            def progress_callback(numbytes):
                # Boto3 calls this from multiple threads pulling the data from S3
                global bytes_read

                # This callback can be called in multiple threads
                # Access to updating the console needs to be locked
                with progress_lock:
                    bytes_read += numbytes
                    pb.update(bytes_read)

            bkt.download_file(key, local_path, Callback=progress_callback)
        return local_path
    
    
ACCESS_MAP = {
    ap.provider: ap for ap in [
        PREMAccessPoint,
        AWSAccessPoint
    ]
}