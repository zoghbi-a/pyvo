import unittest
import pytest
import os

from pyvo.utils.cloud.access_points import (
    AccessPoint, PREMAccessPoint, AWSAccessPoint, 
    AccessPointContainer
)


    
class TestAccessPoint(unittest.TestCase):
    """Tests AccessPoint"""
    
    @classmethod
    def setUpClass(cls):
        cls.ap = AccessPoint()
        
    def test_accessible(self):
        with self.assertRaises(NotImplementedError):
            self.ap.accessible

    def test_download(self):
        with self.assertRaises(NotImplementedError):
            self.ap.download()
            
class TestPREMAccessPoint(unittest.TestCase):
    """Tests PREMAccessPoint"""
    
    @classmethod
    def setUpClass(cls):
        cls.url = 'https://www.ivoa.net/xml/ADQL/ADQL-v1.0.xsd'
        cls.ap = PREMAccessPoint(cls.url)
        
    def test_id(self):
        self.assertEqual(self.ap.id, self.url)
        
    def test_provider(self):
        self.assertEqual(PREMAccessPoint.provider, 'prem')
    
    @pytest.mark.remote_data
    def test_accessible(self):
        access,msg = self.ap.accessible
        self.assertEqual(access, True)
    
    @pytest.mark.remote_data
    def test_download(self):
        path = self.ap.download()
        self.assertEqual(os.path.exists(path), True)
        
        
class TestAWSAccessPoint(unittest.TestCase):
    """Tests AWSAccessPoint"""
    
    def test_provider(self):
        self.assertEqual(AWSAccessPoint.provider, 'aws')
    
    def test_uri_is_None(self):
        """uri is None"""
        # uri is None, and both bucket_name and key are None
        with self.assertRaises(ValueError):
            ap = AWSAccessPoint(uri=None, bucket_name=None, key=None)
        
        # uri is None, and either bucket_name and key are None
        with self.assertRaises(ValueError):
            ap = AWSAccessPoint(uri=None, bucket_name='somebucket', key=None)
            
        with self.assertRaises(ValueError):
            ap = AWSAccessPoint(uri=None, bucket_name=None, key='some/key')
        
    def test_uri(self):
        """uri is passed"""
        
        with self.assertRaises(ValueError):
            ap = AWSAccessPoint(uri='s5://somebucket/file')
            
        ap = AWSAccessPoint(bucket_name='bucket', key='key/file')
        self.assertEqual(ap.s3_uri, 's3://bucket/key/file')
        
    def test_bucket_name(self):
        ap = AWSAccessPoint(uri='s3://bucket/key/file')            
        self.assertEqual(ap.s3_bucket_name, 'bucket')
        
        ap = AWSAccessPoint(bucket_name='bucket2', key='key/file')
        self.assertEqual(ap.s3_bucket_name, 'bucket2')
        

class TestAccessPointContainer(unittest.TestCase):
    """Tests AccessPointContainer"""
    
    def test_wrong_arg(self):
        with self.assertRaises(ValueError):
            apc = AccessPointContainer('str')
    
    def test_1_arg(self):
        ap  = AccessPoint()
        apc = AccessPointContainer(ap)
        self.assertEqual(len(apc.access_points), 1)
    
    def test_2_arg(self):
        ap1 = AccessPoint()
        ap2 = PREMAccessPoint(url='http://some/url')
        apc = AccessPointContainer(ap1, ap2)
        self.assertEqual(len(apc.access_points), 2)
        
    def test_2_arg_same_ap(self):
        ap1 = PREMAccessPoint(url='http://some/url')
        ap2 = PREMAccessPoint(url='http://some/url2')
        apc = AccessPointContainer(ap1, ap2)
        self.assertEqual(len(apc.access_points), 1)
        self.assertEqual(len(list(apc.access_points.values())[0]), 2)
        
    def test_2_arg_repeat_id(self):
        ap1 = PREMAccessPoint(url='http://some/url')
        ap2 = PREMAccessPoint(url='http://some/url')
        apc = AccessPointContainer(ap1, ap2)
        self.assertEqual(len(apc.access_points), 1)
        self.assertEqual(len(list(apc.access_points.values())[0]), 1)