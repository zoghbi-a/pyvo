import unittest
import pytest
import os

from pyvo.utils.cloud.access_points import AccessPoint, PREMAccessPoint, AWSAccessPoint


    
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
    
    @pytest.mark.remote_data
    def test_accessible(self):
        access,msg = self.ap.accessible
        self.assertEqual(access, True)

    def test_download(self):
        path = self.ap.download()
        self.assertEqual(os.path.exists(path), True)