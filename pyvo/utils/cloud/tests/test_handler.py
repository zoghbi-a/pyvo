import unittest
import pytest
import os
from pathlib import Path

import botocore

from astropy.io.votable import parse as votableparse
from pyvo.dal import DALResults, SIAResults

from pyvo.utils.cloud import handler

from pyvo.utils import activate_features
activate_features('cloud')

    
class TestJSONProcess(unittest.TestCase):
    """Tests process_cloud_json"""
    
    @classmethod
    def setUpClass(cls):
        samplexml = f'{Path(__file__).parent.as_posix()}/sample.xml'
        cls.dal_res = DALResults(votableparse(samplexml))
        cls.product = [_ for _ in cls.dal_res]
        cls.table_product = [_ for _ in cls.dal_res.to_table()]
        
    def test_wrong_colname(self):
        ap = handler.process_cloud_json(self.product, 'wrong_col')
        self.assertEqual(len(ap), 2)
        self.assertEqual(ap[0], [])
        self.assertEqual(ap[1], [])
        
    def test_basic_use(self):
        ap = handler.process_cloud_json(self.product, colname='cloud_access')
        self.assertEqual(len(ap), 2)
        self.assertEqual(len(ap[0]), 1)
        self.assertEqual(ap[0][0].s3_bucket_name, 'fornaxdev-east1-curated')
        self.assertEqual(ap[0][0].s3_key, 'FTP/chandra/data/byobsid/2/3052/primary/acisf03052N004_cntr_img2.jpg')
        
    def test_basic_use_table(self):
        ap = handler.process_cloud_json(self.table_product, colname='cloud_access')
        self.assertEqual(len(ap), 2)
        self.assertEqual(len(ap[0]), 1)
        self.assertEqual(ap[0][0].s3_bucket_name, 'fornaxdev-east1-curated')
        self.assertEqual(ap[0][0].s3_key, 'FTP/chandra/data/byobsid/2/3052/primary/acisf03052N004_cntr_img2.jpg')
        
    def test_meta_keyword(self):
        # pass a non-existent profile
        with self.assertRaises(botocore.exceptions.ProfileNotFound):
            ap = handler.process_cloud_json(self.product, colname='cloud_access', 
                                            aws_profile='nopr0file')
        

class TestUCDProcess(unittest.TestCase):
    """Tests process_cloud_ucd"""
    
    @classmethod
    def setUpClass(cls):
        samplexml = f'{Path(__file__).parent.as_posix()}/sample.xml'
        cls.dal_res = DALResults(votableparse(samplexml))
        cls.product = [_ for _ in cls.dal_res]
        cls.table_product = [_ for _ in cls.dal_res.to_table()]
        
    def test_basic_use(self):
        ap = handler.process_cloud_ucd(self.product)
        self.assertEqual(len(ap), 2)
        self.assertEqual(len(ap[0]), 1)
        self.assertEqual(ap[0][0].s3_bucket_name, 'heasarc-public')
        self.assertEqual(ap[0][0].s3_key, 'chandra/data/byobsid/2/3052/primary/acisf03052N004_cntr_img2.jpg')
        
    def test_basic_use_table(self):
        with self.assertRaises(ValueError):
            ap = handler.process_cloud_ucd(self.table_product)
        
        
class TestDatalinkProcess(unittest.TestCase):
    """Tests process_cloud_datalinks"""
    
    @classmethod
    def setUpClass(cls):
        cls.samplexml = f'{Path(__file__).parent.as_posix()}/sample.xml'
        cls.dal_res = SIAResults(votableparse(cls.samplexml))
        cls.product = [_ for _ in cls.dal_res]
        cls.table_product = [_ for _ in cls.dal_res.to_table()]
    
    def test_no_query_result(self):
        with self.assertRaises(ValueError):
            ap = handler.process_cloud_datalinks(products=self.product,
                                                 query_result=None)
    
    def test_basic_use_table(self):
        with self.assertRaises(ValueError):
            ap = handler.process_cloud_datalinks(products=self.table_product, query_result=self.dal_res)
            
    def test_no_datalinks(self):
        #with self.assertRaises(ValueError):
        res = DALResults(votableparse(self.samplexml))
        # remove datalink resource
        _ = res.votable.resources.pop()
        res = DALResults(res.votable)
        ap = handler.process_cloud_datalinks(products=[_ for _ in res],
                                             query_result=res)
        self.assertEqual(len(ap), 2)
        self.assertEqual(ap[0], [])
        self.assertEqual(ap[1], [])
        
    def test_wrong_provider_par(self):
        #with self.assertRaises(ValueError):
        ap = handler.process_cloud_datalinks(products=self.product, query_result=self.dal_res, 
                                             provider_par='n0provider')
        self.assertEqual(len(ap), 2)
        self.assertEqual(ap[0], [])
        self.assertEqual(ap[1], [])
    
    @pytest.mark.remote_data
    def test_basic_use(self):
        ap = handler.process_cloud_datalinks(products=self.product, query_result=self.dal_res)
        self.assertEqual(len(ap), 2)
        self.assertEqual(ap[0][-1].s3_bucket_name, 'heasarc-public')
        

class Test_generate_access_points(unittest.TestCase):
    """Tests generate_access_points"""
    
    @classmethod
    def setUpClass(cls):
        cls.samplexml = f'{Path(__file__).parent.as_posix()}/sample.xml'
        cls.dal_res = SIAResults(votableparse(cls.samplexml))
        cls.product = [_ for _ in cls.dal_res]
        cls.table_product = [_ for _ in cls.dal_res.to_table()]
    
    def test_wrong_arg_type(self):
        with self.assertRaises(ValueError):
            ap = handler.generate_access_points([1,2])
            
    def test_wrong_mode(self):
        with self.assertRaises(ValueError):
            ap = handler.generate_access_points(self.dal_res, mode='newmode')
            
    def test_mode_json(self):
        ap1 = handler.generate_access_points(self.dal_res, mode='json')
        ap2 = handler.process_cloud_json(self.product, colname='cloud_access')
        self.assertEqual(len(ap1), len(ap2))
        for a1,a2 in zip(ap1[0],ap2[0]):
            self.assertEqual(a1.uid, a2.uid)
            
    def test_mode_ucd(self):
        ap1 = handler.generate_access_points(self.dal_res, mode='ucd')
        ap2 = handler.process_cloud_ucd(self.product)
        self.assertEqual(len(ap1), len(ap2))
        for a1,a2 in zip(ap1[0],ap2[0]):
            self.assertEqual(a1.uid, a2.uid)

    @pytest.mark.remote_data        
    def test_mode_datalinks(self):
        ap1 = handler.generate_access_points(self.dal_res, mode='datalink')
        ap2 = handler.process_cloud_datalinks(self.product, self.dal_res)
        self.assertEqual(len(ap1), len(ap2))
        for a1,a2 in zip(ap1[0],ap2[0]):
            self.assertEqual(a1.uid, a2.uid)
            
    def test_return_list(self):
        ap1 = handler.generate_access_points(self.dal_res, mode='json')
        self.assertEqual(len(ap1), len(self.dal_res)) # number of records
        self.assertEqual(len(ap1[0]), 1) # number of access point for entry num 1
        

class Test_CloudHandler(unittest.TestCase):
    """Tests CloudHandler"""
    
    @classmethod
    def setUpClass(cls):
        cls.samplexml = f'{Path(__file__).parent.as_posix()}/sample.xml'
        cls.dal_res = SIAResults(votableparse(cls.samplexml))
        cls.product = [_ for _ in cls.dal_res]
        cls.table_product = [_ for _ in cls.dal_res.to_table()]
        
        cls.json_h = handler.CloudHandler(cls.dal_res, mode='json', urlcolumn='auto')
        
    def test__getdataurl_wrong_product(self):
        """wrong product type"""
        with self.assertRaises(ValueError):
            url = self.json_h._getdataurl(self.product)
        
    def test__getdataurl__unknown_urlcolumn(self):
        """urlcolname does not exist"""
        with self.assertRaises(ValueError):
            url = self.json_h._getdataurl(self.product[0], urlcolumn='nocolumn')
    
    def test__getdataurl__explicit_urlcolumn(self):
        """urlcolumn is expicit"""
        url = self.json_h._getdataurl(self.product[0], urlcolumn='SIA_format')
        self.assertEqual(url, 'image/jpeg')
        
    def test__getdataurl__auto_record_urlcolumn(self):
        """urlcolumn is auto and product is dal Record"""
        url = self.json_h._getdataurl(self.product[0], urlcolumn='auto')
        self.assertEqual(url, self.product[0]['access_url'])
        
    def test__getdataurl__auto_row_urlcolumn(self):
        """urlcolumn is auto and product is Row"""
        url = self.json_h._getdataurl(self.table_product[0], urlcolumn='auto')
        self.assertEqual(url, self.product[0]['access_url'])
        
    def test__getdataurl__urlcolumn_is_None(self):
        """urlcolumn is None"""
        url = self.json_h._getdataurl(self.table_product[0], urlcolumn=None)
        self.assertEqual(url, None)
        
        
    def test__init__product_is_list(self):
        """initialization: product_is_list"""
        json_h = handler.CloudHandler(self.dal_res, mode='json', urlcolumn='auto')
        self.assertEqual(json_h.product_is_list, True)
        
        json_h = handler.CloudHandler(self.dal_res[0], mode='json', urlcolumn='auto')
        self.assertEqual(json_h.product_is_list, False)
        
    def test__init__access_points(self):
        """initialization: access_points"""
        json_h = handler.CloudHandler(self.dal_res, mode='json', urlcolumn='auto')
        self.assertEqual(len(json_h.access_points), len(self.dal_res)) # num. of rows
        self.assertEqual(len(json_h.access_points[0].access_points), 2) # access points: 1 prem, 1 aws
