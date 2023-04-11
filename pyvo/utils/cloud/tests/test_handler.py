import unittest
import pytest
import os
from pathlib import Path

import botocore

from astropy.io.votable import parse as votableparse
from pyvo.dal import DALResults, SIAResults

from pyvo.utils.cloud import handler


    
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
        self.assertEqual(ap[0][0].s3_bucket_name, 'dh-fornaxdev')
        self.assertEqual(ap[0][0].s3_key, 'FTP/chandra/data/byobsid/2/3052/primary/acisf03052N004_cntr_img2.jpg')
        
    def test_basic_use_table(self):
        ap = handler.process_cloud_json(self.table_product, colname='cloud_access')
        self.assertEqual(len(ap), 2)
        self.assertEqual(len(ap[0]), 1)
        self.assertEqual(ap[0][0].s3_bucket_name, 'dh-fornaxdev')
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
        self.assertEqual(ap[0][0].s3_bucket_name, 'nasa-heasarc')
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
        self.assertEqual(ap[0][-1].s3_bucket_name, 'dh-fornaxdev')
        

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
            
    def test_wrong_mode_json(self):
        ap1 = handler.generate_access_points(self.dal_res, mode='json')
        ap2 = handler.process_cloud_json(self.product, colname='cloud_access')
        self.assertEqual(len(ap1), len(ap2))
        for a1,a2 in zip(ap1[0],ap2[0]):
            self.assertEqual(a1.uid, a2.uid)
            
    def test_wrong_mode_ucd(self):
        ap1 = handler.generate_access_points(self.dal_res, mode='ucd')
        ap2 = handler.process_cloud_ucd(self.product)
        self.assertEqual(len(ap1), len(ap2))
        for a1,a2 in zip(ap1[0],ap2[0]):
            self.assertEqual(a1.uid, a2.uid)

    @pytest.mark.remote_data        
    def test_wrong_mode_datalinks(self):
        ap1 = handler.generate_access_points(self.dal_res, mode='datalink')
        ap2 = handler.process_cloud_datalinks(self.product, self.dal_res)
        self.assertEqual(len(ap1), len(ap2))
        for a1,a2 in zip(ap1[0],ap2[0]):
            self.assertEqual(a1.uid, a2.uid)