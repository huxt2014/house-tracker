import os
import math
import unittest
import time
from datetime import date

from house_tracker.modules import (Community, House, CommunityRecord, 
                                   HouseRecord)
from house_tracker.utils import (download_community_pages, download_house_page,
                                 house_num_per_page)
from house_tracker.utils.conf_tool import GlobalConfig
from house_tracker.utils.exceptions import ParseError

class TestDownloadCommunity(unittest.TestCase):
    def setUp(self):
        self.config = GlobalConfig()
        self.config.data_dir = '/tmp/house_tracker'
        self.community_outer_ids = ['5011000003035','5011000014434']
        
        day_number = (date.today() - GlobalConfig().original_date).days
        self.week_number = int(math.ceil(day_number / 7.0))
    
    def test_dowload_and_parse(self):
        
        for outer_id in self.community_outer_ids:
            community = Community(outer_id=outer_id)
            c_record = CommunityRecord()
            self.assertEqual(c_record.create_week, self.week_number)
            
            house_ids = download_community_pages(community, c_record)
            
            # confirm download success
            self.assertTrue(os.path.isdir(os.path.join(self.config.data_dir,
                                                       str(self.week_number),
                                                       outer_id)))
            page_num = int(math.ceil(c_record.house_available/house_num_per_page))
            for num in range(1, page_num+1):
                file_path = os.path.join(self.config.data_dir,
                                         str(self.week_number),
                                         outer_id,
                                         'page%s.html'%num)
                self.assertTrue(os.path.isfile(file_path))
                
            # confirm parse result is valid
            # if parse success, community_record should get the right value
            for attr in ('average_price', 'house_available',
                         'sold_last_season', 'view_last_month'):
                self.assertIsInstance(getattr(c_record, attr), int, attr)
            for attr in ('house_download_finish', 'house_parse_finish'):
                self.assertFalse(getattr(c_record, attr), attr)
            self.assertEqual(c_record.house_available, len(house_ids))
            
            # if parse success, community should get the same value
            for attr in ('average_price', 'house_available',
                         'sold_last_season', 'view_last_month'):
                self.assertEqual(getattr(community, attr), 
                                 getattr(c_record, attr), attr)
            self.assertEqual(community.last_track_week, c_record.create_week)
            
            print community.__str__().encode('utf-8')
            print c_record.__str__().encode('utf-8')


class TestDownloadHouse(unittest.TestCase):
    def setUp(self):
        self.config = GlobalConfig()
        self.config.data_dir = '/tmp/house_tracker'
        self.community_outer_id = '5011000009386'
        
        day_number = (date.today() - GlobalConfig().original_date).days
        self.week_number = int(math.ceil(day_number / 7.0))
    
    def test_dowload_and_parse(self):
        
        community = Community(outer_id=self.community_outer_id)
        c_record = CommunityRecord()
        house_outer_ids = download_community_pages(community, c_record)
        
        for house_outer_id in house_outer_ids:
            
            house = House(outer_id = house_outer_id)
            h_record = HouseRecord()
            self.assertEqual(h_record.create_week, self.week_number)
            
            try:
                download_house_page(house, h_record, self.community_outer_id)
            except ParseError:
                # if download success but only parse failed, confirm download 
                # success only
                file_path = os.path.join(self.config.data_dir, 
                                     str(self.week_number),
                                     self.community_outer_id,
                                     'house%s.html' % house_outer_id)
                self.assertTrue(os.path.isfile(file_path), file_path)
            else:
                # if no exception, confirm download success
                file_path = os.path.join(self.config.data_dir, 
                                         str(self.week_number),
                                         self.community_outer_id,
                                         'house%s.html' % house_outer_id)
                self.assertTrue(os.path.isfile(file_path), file_path)
                
                ## confirm parse result is valid
                # house_record should get the right value
                for attr in ('price', 'view_last_month', 'view_last_week'):
                    self.assertIsInstance(getattr(h_record, attr), int, attr)
                
                # house should get the same value
                for attr in ('price', 'view_last_month', 'view_last_week'):
                    self.assertEqual(getattr(house, attr),  getattr(h_record, attr),
                                     attr)
                self.assertEqual(house.last_track_week, h_record.create_week)
                
                # house should get the right value
                self.assertIsInstance(house.area, float)
                self.assertIsInstance(house.room, unicode)
                self.assertIsInstance(house.build_year, int)
                self.assertIsInstance(house.floor, unicode)
                self.assertTrue(house.available)
                
            print house.__str__().encode('utf-8')
            print h_record.__str__().encode('utf-8')
            
            time.sleep(self.config.time_interval)
    
                       
            
            
            