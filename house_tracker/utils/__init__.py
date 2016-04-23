# coding=utf-8
 
import os
import time
import logging
import re
import math

import requests
from HTMLParser import HTMLParser

from .conf_tool import GlobalConfig
from .exceptions import DownloadError, ParseError


logger = logging.getLogger(__name__)
base_url = 'http://sh.lianjia.com/ershoufang'
house_num_per_page = 20.0


community_str = (u'小区均价.*?>(\d+|暂无均价)'
                 u'.*?'
                 u'正在出售中.*?(\d+).*?套'
                 u'.*?'
                 u'近90天成交.*?>(\d+).*?套'
                 u'.*?'
                 u'近30天带看.*?(\d+).*?次')

house_str = (u'(\d+)<span class="unit">万'
             u'.+?'
             u'(\d).+?(\d).+?厅'
             u'.+?'
             u'(\d+(\.\d+)?).+?平'
             u'.+?'
             u'>([低|中|高]层/\d+层)<'
             u'.+?'
             u'年代.+?(\d{4}|暂无数据)'
             u'.+?'
             u'近7天带看次数.+?(\d+)'
             u'.+?'
             u'总带看.+?(\d+)'
             )

pattern_community = re.compile(community_str, re.DOTALL)
pattern_house = re.compile(house_str, re.DOTALL)


class CommunityPageParser(HTMLParser):
    def __init__(self, content):
        HTMLParser.__init__(self)
        self.house_ids = []
        self.feed(content)
        
    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            if ('name', 'selectDetail') in attrs:
                for attr in attrs:
                    if attr[0] == 'title':
                        self.house_ids.append(attrs[1][1][2:])
                        break

def download_community_pages(community, c_record):
    """
    data_dir/week_number/community_outer_id/page1.html
    """
    
    data_dir = GlobalConfig().data_dir
    
    target_dir = os.path.join(data_dir, str(c_record.create_week))
    if not os.path.isdir(target_dir):
        logger.info('create target directory: %s' % target_dir)
        os.mkdir(target_dir)
    else:
        logger.info('find target directory: %s' % target_dir)
        
    community_dir = os.path.join(target_dir, community.outer_id)
    if not os.path.isdir(community_dir):
        logger.info('create community directory: %s' % community_dir)
        os.mkdir(community_dir)
    else:
        logger.info('find community directory: %s' % community_dir)
    
    house_ids = []
    
    for page_num in range(1,50):
        inner_uri = '/d%sq%ss8' % (page_num, community.outer_id)
        url = base_url + inner_uri
        file_path = '%s/page%s.html' % (community_dir, page_num)
        
        response = requests.get(url)
        # Save html page anyway, for checking latter if parse failed. 
        with open(file_path, 'w') as f:
            f.write(response.text.encode('utf-8'))
        time.sleep(GlobalConfig().time_interval)
        # check the page we got is the page we want. For example, if we request
        # /d1q5011000013303s8, the page we get should contain the following str:
        #  <a href="/ershoufang/d1q5011000013303s8" class="on">1</a>
        try:
            p_str = unicode(inner_uri + '.+?class="on".+?(\d+)')
            result = re.search(p_str, response.text, re.DOTALL)
            if not int(result.group(1)) == page_num:
                raise DownloadError('get an unexpected page: %s' % url)
        except Exception as e:
            raise DownloadError(('Get an unknow page %s' % url,)
                                + e.args)
            
        if page_num == 1:
            try:
                result = pattern_community.search(response.text)
                try:
                    c_record.average_price = community.average_price = int(result.group(1))
                except ValueError:
                    if result.group(1) == u'暂无均价':
                        c_record.average_price = community.average_price = 0
                    else:
                        msg = ('parse community average price failed: '
                                '%s->%s.') % (community.id, community.outer_id)
                        raise ParseError(msg)
                c_record.house_available = community.house_available = int(result.group(2))
                c_record.sold_last_season = community.sold_last_season = int(result.group(3))
                c_record.view_last_month = community.view_last_month = int(result.group(4))
                page_total = int(math.ceil(c_record.house_available
                                           /house_num_per_page)
                                 )
                logger.info(('id %s, %s pages, average price %s, %s available, '
                             '%s sold in last 90 days, %s view times in last '
                             '30 days') 
                             % (community.outer_id, page_total, 
                                c_record.average_price, 
                                c_record.house_available,
                                c_record.sold_last_season, 
                                c_record.view_last_month)
                            )
            except Exception as e:
                logger.exception(e)
                raise ParseError(('parse community profile failed',) +
                                 e.args)
        
        c_parser = CommunityPageParser(response.text)
        house_ids.extend(c_parser.house_ids)
        
        if page_num == page_total:
            if len(house_ids) != c_record.house_available:
                raise ParseError(('the first page of community %s said %s' 
                                  'houses available, but %s got.')
                                 % (community.outer_id, 
                                    c_record.house_available,
                                    len(house_ids))
                                 )
            else:
                break
    else:
        # When loop break as expected, download success. Else, maybe an error.
        raise DownloadError('pages overflow for community: %s' 
                            % community.outer_id)
        
    community.last_track_week = c_record.create_week
    
    return house_ids



        
def download_house_page(house, h_record, community_outer_id):
    """
    data_dir/week_number/community_outer_id/house22334455.html
    """
    
    data_dir = GlobalConfig().data_dir
    community_dir = os.path.join(data_dir, str(h_record.create_week),
                                 community_outer_id)
    if not os.path.isdir(community_dir):
        raise DownloadError('community directory not found: %s, house id is %s' 
                            % (community_dir, house.outer_id))
    
    # download page
    url = '%s/sh%s.html' % (base_url, house.outer_id)
    file_path = '%s/house%s.html' % (community_dir, house.outer_id)
    response = requests.get(url)
    with open(file_path, 'w') as f:
        f.write(response.text.encode('utf-8'))
    time.sleep(GlobalConfig().time_interval)
    # check the page we got is the page we want
    try:
        p_str = u'房源编号[：:]sh(%s)' % house.outer_id
        result = re.search(p_str, response.text)
        if str(result.group(1)) != str(house.outer_id):
            raise DownloadError('Request house %s but %s get, url is %s.' 
                                % (house.outer_id, result.group(1), url)
                                )
    except Exception as e:
        raise DownloadError(('Get an unknow page %s' % url,) + e.args)
    else:
        h_record.download_finish = True
    
    # parse page    
    try:
        result = pattern_house.search(response.text)
        if not result:
            raise ParseError('parse house page failed: %s' % url)
        h_record.price = house.price = int(result.group(1))
        h_record.view_last_week = house.view_last_week = int(result.group(8))
        h_record.view_last_month = house.view_last_month = int(result.group(9))
        
        if not house.room:
            house.room = u'%s室%s厅' % (result.group(2), result.group(3))
            house.area = float(result.group(4))
            house.floor = result.group(6)
            try:
                house.build_year = int(result.group(7))
            except ValueError:
                if result.group(7) == u'暂无数据':
                    house.build_year = 0
                else:
                    raise ParseError('parse build year failed: %s' % url)
            
        h_record.parse_finish = True
    except Exception as e:
        raise ParseError(('parse house page failed: %s' % url,) + e.args)
    
    house.last_track_week = h_record.create_week
    house.available = True

    
def assert_download_success(content, community=False, house=False):
    
    pass 
    
    
    
