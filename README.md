# house-tracker

一个爬取www.fangdi.com.cn和sh.lianjia.com信息的工具，支持邮件通知与页面展示。

对于fangdi.com，每次完成任务后会发送邮件通知：

![](/docs/fangdi-email.jpeg)

对于sh.lianjia.com，页面展示价格走势：

![](/docs/lianjia-web.png)


## 安装方式

配置文件参考house_tracker_settings_example.py，另存为house_tracker_settings.py放到import可以找到的路径下。

clone后进入到目录

```bash
python setup.py sdist
pip install dist/house_tracker-1.0.0.tar.gz

house-tracker migrate upgrade head
```

仅在Python 3.4 3.5下运行过。



