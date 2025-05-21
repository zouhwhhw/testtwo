import requests
import json
import re
import sys
from pyzabbix import ZabbixAPI

from alibabacloud_tea_openapi.client import Client as OpenApiClient
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_tea_util import models as util_models
from alibabacloud_openapi_util.client import Client as OpenApiUtilClient


class Alivoice:
    def __init__(self, ak, sk, tts_code):
        self.ak = ak
        self.sk = sk
        self.tts_code = tts_code

    # 创建阿里云api请求客户端
    def create_client(self) -> OpenApiClient:
        config = open_api_models.Config(
            access_key_id=self.ak,
            access_key_secret=self.sk
        )
        # Endpoint 请参考 https://api.aliyun.com/product/Dyvmsapi
        config.endpoint = f'dyvmsapi.aliyuncs.com'
        return OpenApiClient(config)

    def create_api_info(self) -> open_api_models.Params:
        """
        API 相关
        @param path: params
        @return: OpenApi.Params
        """
        params = open_api_models.Params(
            # 接口名称,
            action='SingleCallByTts',
            # 接口版本,
            version='2017-05-25',
            # 接口协议,
            protocol='HTTPS',
            # 接口 HTTP 方法,
            method='POST',
            auth_type='AK',
            style='RPC',
            # 接口 PATH,
            pathname=f'/',
            # 接口请求体内容格式,
            req_body_type='json',
            # 接口响应体内容格式,
            body_type='json'
        )
        return params

    def send_alivoice_msg(self, called_number: str, msg: dict):
        client = self.create_client()
        params = self.create_api_info()
        # query params
        queries = {}
        queries['CalledNumber'] = called_number
        queries['TtsCode'] = self.tts_code
        queries['TtsParam'] = msg
        # runtime options
        runtime = util_models.RuntimeOptions()
        request = open_api_models.OpenApiRequest(
            query=OpenApiUtilClient.query(queries)
        )
        # 复制代码运行请自行打印 API 的返回值
        # 返回值为 Map 类型，可从 Map 中获得三类数据：响应体 body、响应头 headers、HTTP 返回的状态码 statusCode。
        client.call_api(params, request, runtime)


class Message:
    def __init__(self, zb_user: str, zb_pass: str, robot_url: str, called_number: list, alivoice: Alivoice):
        self.zb_user = zb_user
        self.zb_pass = zb_pass
        self.robot_url = robot_url
        self.called_number = called_number
        self.alivoice = alivoice

    # 调zabbix接口获取未恢复的告警
    def get_not_recovered_alert(self) -> list:
        # Zabbix服务器的URL
        zabbix_url = 'https://zabbix.yunwei-afs.com/api_jsonrpc.php'

        # 连接到Zabbix API
        zapi = ZabbixAPI(zabbix_url)
        zapi.login(self.zb_user, self.zb_pass)

        # # 查询未恢复的告警
        unresolved_alarms = zapi.trigger.get(
            output=['description', 'problemid', 'name', 'severity', 'status', 'host'],
            monitored=True,
            active=True,
            filter={
                "value": 1,  # 状态为1（触发）
                "description": "",  # 不限制描述
                "priority": [3, 4, 5],  # 一般严重及以上
                "state": 0,  # 状态为0（未恢复）
            },
            sortfield='lastchange',
            sortorder='DESC',  # 按最后更新时间降序排列
            expandDescription=True,
            selectHosts=['host'],
        )

        # 获取未恢复的告警信息
        not_recovered_content = []
        if len(unresolved_alarms) > 0:
            for alarm in unresolved_alarms:
                temp = ">主机：{}  问题：{}\n".format(alarm['hosts'][0]['host'], alarm['description'])
                not_recovered_content.append(temp)

        # 登出API
        zapi.user.logout()
        return not_recovered_content

    # 发送告警到企业微信的请求
    def send_message(self, content: str) -> None:

        headers = {'Content-Type': 'application/json'}

        data = {
            "msgtype": "markdown",
            "markdown": {
                "content": content
            }
        }
        requests.post(self.robot_url, headers=headers, data=json.dumps(data))

    # 合并告警信息，触发告警
    def send_wechat_msg(self, subject: str, message: str) -> None:
        not_recovered_content = self.get_not_recovered_alert()

        # 存在未恢复告警则增加'未恢复告警列表'
        if len(not_recovered_content) > 0:
            # 发送企业微信告警消息
            content = subject + '\n' + message + '\n\n' + '>未恢复告警列表：' + '\n'
            temp = ''
            for msg in not_recovered_content:
                temp += msg
                if len(content + temp) > 3089:
                    self.send_request(content + temp)
                    temp = ''
            if temp != '':
                self.send_message(content + temp)

            # 电话通知
            sbj_patt = re.compile(r'PROBLEM')
            msg_patt = re.compile(r'Agent ping')
            host_patt = re.compile(r'>主机：.*')
            if sbj_patt.search(subject) and msg_patt.search(message):
                host = host_patt.search(message).group()
                host = host[host.index(">", 1):]
                host = host[:host.index("<", 1)]
                data = {"hostname": host, "msg": "宕机"}
                for n in self.called_number:
                    self.alivoice.send_alivoice_msg(n, json.dumps(data))
            return
        content = subject + '\n' + message + '\n'
        self.send_message(content)

        # 电话通知
        sbj_patt = re.compile(r'PROBLEM')
        msg_patt = re.compile(r'Agent ping')
        host_patt = re.compile(r'>主机：.*')
        if sbj_patt.search(subject) and msg_patt.search(message):
            host = host_patt.search(message).group()
            host = host[host.index(">", 1):]
            host = host[:host.index("<", 1)]
            data = {"hostname": host, "msg": "宕机"}
            for n in self.called_number:
                self.alivoice.send_alivoice_msg(n, json.dumps(data))


if __name__ == "__main__":
    # alivoice实例属性
    ak = ''
    sk = ''
    tts_code = ''

    # message实例属性
    zb_user = ''
    zb_pass = ''
    robot_url = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=56df4ca3-ff7d-4cb1-ad46-7a44e690a56f'
    # robot_url = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=24e29693-19e6-417c-b4ad-9f5c50faa29a'

    # 通知电话列表
    called_number = [
        '13925203230',
        '13168025640',
        '18129951908',
        '18269425385',
        '16620875313',
    ]

    # zabbix传入的变量
    subject = sys.argv[1]
    message = sys.argv[2]

    # 实例化语音对象，和Message类进行组合
    alivoice = Alivoice(ak, sk, tts_code)

    # 实例化消息对象，调用方法发送告警
    msg = Message(zb_user, zb_pass, robot_url, called_number, alivoice)
    msg.send_wechat_msg(subject, message)