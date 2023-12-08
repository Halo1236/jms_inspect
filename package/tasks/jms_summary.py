import re

from .base import BaseTask, TaskType

__all__ = ['JmsSummaryTask']


class JmsSummaryTask(BaseTask):
    NAME = '堡垒机信息汇总'
    TYPE = TaskType.VIRTUAL
    PRIORITY = 9998

    def __init__(self):
        super().__init__()
        self.mysql_client = None

    @staticmethod
    def get_do_params():
        return ['mysql_client']

    def _task_get_jms_summary(self):

        # 获取活跃用户总数
        sql = "SELECT COUNT(*) FROM users_user WHERE is_service_account=0 AND last_login IS NOT NULL"
        self.mysql_client.execute(sql)
        user_count = self.mysql_client.fetchone()[0]
        # 获取资产总数
        sql = "SELECT COUNT(*) FROM assets_asset"
        self.mysql_client.execute(sql)
        asset_count = self.mysql_client.fetchone()[0]
        # 获取会话总数
        sql = "SELECT COUNT(*) FROM terminal_session"
        self.mysql_client.execute(sql)
        sessions = self.mysql_client.fetchone()[0]

        # 获取基础平台为 Windows 及 Linux 的平台 ID
        sql_1 = "SELECT type, id FROM assets_platform WHERE type=%s OR type=%s"
        # 获取基于某个平台的资产总数
        sql_2 = "SELECT COUNT(*) FROM assets_asset WHERE platform_id IN %s"
        self.mysql_client.execute(sql_1, ('windows', 'linux'))
        result6_1 = self.mysql_client.fetchall()
        linux_id_list = []
        windows_id_list = []
        for type, p_id in result6_1:
            if type == 'windows':
                windows_id_list.append(p_id)
            else:
                linux_id_list.append(p_id)
        self.mysql_client.execute(sql_2, (linux_id_list,))
        linux_asset_count = self.mysql_client.fetchone()[0]
        self.mysql_client.execute(sql_2, (windows_id_list,))
        windows_asset_count = self.mysql_client.fetchone()[0]

        # 获取数据库资产总数
        sql_1 = "SELECT id FROM assets_platform WHERE category=%s"
        # 获取基于某个平台的资产总数
        sql_2 = "SELECT COUNT(*) FROM assets_asset WHERE platform_id IN %s"
        self.mysql_client.execute(sql_1, ('database',))
        result7_1 = self.mysql_client.fetchall()
        self.mysql_client.execute(sql_2, (result7_1,))
        database_asset_count = self.mysql_client.fetchone()[0]

        # 获取组织数量
        sql = "SELECT COUNT(*) FROM orgs_organization"
        self.mysql_client.execute(sql)
        organization_count = self.mysql_client.fetchone()[0]
        # 获取最大单日登录次数
        sql = "SELECT DATE(datetime) AS d, COUNT(*) AS num FROM audits_userloginlog " \
              "WHERE status=1 GROUP BY d ORDER BY num DESC LIMIT 1"
        self.mysql_client.execute(sql)
        res = self.mysql_client.fetchone()
        max_login_count = '%s(%s)' % (res[1], res[0])
        # 最大单日访问资产数
        sql = "SELECT DATE(date_start) AS d, COUNT(*) AS num FROM terminal_session " \
              "GROUP BY d ORDER BY num DESC LIMIT 1"
        self.mysql_client.execute(sql)
        res = self.mysql_client.fetchone()
        if res:
            max_connect_asset_count = '%s(%s)' % (res[1], res[0])
        else:
            max_connect_asset_count = '0'

        # 近三月最大单日用户登录数
        sql = "SELECT DATE(datetime) AS d, COUNT(*) AS num FROM audits_userloginlog " \
              "WHERE status=1 AND datetime > DATE_SUB(CURDATE(), INTERVAL 3 MONTH) " \
              "GROUP BY d ORDER BY num DESC LIMIT 1"
        self.mysql_client.execute(sql)
        res = self.mysql_client.fetchone()
        if res:
            last_3_month_max_login_count = '%s(%s)' % (res[1], res[0])
        else:
            last_3_month_max_login_count = '0'

        # 近三月最大单日资产登录数
        sql = "SELECT DATE(date_start) AS d, COUNT(*) AS num FROM  terminal_session " \
              "WHERE date_start > DATE_SUB(CURDATE(), INTERVAL 3 MONTH) " \
              "GROUP BY d ORDER BY num DESC LIMIT 1"
        self.mysql_client.execute(sql)
        res = self.mysql_client.fetchone()
        if res:
            last_3_month_max_connect_asset_count = '%s(%s)' % (res[1], res[0])
        else:
            last_3_month_max_connect_asset_count = '0'

        # 近一月登录用户数
        sql = "SELECT COUNT(DISTINCT username) FROM audits_userloginlog " \
              "WHERE status=1 AND datetime > DATE_SUB(CURDATE(), INTERVAL 1 MONTH)"
        self.mysql_client.execute(sql)
        last_1_month_login_count = self.mysql_client.fetchone()[0]
        # 近一月登录资产数
        sql = "SELECT COUNT(*) FROM terminal_session " \
              "WHERE date_start > DATE_SUB(CURDATE(), INTERVAL 1 MONTH)"
        self.mysql_client.execute(sql)
        last_1_month_connect_asset_count = self.mysql_client.fetchone()[0]
        # 近一月文件上传数
        sql = "SELECT COUNT(*) FROM audits_ftplog WHERE operate='Upload' " \
              "AND date_start > DATE_SUB(CURDATE(), INTERVAL 3 MONTH)"
        self.mysql_client.execute(sql)
        last_1_month_upload_count = self.mysql_client.fetchone()[0]
        # 近三月登录用户数
        sql = "SELECT COUNT(DISTINCT username) FROM audits_userloginlog " \
              "WHERE status=1 AND datetime > DATE_SUB(CURDATE(), INTERVAL 3 MONTH)"
        self.mysql_client.execute(sql)
        last_3_month_login_count = self.mysql_client.fetchone()[0]
        # 近三月登录资产数
        sql = "SELECT COUNT(*) FROM terminal_session " \
              "WHERE date_start > DATE_SUB(CURDATE(), INTERVAL 3 MONTH)"
        self.mysql_client.execute(sql)
        last_3_month_connect_asset_count = self.mysql_client.fetchone()[0]
        # 近三月文件上传数
        sql = "SELECT COUNT(*) FROM audits_ftplog WHERE operate='Upload' " \
              "AND date_start > DATE_SUB(CURDATE(), INTERVAL 3 MONTH)"
        self.mysql_client.execute(sql)
        last_3_month_upload_count = self.mysql_client.fetchone()[0]
        # 近三月命令记录数
        sql = "SELECT COUNT(*) FROM terminal_command WHERE " \
              "FROM_UNIXTIME(timestamp) > DATE_SUB(CURDATE(), INTERVAL 3 MONTH)"
        self.mysql_client.execute(sql)
        last_3_month_command_count = self.mysql_client.fetchone()[0]
        # 近三月高危命令记录数
        sql = "SELECT count(*) FROM terminal_command WHERE risk_level=5 and " \
              "FROM_UNIXTIME(timestamp) > DATE_SUB(CURDATE(), INTERVAL 3 MONTH)"
        self.mysql_client.execute(sql)
        res = self.mysql_client.fetchone()
        if res:
            last_3_month_danger_command_count = res[0]
        else:
            last_3_month_danger_command_count = '0'

        # 近三月最大会话时长
        sql = "SELECT timediff(date_end, date_start) AS duration from terminal_session " \
              "WHERE date_start > DATE_SUB(CURDATE(), INTERVAL 3 MONTH) " \
              "ORDER BY duration DESC LIMIT 1"
        self.mysql_client.execute(sql)
        res = self.mysql_client.fetchone()
        if res:
            last_3_month_max_session_duration = res[0]
        else:
            last_3_month_max_session_duration = '0'

        # 近三月平均会话时长
        sql = "SELECT ROUND(AVG(TIME_TO_SEC(TIMEDIFF(date_end, date_start))), 0) AS duration " \
              "FROM terminal_session WHERE date_start > DATE_SUB(CURDATE(), INTERVAL 3 MONTH)"
        self.mysql_client.execute(sql)
        res = self.mysql_client.fetchone()
        if res:
            last_3_month_avg_session_duration = res[0]
        else:
            last_3_month_avg_session_duration = '0'

        # 近三月工单申请数
        sql = "SELECT COUNT(*) FROM tickets_ticket WHERE date_created > DATE_SUB(CURDATE(), INTERVAL 3 MONTH)"
        self.mysql_client.execute(sql)
        res = self.mysql_client.fetchone()
        if res:
            last_3_month_ticket_count = res[0]
        else:
            last_3_month_ticket_count = '0'

        info_dict = {
            'user_count': user_count,
            'sessions': sessions,
            'asset_count': asset_count,
            'organization_count': organization_count,

            'linux_asset_count': linux_asset_count,
            'windows_asset_count': windows_asset_count,
            'database_asset_count': database_asset_count,

            'max_login_count': max_login_count,
            'max_connect_asset_count': max_connect_asset_count,
            'last_1_month_login_count': last_1_month_login_count,
            'last_1_month_connect_asset_count': last_1_month_connect_asset_count,
            'last_1_month_upload_count': last_1_month_upload_count,
            'last_3_month_connect_asset_count': last_3_month_connect_asset_count,
            'last_3_month_max_login_count': last_3_month_max_login_count,
            'last_3_month_max_connect_asset_count': last_3_month_max_connect_asset_count,
            'last_3_month_login_count': last_3_month_login_count,
            'last_3_month_upload_count': last_3_month_upload_count,
            'last_3_month_command_count': last_3_month_command_count,
            'last_3_month_danger_command_count': last_3_month_danger_command_count,
            'last_3_month_max_session_duration': str(last_3_month_max_session_duration),
            'last_3_month_avg_session_duration': last_3_month_avg_session_duration,
            'last_3_month_ticket_count': last_3_month_ticket_count
        }
        self.task_result.update(info_dict)

    def _task_get_chart_data(self):
        # 3个月用户登录折线图
        sql = "SELECT DATE(datetime) AS d, COUNT(*) AS num FROM audits_userloginlog " \
              "WHERE status=1 and DATE_SUB(CURDATE(),  INTERVAL 3 MONTH) <= datetime GROUP BY d"
        self.mysql_client.execute(sql)
        resp = self.mysql_client.fetchall()
        x = [str(i[0]) for i in resp]
        y = [i[1] for i in resp]
        self.task_result['user_login_chart'] = {'x': x, 'y': y}

        # 3个月资产登录折线图
        sql = "SELECT DATE(date_start) AS d, COUNT(*) AS num FROM terminal_session " \
              "WHERE DATE_SUB(CURDATE(),  INTERVAL 3 MONTH) <= date_start GROUP BY d"
        self.mysql_client.execute(sql)
        resp = self.mysql_client.fetchall()
        x = [str(i[0]) for i in resp]
        y = [i[1] for i in resp]
        self.task_result['asset_connect_chart'] = {'x': x, 'y': y}

        # 3个月活跃用户柱状图
        sql = "SELECT username, count(*) AS num FROM audits_userloginlog " \
              "WHERE status=1 and DATE_SUB(CURDATE(), INTERVAL 3 MONTH) <= datetime " \
              "GROUP BY username ORDER BY num DESC LIMIT 10;"
        self.mysql_client.execute(sql)
        resp = self.mysql_client.fetchall()
        x = [str(i[0]) for i in resp]
        y = [i[1] for i in resp]
        self.task_result['active_user_chart'] = {'x': x, 'y': y}

        # 近3个月活跃资产柱状图
        sql = "SELECT asset, count(*) AS num FROM terminal_session " \
              "WHERE DATE_SUB(CURDATE(), INTERVAL 3 MONTH) <= date_start " \
              "GROUP BY asset ORDER BY num DESC LIMIT 10;"
        self.mysql_client.execute(sql)
        resp = self.mysql_client.fetchall()
        x = [str(i[0]) for i in resp]
        y = [i[1] for i in resp]
        self.task_result['active_asset_chart'] = {'x': x, 'y': y}

        # 近3个月各种协议访问饼状图
        sql = "SELECT protocol, count(*) AS num FROM terminal_session " \
              "WHERE DATE_SUB(CURDATE(), INTERVAL 3 MONTH) <= date_start " \
              "GROUP BY protocol ORDER BY num DESC"
        self.mysql_client.execute(sql)
        self.task_result['protocol_chart'] = [
            {'name': i[0], 'value': i[1]} for i in self.mysql_client.fetchall()
        ]

        # 资产类型饼状图
        sql = "SELECT b.type AS platform, count(*) AS num FROM assets_asset a, assets_platform b " \
              "WHERE a.platform_id=b.id GROUP BY b.type ORDER BY num DESC LIMIT 10"
        self.mysql_client.execute(sql)
        self.task_result['platform_chart'] = [
            {'name': i[0], 'value': i[1]} for i in self.mysql_client.fetchall()
        ]

        # 组件类型使用情况饼状图
        sql = "SELECT a.name AS terminal, count(*) AS num FROM terminal a, terminal_session b " \
              "WHERE b.terminal_id= a.id GROUP BY terminal ORDER BY num DESC"
        self.mysql_client.execute(sql)
        self.task_result['terminal_chart'] = [
            {'name': re.findall(r'\[(.*?)\]', i[0])[0], 'value': i[1]} for i in self.mysql_client.fetchall()
        ]

        self.task_result['settings_chart'] = []
        # 功能使用情况
        sql = "SELECT name, category, `value` FROM settings_setting"
        self.mysql_client.execute(sql)

        focus_setting = {
            'TICKETS_ENABLED': ['工单', '是'],
            'TERMINAL_RAZOR_ENABLED': ['Razor组件', '是'],
            'TERMINAL_MAGNUS_ENABLED': ['Magnus组件', '否'],
            'SMS_ENABLED': ['短信认证', '是'],
            'AUTH_LDAP': ['LDAP认证', '否'],
            'AUTH_CAS': ['CAS认证', '否'],
            'AUTH_OPENID': ['OIDC认证', '是'],
            'AUTH_SAML2': ['SAML2认证', '是'],
            'AUTH_OAUTH2': ['OAuth2认证', '是'],
            'AUTH_DINGTALK': ['钉钉认证', '是'],
            'AUTH_WECOM': ['企业微信认证', '是'],
            'AUTH_FEISHU': ['飞书认证', '是'],
            'AUTH_RADIUS': ['Radius认证', '是'],
            'OTP_IN_RADIUS': ['使用Radius OTP', '是'],
            'SECURITY_MFA_AUTH': ['全局MFA认证', '否'],
            'CHANGE_ACCOUNT_SECRET': ['账号改密', '是'],
            'ACCOUNT_BACKUP': ['账号备份', '是'],
        }

        for i in self.mysql_client.fetchall():
            if focus_setting.get(i[0], None):
                self.task_result['settings_chart'].append(
                    {'name': focus_setting[i[0]][0], 'xpack': focus_setting[i[0]][1],
                     'value': 'false' if i[2] == '0' else i[2]})

        sql = "SELECT count(*) FROM accounts_changesecretautomation"
        self.mysql_client.execute(sql)
        res = self.mysql_client.fetchone()
        if res:
            self.task_result['settings_chart'].append(
                {'name': focus_setting['CHANGE_ACCOUNT_SECRET'][0],
                 'xpack': focus_setting['CHANGE_ACCOUNT_SECRET'][1], 'value': 'true' if res[0] else 'false'})

        sql = "SELECT count(*) FROM accounts_accountbackupautomation"
        self.mysql_client.execute(sql)
        res = self.mysql_client.fetchone()
        if res:
            self.task_result['settings_chart'].append(
                {'name': focus_setting['ACCOUNT_BACKUP'][0],
                 'xpack': focus_setting['ACCOUNT_BACKUP'][1], 'value': 'true' if res[0] else 'false'})
