# _*_ coding: UTF-8 _*_
# @Time : 2024/5/10 17:28
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : send_email.py
# @Project : stockSpiderAndstockMonitor
import smtplib
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email import encoders
from email.utils import formataddr
# import gc


def do_send_mail(text, to_addr_list, email_subject):
    # if text is not None:
    send(text, to_addr_list, email_subject)
    # del text
    # del to_addr_list
    # del email_subject
    # gc.collect()


# python对SMTP支持有smtplib和email两个模块，其中，email负责构造邮件，smtplib负责发送邮件。
# 注：简单邮件传输协议 (Simple Mail Transfer Protocol, SMTP) ，SMTP是简单的基于的文本的协议
def send(text, to_addr_list, email_subject):
    # （QQ邮箱）
    email_host = "smtp.qq.com"  # smtp 邮件服务器
    host_port = 465  # smtp 邮件服务器端口：SSL 连接
    username = "1410959463@qq.com"  # 发件地址
    password = "drxdbmgdotnibada"
    # args是传入的收件地址，将收件地址转换成列表存储
    # to_addr_list = []
    # for i in args:
    #     to_addr_list.append(i)
    email_content = text

    display_name = '游元杰'
    email_address = '1410959463@qq.com'
    email_from = formataddr((display_name, email_address))
    # base_path = os.path.dirname(os.path.abspath(__file__))
    # reportfilename = file
    # source_path = reportfilename
    # part_name = pa_name
    sendmail = DoSendMail()
    email_obj = sendmail.build_email_obj(email_subject, email_from, to_addr_list)
    # todo:attach_content(email_obj, email_content),email_content是邮件正文
    sendmail.attach_content(email_obj, email_content)
    # sendmail.attach_part(email_obj, source_path, part_name)
    sendmail.send_email(email_obj, email_host, host_port, username, password, to_addr_list)
    # del email_address, display_name, email_from, sendmail
    # gc.collect()


class DoSendMail:
    # 构造邮件对象，设置邮件主题、发件人、收件人，最后返回邮件对象
    @classmethod
    def build_email_obj(cls, email_subject, email_from, to_addr_list):
        # email_subject:邮件主题    email_from:发件人  to_addr_list:收件人列表
        # 构造 MIMEMultipart 对象做为根容器
        email_obj = MIMEMultipart()
        email_to = ''.join(to_addr_list)  # 将收件人地址用“,”连接
        # 邮件主题、发件人、收件人
        email_obj['Subject'] = Header(email_subject, 'utf-8')
        email_obj['From'] = email_from
        email_obj['To'] = Header(email_to, 'utf-8')
        return email_obj

    @classmethod
    def attach_content(cls, email_obj, email_content, content_type='plain', charset='utf-8'):
        # 创建邮件正文，并将其附加到跟容器：邮件正文可以是纯文本，也可以是HTML（为HTML时，需设置content_type值为 'html'）
        # :param email_obj:邮件对象
        # :param email_content:邮件正文内容
        # :param content_type:邮件内容格式 'plain'、'html'..，默认为纯文本格式 'plain'
        # :param charset:编码格式，默认为 utf-8
        # :return:
        content = MIMEText(email_content, content_type, charset)  # 创建邮件正文对象
        email_obj.attach(content)  # 将邮件正文附加到根容器

    @classmethod
    def attach_part(cls, email_obj, source_path, part_name):
        # email_obj:邮件对象 source_path:附件源文件路径 part_name:附件名
        # 添加附件：附件可以为照片，也可以是文档
        part = MIMEBase('application', 'octet-stream')  # 'octet-stream': binary data   创建附件对象
        part.set_payload(open(source_path, 'rb').read())  # 将附件源文件加载到附件对象
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment', filename=('utf-8', '', '%s' % part_name))  # 给附件添加头文件
        email_obj.attach(part)  # 将附件附加到根容器

    @classmethod
    def send_email(cls, email_obj, email_host, host_port, username, password, to_addr_list):
        # email_obj:邮件对象    email_host:SMTP服务器主机    host_port:SMTP服务端口号 from_addr:发件地址  to_addr_list:收件地址
        try:
            '''
                # import smtplib
                # smtp_obj = smtplib.SMTP([host[, port[, local_hostname]]] )
                    # host: SMTP服务器主机。
                    # port: SMTP服务端口号，一般情况下SMTP端口号为25。
                # smtp_obj = smtplib.SMTP('smtp.qq.com', 25)
            '''
            smtp_obj = smtplib.SMTP_SSL(email_host, host_port)  # 连接 smtp 邮件服务器
            smtp_obj.login(username, password)  # 登录邮箱的地址，密码
            smtp_obj.sendmail(username, to_addr_list, email_obj.as_string())  # 发送邮件：email_obj.as_string()即发送的信息
            smtp_obj.quit()  # 关闭连接
            print("发送成功！")
            # # todo 发件人--》# 注意这里不是填邮箱密码而是授权码，授权码需要去邮箱设置里获取
            # smtp = SMTP(user="1410959463@qq.com", password="tukuqovcucdsicie", host="smtp.qq.com")
            # # todo 收件人
            # smtp.sender(to="13086397065@163.com", attachments=filename)
            return True
        except smtplib.SMTPException as e:
            print("发送失败！")
            print(e)
            return False
