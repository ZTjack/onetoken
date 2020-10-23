from email.header import Header
from email.mime.text import MIMEText
from email.utils import parseaddr, formataddr
import smtplib


def _format_addr(s):
    name, addr = parseaddr(s)
    return formataddr((Header(name, 'utf-8').encode(), addr))


from_addr = '18616950387@163.com'
password = 'RCEDOIDUZBEWSBWW'
to_addr = '605330927@qq.com'
smtp_server = 'smtp.163.com'

msg = MIMEText('xxx策略停了，请快去看看', 'plain', 'utf-8')
msg['From'] = _format_addr('策略监控 <%s>' % from_addr)
msg['To'] = _format_addr('小菜鸡 <%s>' % to_addr)
msg['Subject'] = Header('策略停了！！！！！！', 'utf-8').encode()

server = smtplib.SMTP(smtp_server, 25)
server.set_debuglevel(1)
server.login(from_addr, password)
server.sendmail(from_addr, [to_addr], msg.as_string())
server.quit()
