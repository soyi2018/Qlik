import smtplib
from os.path import basename
from email.header import Header
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate, formataddr
from airflow.configuration import conf


def send_mail(send_from, send_to, send_cc, send_bcc, subject, html, files=None,
              server="smtp.office365.com"):

    msgRoot = MIMEMultipart('related')
    msgRoot['From'] = send_from
    msgRoot['To'] = send_to
    msgRoot['Cc'] = send_cc
    msgRoot['Bcc'] = send_bcc
    msgRoot['Date'] = formatdate(localtime=True)
    msgRoot['Subject'] = subject
    rcpt = send_to.split(',')+send_cc.split(',')+send_bcc.split(',')

    msgAlt = MIMEMultipart('alternative')
    msgRoot.attach(msgAlt)

    if '<html>' in html:
        msgAlt.attach(MIMEText(html, 'html'))
        # path may need to be changed in different environment
        imgpath = r'./dags/xxx/images'
        imgs = ['xxx']
        for img in imgs:
            with open(imgpath+'/'+img+'.png', 'rb') as fp:
                msgImage = MIMEImage(fp.read())
            msgImage.add_header('Content-ID', '<'+img+'>')
            msgRoot.attach(msgImage)

    else:
        msgAlt.attach(MIMEText(html, 'plain'))

    for f in files or []:
        with open(f, "rb") as att:
            part = MIMEApplication(att.read(), Name=basename(f))
        # After the file is closed
        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
        msgRoot.attach(part)

    smtp_server = conf.get('smtp', 'SMTP_HOST')
    smtp_port = conf.getint('smtp', 'SMTP_PORT')
    smtp_starttls = conf.getboolean('smtp', 'SMTP_STARTTLS')
    smtp_ssl = conf.getboolean('smtp', 'SMTP_SSL')
    smtp_retry_limit = conf.getint('smtp', 'SMTP_RETRY_LIMIT')
    smtp_timeout = conf.getint('smtp', 'SMTP_TIMEOUT')
    smtp_user = conf.get('smtp', 'SMTP_USER')
    smtp_password = conf.get('smtp', 'SMTP_PASSWORD')

    for attempt in range(1, smtp_retry_limit + 1):
        try:
            if smtp_ssl:
                smtp = smtplib.SMTP_SSL(
                    host=smtp_server, port=smtp_port, timeout=smtp_timeout)
            else:
                smtp = smtplib.SMTP(
                    host=smtp_server, port=smtp_port, timeout=smtp_timeout)
        except smtplib.SMTPServerDisconnected:
            if attempt < smtp_retry_limit:
                continue
            raise

        if smtp_starttls:
            smtp.starttls()
        if smtp_user and smtp_password:
            smtp.login(smtp_user, smtp_password)
        smtp.sendmail(send_from, rcpt, msgRoot.as_string())
        smtp.quit()
        break
