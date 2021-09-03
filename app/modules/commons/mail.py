import smtplib
from email.mime.text import MIMEText

class Mail():

    def __init__(self, conf):
        self.__conf = conf

    def createMessage(self, mail_from, mail_to, title, message):
        msg = MIMEText(message)
        msg['From'] = mail_from
        msg['To'] = mail_to
        msg['Subject'] = title

        return msg.as_string()

    def send_mail(self, title, msg):
        mail_host = self.__conf.get('mail_host')
        mail_port = self.__conf.get('mail_port')
        mail_from = self.__conf.get('mail_from')  
        mail_to  = self.__conf.get('mail_to').split(',')
        mail_user_name = self.__conf.get('mail_user_name')
        mail_password = self.__conf.get('mail_password')

        server = smtplib.SMTP(mail_host, mail_port)  
        server.starttls()
        if mail_user_name is not '' and mail_password is not '':
            server.login(mail_user_name,mail_password)

        message = self.createMessage(mail_from, mail_to, title, msg)
        server.sendmail(mail_from, mail_to, message)  
        server.quit()
    