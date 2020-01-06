import datetime
import time
import os
import sys
import smtplib

import mimetypes

from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class mailSender(object):
    
    def __init__(self, local_logger, smtp):
        self.smtp=smtp
        self.log=local_logger
    
    def sendalertsbymail(self,message,snapshot,recipients):
        
        for recip in recipients:
            try:
                if recipients[recip]['alerts']:
                    #self.log.info('Adding %s to the recipients list as %s' % (recip, recipients[recip]['email']))
                    self.sendmail(recipients[recip]['email'], '', message, snapshot)
                    time.sleep(.5)
            except:
                self.log.error('Error sending mail alerts',exc_info=True)

            
    def sendmail(self, recipients, subject, message, snapshot):

        if type(recipients).__name__=='str' or type(recipients).__name__=='unicode':
            recipients=recipients.split(",");
        
        commaspace = ', '
        outer = MIMEMultipart()
        outer['From'] = self.smtp['sender']
        outer['Subject']=subject
        outer['To'] = commaspace.join(recipients)
        textmessage = MIMEText(message, 'plain')

        try:
            if snapshot:
                msg = MIMEImage(snapshot,_subtype="jpg")
                msg.add_header('Content-Disposition', 'attachment', filename='alertsnap.jpg')
                outer.attach(msg)
        except:
            self.log.info('Error attaching image',exc_info=true)
        
        outer.attach(textmessage)

        try:
            composed = outer.as_string()
            s = smtplib.SMTP_SSL(self.smtp['server'],self.smtp['port'])
            s.login(self.smtp['user'],self.smtp['password'])
            #self.log.info("Alerting: "+str(recipients)+" to "+str(message).replace('\r', ' '))
            res=s.sendmail(self.smtp['sender'], recipients, composed)
            s.quit()
            if res:
                self.log.info('Warning: response from %s after sending: %s' % (self.smtp['server'],res))
            #else:
            #    self.log.info("Alert send completed for %s" % recipients)
        except:
            self.log.info("Error sending mail",exc_info=True)
