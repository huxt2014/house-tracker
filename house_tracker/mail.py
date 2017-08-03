# coding=utf-8

import smtplib
import logging
import functools
from email.mime.text import MIMEText

from .models.base import sig_batch_job_done


logger = logging.getLogger(__name__)


def send_email(batch_job, address_list=None, smtp=None):
    s = smtplib.SMTP(smtp["host"])
    try:
        s.login(smtp["user"], smtp["password"])
    except smtplib.SMTPAuthenticationError as e:
        logger.error("smtp login failed: %s" % e)
        return

    content = batch_job.mail_content()

    for addr in address_list:
        msg = MIMEText(content)
        msg["Subject"] = batch_job.__str__()
        msg["From"] = smtp["user"]
        msg["To"] = addr
        s.send_message(msg)
        logger.info("send email to %s: %s" % (addr, batch_job))

    s.quit()


def send_when_batch_job_done(config):
    if not config.email_list:
        logger.info("no email address")
        return
    elif not config.smtp:
        logger.warning("smtp server not configured, email will not be sent.")
        return

    func = functools.partial(send_email,
                             address_list=config.email_list,
                             smtp=config.smtp)
    sig_batch_job_done.connect(func, weak=False)
