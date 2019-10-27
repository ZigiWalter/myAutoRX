#!/usr/bin/env python
#
#   radiosonde_auto_rx - Email Notification
#
#   Copyright (C) 2018 Philip Heron <phil@sanslogic.co.uk>
#   Released under GNU GPL v3 or later

import datetime
import logging
import time
import smtplib
from email.mime.text import MIMEText
from email.utils import formatdate
from threading import Thread
from .config import read_auto_rx_config
from .utils import position_info
import socket

try:
    # Python 2
    from Queue import Queue

except ImportError:
    # Python 3
    from queue import Queue


class EmailNotification(object):
    """ Radiosonde Email Notification Class.

    Accepts telemetry dictionaries from a decoder, and sends an email on newly detected sondes.
    Incoming telemetry is processed via a queue, so this object should be thread safe.

    """

    # We require the following fields to be present in the input telemetry dict.
    REQUIRED_FIELDS = [ 'id', 'lat', 'lon', 'alt', 'type', 'freq']

    def __init__(self, smtp_server = 'localhost', smtp_port=25, smtp_authentication='None', smtp_login="None", smtp_password="None", mail_from = None, mail_to = None, mail_subject = None, station_position = None):
        """ Init a new E-Mail Notification Thread """
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.smtp_authentication = smtp_authentication
        self.smtp_login = smtp_login
        self.smtp_password = smtp_password
        self.mail_from = mail_from
        self.mail_to = mail_to
        self.mail_subject = mail_subject
        self.station_position = station_position

        # Dictionary to track sonde IDs
        self.sondes = {}

        # Input Queue.
        self.input_queue = Queue()

        # Start queue processing thread.
        self.input_processing_running = True
        self.input_thread = Thread(target = self.process_queue)
        self.input_thread.start()

        self.log_info("Started E-Mail Notifier Thread")


    def add(self, telemetry):
        """ Add a telemetery dictionary to the input queue. """
        # Check the telemetry dictionary contains the required fields.
        for _field in self.REQUIRED_FIELDS:
            if _field not in telemetry:
                self.log_error("JSON object missing required field %s" % _field)
                return

        # Add it to the queue if we are running.
        if self.input_processing_running:
            self.input_queue.put(telemetry)
        else:
            self.log_error("Processing not running, discarding.")


    def process_queue(self):
        """ Process packets from the input queue. """
        while self.input_processing_running:

            # Process everything in the queue.
            while self.input_queue.qsize() > 0:
                try:
                    _telem = self.input_queue.get_nowait()
                    self.process_telemetry(_telem)

                except Exception as e:
                    self.log_error("Error processing telemetry dict - %s" % str(e))

            # Sleep while waiting for some new data.
            time.sleep(0.5)


    def process_telemetry(self, telemetry):
        """ Process a new telemmetry dict, and send an e-mail if it is a new sonde. """
        _id = telemetry['id']

        if _id not in self.sondes:
            try:
                hostname = socket.gethostname();
                myIp=(([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")] or [[(s.connect(("8.8.8.8", 53)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) + ["no IP found"])[0]

                # This is a new sonde. Send the email.
                msg  = 'Sonde launch detected:\n'
                msg += '\n'

                if 'encrypted' in telemetry:
                    msg += "ENCRYPTED RADIOSONDE DETECTED!\n"

                msg += 'Callsign:  %s\n' % _id
                msg += 'Type:      %s\n' % telemetry['type']
                msg += 'Frequency: %s\n' % telemetry['freq']
                msg += 'Position:  %.5f,%.5f\n' % (telemetry['lat'], telemetry['lon'])
                msg += 'Altitude:  %d m\n' % round(telemetry['alt'])
                
                if self.station_position != None:
                    _relative_position = position_info(self.station_position, (telemetry['lat'], telemetry['lon'], telemetry['alt']))
                    msg += 'Range:     %.1f km\n' % (_relative_position['straight_distance']/1000.0)
                    msg += 'Bearing:   %d degrees True\n' % int(_relative_position['bearing'])

                msg += '\n'
                #msg += 'https://tracker.habhub.org/#!qm=All&q=RS_%s\n' % _id
                msg += 'https://sondehub.org/%s\n' % _id
                msg += 'http://' + myIp + ':5000\n'
                # Construct subject
                _subject = "[" + hostname + "] " + self.mail_subject
                _subject = _subject.replace('<id>', telemetry['id'])
                _subject = _subject.replace('<type>', telemetry['type'])
                _subject = _subject.replace('<freq>', telemetry['freq'])

                if 'encrypted' in telemetry:
                    _subject += " - ENCRYPTED SONDE"

                logging.debug("Email - Subject: %s" % _subject)


                # Connect to the SMTP server.

                if self.smtp_authentication == 'SSL':
                    s = smtplib.SMTP_SSL(self.smtp_server, self.smtp_port)
                else:
                    s = smtplib.SMTP(self.smtp_server, self.smtp_port)

                if self.smtp_authentication == 'TLS':
                    s.starttls()

                if self.smtp_login != "None":
                    s.login(self.smtp_login, self.smtp_password) 

                # Send messages to all recepients.
                for _destination in self.mail_to.split(';'):
                    mime_msg = MIMEText(msg, 'plain', 'UTF-8')

                    mime_msg['From'] = self.mail_from
                    mime_msg['To'] = _destination
                    mime_msg["Date"] = formatdate()
                    mime_msg['Subject'] = _subject

                    s.sendmail(mime_msg['From'], _destination, mime_msg.as_string())

                    time.sleep(2)

                
                s.quit()

                self.log_info("E-mail sent.")
            except Exception as e:
                self.log_error("Error sending E-mail - %s" % str(e))

        self.sondes[_id] = { 'last_time': time.time() }


    def close(self):
        """ Close input processing thread. """
        self.log_debug("Waiting for processing thread to close...")
        self.input_processing_running = False

        if self.input_thread is not None:
            self.input_thread.join()


    def running(self):
        """ Check if the logging thread is running.

        Returns:
            bool: True if the logging thread is running.
        """
        return self.input_processing_running


    def log_debug(self, line):
        """ Helper function to log a debug message with a descriptive heading. 
        Args:
            line (str): Message to be logged.
        """
        logging.debug("E-Mail - %s" % line)


    def log_info(self, line):
        """ Helper function to log an informational message with a descriptive heading. 
        Args:
            line (str): Message to be logged.
        """
        logging.info("E-Mail - %s" % line)


    def log_error(self, line):
        """ Helper function to log an error message with a descriptive heading. 
        Args:
            line (str): Message to be logged.
        """
        logging.error("E-Mail - %s" % line)


if __name__ == "__main__":
    # Test Script - Send an example email using the settings in station.cfg

    logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.DEBUG)
    
    # Read in the station config, which contains the email settings.
    config = read_auto_rx_config('station.cfg', no_sdr_test=True)

    # Start up an email notifification object.
    _email_notification = EmailNotification(
        smtp_server = config['email_smtp_server'],
        smtp_port = config['email_smtp_port'],
        smtp_authentication = config['email_smtp_authentication'],
        smtp_login = config['email_smtp_login'],
        smtp_password = config['email_smtp_password'],
        mail_from = config['email_from'],
        mail_to = config['email_to'],
        mail_subject = config['email_subject']
    )

    # Wait a second..
    time.sleep(1)

    # Add in a packet of telemetry, which will cause the email notifier to send an email.
    _email_notification.add({'id':'N1234557', 'frame':10, 'lat':-10.0, 'lon':10.0, 'alt':10000, 'temp':1.0, 'type':'RS41', 'freq':'401.520 MHz', 'freq_float':401.52, 'heading':0.0, 'vel_h':5.1, 'vel_v':-5.0, 'datetime_dt':datetime.datetime.utcnow()})

    # Wait a little bit before shutting down.
    time.sleep(5)
    _email_notification.close()
