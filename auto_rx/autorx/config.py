#!/usr/bin/env python
#
#   radiosonde_auto_rx - Configuration File Reader
#
#   Copyright (C) 2018  Mark Jessop <vk5qi@rfhead.net>
#   Released under GNU GPL v3 or later
#

import copy
import logging
import os
import traceback
import json
from configparser import RawConfigParser
from .sdr_wrappers import test_sdr

# Dummy initial config with some parameters we need to make the web interface happy.
global_config = {
    "min_freq": 400.0,
    "max_freq": 403.0,
    "snr_threshold": 10,
    "station_lat": 0.0,
    "station_lon": 0.0,
    "station_alt": 0.0,
}

# Web interface credentials
web_password = "none"

# Fixed minimum update rate for APRS
# This is set to avoid congestion on the APRS-IS network
# Please respect other users of the network and leave this setting as it is.
MINIMUM_APRS_UPDATE_RATE = 30


def read_auto_rx_config(filename, no_sdr_test=False):
    """Read an Auto-RX v2 Station Configuration File.

    This function will attempt to parse a configuration file.
    It will also confirm the accessibility of any SDRs specified in the config file.

    Args:
            filename (str): Filename of the configuration file to read.
            no_sdr_test (bool): Skip testing the SDRs (used for some unit tests)

    Returns:
            auto_rx_config (dict): The configuration dictionary.
            sdr_config (dict): A dictionary with SDR parameters.
    """
    global global_config, web_password
    # Configuration Defaults:
    auto_rx_config = {
        # Log Settings
        "per_sonde_log": True,
        # Email Settings
        "email_enabled": False,
        #'email_error_notifications': False,
        "email_smtp_server": "localhost",
        "email_smtp_port": 25,
        "email_smtp_authentication": "None",
        "email_smtp_login": "None",
        "email_smtp_password": "None",
        "email_from": "sonde@localhost",
        "email_to": None,
        "email_subject": "<type> Sonde launch detected on <freq>: <id>",
        "email_nearby_landing_subject": "Nearby Radiosonde Landing Detected - <id>",
        # SDR Settings
        "sdr_type": "RTLSDR",
        "sdr_hostname": "localhost",
        "sdr_port": 5555,
        "sdr_fm": "rtl_fm",
        "sdr_power": "rtl_power",
        "ss_iq_path": "./ss_iq",
        "ss_power_path": "./ss_power",
        "sdr_quantity": 1,
        # Search Parameters
        "min_freq": 400.4,
        "max_freq": 404.0,
        "rx_timeout": 120,
        "only_scan": [],
        "never_scan": [],
        "always_scan": [],
        "always_decode": [],
        # Location Settings
        "station_lat": 0.0,
        "station_lon": 0.0,
        "station_alt": 0.0,
        "station_code": "SONDE",  # NOTE: This will not be read from the config file, but will be left in place for now
        # as a default setting.
        "gpsd_enabled": False,
        "gpsd_host": "localhost",
        "gpsd_port": 2947,
        # Position Filter Settings
        "max_altitude": 50000,
        "max_radius_km": 1000,
        "min_radius_km": 0,
        "radius_temporary_block": False,
        # "sonde_time_threshold": 3, # Commented out to ensure warning message is shown.
        # Habitat Settings
        "habitat_uploader_callsign": "SONDE_AUTO_RX",
        "habitat_uploader_antenna": "1/4-wave",
        "habitat_upload_listener_position": False,
        # APRS Settings
        "aprs_enabled": False,
        "aprs_upload_rate": 30,
        "aprs_user": "N0CALL",
        "aprs_pass": "00000",
        "aprs_server": "rotate.aprs2.net",
        "aprs_object_id": "<id>",
        #'aprs_use_custom_object_id': False,
        "aprs_custom_comment": "Radiosonde Auto-RX <freq>",
        "aprs_position_report": False,
        "station_beacon_enabled": False,
        "station_beacon_rate": 30,
        "station_beacon_comment": "radiosonde_auto_rx SondeGate v<version>",
        "station_beacon_icon": "/r",
        # Web Settings,
        "web_host": "0.0.0.0",
        "web_port": 5000,
        "web_archive_age": 120,
        "web_control": False,
        # "web_password": "none",  # Commented out to ensure warning message is shown
        #'kml_refresh_rate': 10,
        # Advanced Parameters
        "search_step": 800,
        "snr_threshold": 10,
        "min_distance": 1000,
        "dwell_time": 10,
        "max_peaks": 10,
        "quantization": 10000,
        "decoder_spacing_limit": 15000,
        "synchronous_upload": False,
        "scan_dwell_time": 20,
        "detect_dwell_time": 5,
        "scan_delay": 10,
        "payload_id_valid": 5,
        "temporary_block_time": 60,
        "rs41_drift_tweak": False,
        "decoder_stats": False,
        "ngp_tweak": False,
        # Rotator Settings
        "enable_rotator": False,
        "rotator_update_rate": 30,
        "rotator_hostname": "127.0.0.1",
        "rotator_port": 4533,
        "rotation_threshold": 5.0,
        "rotator_homing_enabled": False,
        "rotator_homing_delay": 10,
        "rotator_home_azimuth": 0,
        "rotator_home_elevation": 0,
        "rotator_azimuth_only": False,
        # OziExplorer Settings
        "ozi_enabled": False,
        "ozi_update_rate": 5,
        "ozi_host": "<broadcast>",
        "ozi_port": 55681,
        "payload_summary_enabled": False,
        "payload_summary_host": "<broadcast>",
        "payload_summary_port": 55672,
        # Debugging settings
        "save_detection_audio": False,
        "save_decode_audio": False,
        "save_decode_iq": False,
        "save_raw_hex": False,
        "save_system_log": False,
        "enable_debug_logging": False,
        "save_cal_data": False,
        # New Sondehub DB Settings
        "sondehub_enabled": True,
        "sondehub_upload_rate": 30,
        # "sondehub_contact_email": "none@none.com" # Commented out to ensure a warning message is shown on startup
        "wideband_sondes": False, # Wideband sonde detection / decoding
    }

    try:

        # Check the file exists.
        if not os.path.isfile(filename):
            logging.critical("Config file %s does not exist!" % filename)
            return None

        config = RawConfigParser(auto_rx_config)
        config.read(filename)

        # Log Settings
        auto_rx_config["per_sonde_log"] = config.getboolean("logging", "per_sonde_log")

        # Email Settings
        if config.has_option("email", "email_enabled"):
            try:
                auto_rx_config["email_enabled"] = config.getboolean(
                    "email", "email_enabled"
                )
                auto_rx_config["email_smtp_server"] = config.get("email", "smtp_server")
                auto_rx_config["email_smtp_port"] = config.get("email", "smtp_port")
                auto_rx_config["email_smtp_authentication"] = config.get(
                    "email", "smtp_authentication"
                )
                auto_rx_config["email_smtp_login"] = config.get("email", "smtp_login")
                auto_rx_config["email_smtp_password"] = config.get(
                    "email", "smtp_password"
                )
                auto_rx_config["email_from"] = config.get("email", "from")
                auto_rx_config["email_to"] = config.get("email", "to")
                auto_rx_config["email_subject"] = config.get("email", "subject")

                if auto_rx_config["email_smtp_authentication"] not in [
                    "None",
                    "TLS",
                    "SSL",
                ]:
                    logging.error(
                        "Config - Invalid email authentication setting. Must be None, TLS or SSL."
                    )
                    return None

            except:
                logging.error("Config - Invalid or missing email settings. Disabling.")
                auto_rx_config["email_enabled"] = False

        # SDR Settings
        auto_rx_config["sdr_fm"] = config.get("advanced", "sdr_fm_path")
        auto_rx_config["sdr_power"] = config.get("advanced", "sdr_power_path")
        auto_rx_config["sdr_quantity"] = config.getint("sdr", "sdr_quantity")

        # Search Parameters
        auto_rx_config["min_freq"] = config.getfloat("search_params", "min_freq")
        auto_rx_config["max_freq"] = config.getfloat("search_params", "max_freq")
        auto_rx_config["rx_timeout"] = config.getint("search_params", "rx_timeout")

        if (
            config.has_option("search_params", "only_scan")
            and config.get("search_params", "only_scan") != ""
        ):  # check if user has new name for scan lists
            auto_rx_config["only_scan"] = json.loads(
                config.get("search_params", "only_scan")
            )
        else:
            logging.warning(
                "Config - whitelist configuration has been deprecated and replaced with only_scan list"
            )
            auto_rx_config["only_scan"] = json.loads(
                config.get("search_params", "whitelist")
            )

        if (
            config.has_option("search_params", "never_scan")
            and config.get("search_params", "never_scan") != ""
        ):  # check if user has new name for scan lists
            auto_rx_config["never_scan"] = json.loads(
                config.get("search_params", "never_scan")
            )
        else:
            logging.warning(
                "Config - blacklist configuration has been deprecated and replaced with never_scan list"
            )
            auto_rx_config["never_scan"] = json.loads(
                config.get("search_params", "blacklist")
            )

        if (
            config.has_option("search_params", "always_scan")
            and config.get("search_params", "always_scan") != ""
        ):  # check if user has new name for scan lists
            auto_rx_config["always_scan"] = json.loads(
                config.get("search_params", "always_scan")
            )
        else:
            logging.warning(
                "Config - greylist configuration has been deprecated and replaced with always_scan list"
            )
            auto_rx_config["always_scan"] = json.loads(
                config.get("search_params", "greylist")
            )

        # Location Settings
        auto_rx_config["station_lat"] = config.getfloat("location", "station_lat")
        auto_rx_config["station_lon"] = config.getfloat("location", "station_lon")
        auto_rx_config["station_alt"] = config.getfloat("location", "station_alt")

        if auto_rx_config["station_lat"] > 90.0 or auto_rx_config["station_lat"] < -90.0:
            logging.critical("Config - Invalid Station Latitude! (Outside +/- 90 degrees!)")
            return None
        
        if auto_rx_config["station_lon"] > 180.0 or auto_rx_config["station_lon"] < -180.0:
            logging.critical("Config - Invalid Station Longitude! (Outside +/- 180 degrees!)")
            return None


        # Position Filtering
        auto_rx_config["max_altitude"] = config.getint("filtering", "max_altitude")
        auto_rx_config["max_radius_km"] = config.getint("filtering", "max_radius_km")
        auto_rx_config['geo_filter_enable'] = config.getboolean('filtering', 'geo_filter_enable')
        auto_rx_config['decode_limit_period'] = config.getint('filtering', 'decode_limit_period')
        auto_rx_config['decode_limit_min_alt'] = config.getint('filtering', 'decode_limit_min_alt')
        auto_rx_config['brownlist'] = json.loads(config.get('filtering', 'brownlist'))
        auto_rx_config['black_types'] = json.loads(config.get('filtering', 'black_types'))
        auto_rx_config['block_on_detect_fail_time'] = config.getint('filtering', 'block_on_detect_fail_time')
        auto_rx_config['block_on_detect_fail_count'] = config.getint('filtering', 'block_on_detect_fail_count')
        auto_rx_config['block_on_first_detect_fail_count'] = config.getint('filtering', 'block_on_first_detect_fail_count')    
        auto_rx_config['no_auto_block'] = json.loads(config.get('filtering', 'no_auto_block'))
        auto_rx_config['auto_block_min_band_width'] = config.getint('filtering', 'auto_block_min_band_width')
        auto_rx_config['imet_upload_filter_polygon_lat'] = json.loads(config.get('filtering', 'imet_upload_filter_polygon_lat'))
        auto_rx_config['imet_upload_filter_polygon_lon'] = json.loads(config.get('filtering', 'imet_upload_filter_polygon_lon'))

        auto_rx_config["habitat_uploader_callsign"] = config.get(
            "habitat", "uploader_callsign"
        )
        auto_rx_config["habitat_upload_listener_position"] = config.getboolean(
            "habitat", "upload_listener_position"
        )
        auto_rx_config["habitat_uploader_antenna"] = config.get(
            "habitat", "uploader_antenna"
        ).strip()

        # APRS Settings
        auto_rx_config["aprs_enabled"] = config.getboolean("aprs", "aprs_enabled")
        auto_rx_config["aprs_upload_rate"] = config.getint("aprs", "upload_rate")
        auto_rx_config["aprs_user"] = config.get("aprs", "aprs_user")
        auto_rx_config["aprs_pass"] = config.get("aprs", "aprs_pass")
        auto_rx_config["aprs_server"] = config.get("aprs", "aprs_server")
        auto_rx_config["aprs_object_id"] = config.get("aprs", "aprs_object_id")
        auto_rx_config["aprs_custom_comment"] = config.get(
            "aprs", "aprs_custom_comment"
        )
        # 2021-08-08 - Disable option for producing APRS position reports.
        #auto_rx_config["aprs_position_report"] = config.getboolean(
        #    "aprs", "aprs_position_report"
        #)
        auto_rx_config["aprs_position_report"] = False
        auto_rx_config["station_beacon_enabled"] = config.getboolean(
            "aprs", "station_beacon_enabled"
        )
        auto_rx_config["station_beacon_rate"] = config.getint(
            "aprs", "station_beacon_rate"
        )
        auto_rx_config["station_beacon_comment"] = config.get(
            "aprs", "station_beacon_comment"
        )
        auto_rx_config["station_beacon_icon"] = config.get(
            "aprs", "station_beacon_icon"
        )

        if auto_rx_config["aprs_upload_rate"] < MINIMUM_APRS_UPDATE_RATE:
            logging.warning(
                "Config - APRS Update Rate clipped to minimum of %d seconds."
                % MINIMUM_APRS_UPDATE_RATE
            )
            auto_rx_config["aprs_upload_rate"] = MINIMUM_APRS_UPDATE_RATE

        # OziPlotter Settings
        auto_rx_config["ozi_enabled"] = config.getboolean("oziplotter", "ozi_enabled")
        auto_rx_config["ozi_update_rate"] = config.getint(
            "oziplotter", "ozi_update_rate"
        )
        auto_rx_config["ozi_port"] = config.getint("oziplotter", "ozi_port")
        auto_rx_config["payload_summary_enabled"] = config.getboolean(
            "oziplotter", "payload_summary_enabled"
        )
        auto_rx_config["payload_summary_port"] = config.getint(
            "oziplotter", "payload_summary_port"
        )

        # Advanced Settings
        auto_rx_config["search_step"] = config.getfloat("advanced", "search_step")
        auto_rx_config["snr_threshold"] = config.getfloat("advanced", "snr_threshold")
        auto_rx_config["min_distance"] = config.getfloat("advanced", "min_distance")
        auto_rx_config["dwell_time"] = config.getint("advanced", "dwell_time")
        auto_rx_config["quantization"] = config.getint("advanced", "quantization")
        auto_rx_config["max_peaks"] = config.getint("advanced", "max_peaks")
        auto_rx_config["scan_dwell_time"] = config.getint("advanced", "scan_dwell_time")
        auto_rx_config["detect_dwell_time"] = config.getint(
            "advanced", "detect_dwell_time"
        )
        auto_rx_config["scan_delay"] = config.getint("advanced", "scan_delay")
        auto_rx_config["payload_id_valid"] = config.getint(
            "advanced", "payload_id_valid"
        )
        auto_rx_config["synchronous_upload"] = config.getboolean(
            "advanced", "synchronous_upload"
        )

        # Rotator Settings
        auto_rx_config["rotator_enabled"] = config.getboolean(
            "rotator", "rotator_enabled"
        )
        auto_rx_config["rotator_update_rate"] = config.getint("rotator", "update_rate")
        auto_rx_config["rotator_hostname"] = config.get("rotator", "rotator_hostname")
        auto_rx_config["rotator_port"] = config.getint("rotator", "rotator_port")
        auto_rx_config["rotator_homing_enabled"] = config.getboolean(
            "rotator", "rotator_homing_enabled"
        )
        auto_rx_config["rotator_home_azimuth"] = config.getfloat(
            "rotator", "rotator_home_azimuth"
        )
        auto_rx_config["rotator_home_elevation"] = config.getfloat(
            "rotator", "rotator_home_elevation"
        )
        auto_rx_config["rotator_homing_delay"] = config.getint(
            "rotator", "rotator_homing_delay"
        )
        auto_rx_config["rotation_threshold"] = config.getfloat(
            "rotator", "rotation_threshold"
        )

        # Web interface settings.
        auto_rx_config["web_host"] = config.get("web", "web_host")
        auto_rx_config["web_port"] = config.getint("web", "web_port")
        auto_rx_config["web_archive_age"] = config.getint("web", "archive_age")

        auto_rx_config["save_detection_audio"] = config.getboolean(
            "debugging", "save_detection_audio"
        )
        auto_rx_config["save_decode_audio"] = config.getboolean(
            "debugging", "save_decode_audio"
        )
        auto_rx_config["save_decode_iq"] = config.getboolean(
            "debugging", "save_decode_iq"
        )

        # NOTE 2019-09-21: The station code will now be fixed at the default to avoid multiple iMet callsign issues.
        # auto_rx_config['station_code'] = config.get('location', 'station_code')
        # if len(auto_rx_config['station_code']) > 5:
        # 	auto_rx_config['station_code'] = auto_rx_config['station_code'][:5]
        # 	logging.warning("Config - Clipped station code to 5 digits: %s" % auto_rx_config['station_code'])

        auto_rx_config["temporary_block_time"] = config.getint(
            "advanced", "temporary_block_time"
        )

        # New demod tweaks - Added 2019-04-23
        # Default to experimental decoders on for FSK/GFSK sondes...
        auto_rx_config["experimental_decoders"] = {
            "RS41": True,
            "RS92": True,
            "DFM": True,
            "M10": True,
            "M20": True,
            "IMET": False,
            "IMET5": True,
            "LMS6": True,
            "MK2LMS": False,
            "MEISEI": True,
            "MTS01": False, # Until we test it
            "MRZ": False,  # .... except for the MRZ, until we know it works.
            "WXR301": True,
            "WXRPN9": True,
            "UDP": False,
        }

        auto_rx_config["decoder_spacing_limit"] = config.getint(
            "advanced", "decoder_spacing_limit"
        )
        # Use 'experimental' (not really, anymore!) decoders for RS41, RS92, M10, DFM and LMS6-400.
        # Don't allow overriding to the FM based decoders.
        # auto_rx_config["experimental_decoders"]["RS41"] = config.getboolean(
        #     "advanced", "rs41_experimental"
        # )
        # auto_rx_config["experimental_decoders"]["RS92"] = config.getboolean(
        #     "advanced", "rs92_experimental"
        # )
        # auto_rx_config["experimental_decoders"]["M10"] = config.getboolean(
        #     "advanced", "m10_experimental"
        # )
        # auto_rx_config["experimental_decoders"]["DFM"] = config.getboolean(
        #     "advanced", "dfm_experimental"
        # )
        # auto_rx_config["experimental_decoders"]["LMS6"] = config.getboolean(
        #     "advanced", "lms6-400_experimental"
        # )

        try:
            auto_rx_config["web_control"] = config.getboolean("web", "web_control")
            auto_rx_config["ngp_tweak"] = config.getboolean("advanced", "ngp_tweak")
            auto_rx_config["gpsd_enabled"] = config.getboolean(
                "location", "gpsd_enabled"
            )
            auto_rx_config["gpsd_host"] = config.get("location", "gpsd_host")
            auto_rx_config["gpsd_port"] = config.getint("location", "gpsd_port")
        except:
            logging.warning(
                "Config - Did not find web control / ngp_tweak / gpsd options, using defaults (disabled)"
            )
            auto_rx_config["web_control"] = False
            auto_rx_config["ngp_tweak"] = False
            auto_rx_config["gpsd_enabled"] = False

        try:
            auto_rx_config["min_radius_km"] = config.getint(
                "filtering", "min_radius_km"
            )
            auto_rx_config["radius_temporary_block"] = config.getboolean(
                "filtering", "radius_temporary_block"
            )
        except:
            logging.warning(
                "Config - Did not find minimum radius filter setting, using default (0km)."
            )
            auto_rx_config["min_radius_km"] = 0
            auto_rx_config["radius_temporary_block"] = False

        try:
            auto_rx_config["aprs_use_custom_object_id"] = config.getboolean(
                "aprs", "aprs_use_custom_object_id"
            )
        except:
            logging.warning(
                "Config - Did not find aprs_use_custom_object_id setting, using default (False)"
            )
            auto_rx_config["aprs_use_custom_object_id"] = False

        try:
            auto_rx_config["aprs_port"] = config.getint("aprs", "aprs_port")
        except:
            logging.warning(
                "Config - Did not find aprs_port setting - using default of 14590."
            )
            auto_rx_config["aprs_port"] = 14590

        try:
            auto_rx_config["email_error_notifications"] = config.getboolean(
                "email", "error_notifications"
            )
            auto_rx_config["email_launch_notifications"] = config.getboolean(
                "email", "launch_notifications"
            )
            auto_rx_config["email_landing_notifications"] = config.getboolean(
                "email", "landing_notifications"
            )
            auto_rx_config["email_landing_range_threshold"] = config.getfloat(
                "email", "landing_range_threshold"
            )
            auto_rx_config["email_landing_altitude_threshold"] = config.getfloat(
                "email", "landing_altitude_threshold"
            )
        except:
            logging.warning(
                "Config - Did not find new email settings (v1.3.3), using defaults"
            )
            auto_rx_config["email_error_notifications"] = False
            auto_rx_config["email_launch_notifications"] = True
            auto_rx_config["email_landing_notifications"] = True
            auto_rx_config["email_landing_range_threshold"] = 30
            auto_rx_config["email_landing_altitude_threshold"] = 1000

        try:
            auto_rx_config["kml_refresh_rate"] = config.getint(
                "web", "kml_refresh_rate"
            )
        except:
            logging.warning(
                "Config - Did not find kml_refresh_rate setting, using default (10 seconds)."
            )
            auto_rx_config["kml_refresh_rate"] = 10

        # New Sondehub db Settings
        try:
            auto_rx_config["sondehub_enabled"] = config.getboolean(
                "sondehub", "sondehub_enabled"
            )
            auto_rx_config["sondehub_upload_rate"] = config.getint(
                "sondehub", "sondehub_upload_rate"
            )
            if auto_rx_config["sondehub_upload_rate"] < 10:
                logging.warning(
                    "Config - Clipped Sondehub update rate to lower limit of 10 seconds"
                )
                auto_rx_config["sondehub_upload_rate"] = 10
        except:
            logging.warning(
                "Config - Did not find sondehub_enabled setting, using default (enabled / 15 seconds)."
            )
            auto_rx_config["sondehub_enabled"] = True
            auto_rx_config["sondehub_upload_rate"] = 15

        try:
            auto_rx_config["experimental_decoders"]["MRZ"] = config.getboolean(
                "advanced", "mrz_experimental"
            )
        except:
            logging.warning(
                "Config - Did not find MRZ decoder experimental decoder setting, using default (disabled)."
            )
            auto_rx_config["experimental_decoders"]["MRZ"] = False

        try:
            auto_rx_config["experimental_decoders"]["IMET5"] = config.getboolean(
                "advanced", "imet54_experimental"
            )
        except:
            logging.warning(
                "Config - Did not find iMet-54 decoder experimental decoder setting, using default (enabled)."
            )
            auto_rx_config["experimental_decoders"]["IMET5"] = True

        # Sondehub Contact email (1.5.1)
        try:
            auto_rx_config["sondehub_contact_email"] = config.get(
                "sondehub", "sondehub_contact_email"
            )
        except:
            logging.warning(
                "Config - Did not find Sondehub contact e-mail setting, using default (none)."
            )
            auto_rx_config["sondehub_contact_email"] = "none@none.com"

        # Sonde time threshold (1.5.1)
        try:
            auto_rx_config["sonde_time_threshold"] = config.getfloat(
                "filtering", "sonde_time_threshold"
            )
        except:
            logging.warning(
                "Config - Did not find Sonde Time Threshold, using default (3 hrs)."
            )
            auto_rx_config["sonde_time_threshold"] = 3

        # Web control password
        try:
            auto_rx_config["web_password"] = config.get("web", "web_password")
            if auto_rx_config["web_password"] == "none":
                logging.warning("Config - Web Password not set, disabling web control")
                auto_rx_config["web_control"] = True
        except:
            logging.warning(
                "Config - Did not find Web Password setting, using default (web control disabled)"
            )
            auto_rx_config["web_control"] = False
            auto_rx_config["web_password"] = "none"
        
        try:
            auto_rx_config["save_raw_hex"] = config.getboolean(
                "debugging", "save_raw_hex"
            )
        except:
            logging.warning(
                "Config - Did not find save_raw_hex setting, using default (disabled)"
            )
            auto_rx_config["save_raw_hex"] = False
        
        try:
            auto_rx_config["experimental_decoders"]["MK2LMS"] = config.getboolean(
                "advanced", "lms6-1680_experimental"
            )
        except:
            logging.warning(
                "Config - Did not find lms6-1680_experimental setting, using default (disabled)"
            )
            auto_rx_config["experimental_decoders"]["MK2LMS"] = False

        try:
            auto_rx_config["email_nearby_landing_subject"] = config.get(
                "email", "nearby_landing_subject"
            )
        except:
            logging.warning(
                "Config - Did not find email_nearby_landing_subject setting, using default"
            )
            auto_rx_config["email_nearby_landing_subject"] = "Nearby Radiosonde Landing Detected - <id>"


        # As of auto_rx version 1.5.10, we are limiting APRS output to only radiosondy.info,
        # and only on the non-forwarding port. 
        # This decision was not made lightly, and is a result of the considerable amount of
        # non-amateur traffic that radiosonde flights are causing within the APRS-IS network.
        # Until some form of common format can be agreed to amongst the developers of *all* 
        # radiosonde tracking software to enable radiosonde telemetry to be de-duped, 
        # I have decided to help reduce the impact on the wider APRS-IS network by restricting 
        # the allowed servers and ports.
        # If you are using another APRS-IS server that *does not* forward to the wider APRS-IS
        # network and want it allowed, then please raise an issue at
        # https://github.com/projecthorus/radiosonde_auto_rx/issues
        #
        # You are of course free to fork and modify this codebase as you wish, but please be aware
        # that this goes against the wishes of the radiosonde_auto_rx developers to not be part
        # of the bigger problem of APRS-IS congestion. 

        ALLOWED_APRS_SERVERS = ["radiosondy.info", "wettersonde.net", "localhost"]
        ALLOWED_APRS_PORTS = [14580, 14590]

        if auto_rx_config["aprs_server"] not in ALLOWED_APRS_SERVERS:
            logging.warning(
                "Please do not upload to servers which forward to the wider APRS-IS network and cause network congestion. Switching to default server of radiosondy.info. If you believe this to be in error, please raise an issue at https://github.com/projecthorus/radiosonde_auto_rx/issues"
            )
            auto_rx_config["aprs_server"] = "radiosondy.info"
        
        if auto_rx_config["aprs_port"] not in ALLOWED_APRS_PORTS:
            logging.warning(
                "Please do not use APRS ports which forward data out to the wider APRS-IS network and cause network congestion. Switching to default port of 14590. If you believe this to be in error, please raise an issue at https://github.com/projecthorus/radiosonde_auto_rx/issues"
            )
            auto_rx_config["aprs_port"] = 14590


        # 1.6.0 - New SDR options
        if not config.has_option("sdr", "sdr_type"):
            logging.warning(
                "Config - Missing sdr_type configuration option, defaulting to RTLSDR."
            )
            auto_rx_config["sdr_type"] = "RTLSDR"
        else:
            auto_rx_config["sdr_type"] = config.get("sdr", "sdr_type")

        try:
            auto_rx_config["sdr_hostname"] = config.get("sdr", "sdr_hostname")
            auto_rx_config["sdr_port"] = config.getint("sdr", "sdr_port")
            auto_rx_config["ss_iq_path"] = config.get("advanced", "ss_iq_path")
            auto_rx_config["ss_power_path"] = config.get("advanced", "ss_power_path")
        except:
            logging.debug("Config - Did not find new sdr_type associated options.")

        try:
            auto_rx_config["always_decode"] = json.loads(
                config.get("search_params", "always_decode")
            )
        except:
            logging.debug(
                "Config - No always_decode settings, defaulting to none."
            )
            auto_rx_config["always_decode"] = []

        try:
            auto_rx_config["experimental_decoders"]["MEISEI"] = config.getboolean(
                "advanced", "meisei_experimental"
            )
        except:
            logging.warning(
                "Config - Did not find meisei_experimental setting, using default (enabled)"
            )
            auto_rx_config["experimental_decoders"]["MEISEI"] = True

        try:
            auto_rx_config["save_system_log"] = config.getboolean(
                "logging", "save_system_log"
            )
            auto_rx_config["enable_debug_logging"] = config.getboolean(
                "logging", "enable_debug_logging"
            )
        except:
            logging.warning(
                "Config - Did not find system / debug logging options, using defaults (disabled, unless set as a command-line option.)"
            )

        # 1.6.2 - Encrypted Sonde Email Notifications
        try:
            auto_rx_config["email_encrypted_sonde_notifications"] = config.getboolean(
                "email", "encrypted_sonde_notifications"
            )
        except:
            logging.warning(
                "Config - Did not find encrypted_sonde_notifications setting (new in v1.6.2), using default (True)"
            )
            auto_rx_config["email_encrypted_sonde_notifications"] = True


        # 1.6.3 - Weathex WXR301d support
        try:
            auto_rx_config["wideband_sondes"] = config.getboolean(
                "advanced", "wideband_sondes"
            )
        except:
            logging.warning(
                "Config - Missing wideband_sondes option (new in v1.6.3), using default (False)"
            )
            auto_rx_config["wideband_sondes"] = False

        # 1.7.1 - Save RS41 Calibration Data
        try:
            auto_rx_config["save_cal_data"] = config.getboolean(
                "logging", "save_cal_data"
            )
        except:
            logging.warning(
                "Config - Missing save_cal_data option (new in v1.7.1), using default (False)"
            )
            auto_rx_config["save_cal_data"] = False

        # 1.7.5 - Azimuth-Only Rotator configuration
        try:
            auto_rx_config['rotator_azimuth_only'] = config.getboolean(
                "rotator", "azimuth_only"
            )
        except:
            logging.debug("Config - Missing rotator azimuth_only option (new in v1.7.5), using default (False)")
            auto_rx_config['rotator_azimuth_only'] = False

        # 1.7.5 - Targeted summary output
        try:
            auto_rx_config["ozi_host"] = config.get("oziplotter", "ozi_host")
            auto_rx_config["payload_summary_host"] = config.get("oziplotter", "payload_summary_host")
        except:
            logging.warning(
                "Config - Missing ozi_host or payload_summary_host option (new in v1.7.5), using default (<broadcast>)"
            )
            auto_rx_config["ozi_host"] = "<broadcast>"
            auto_rx_config["payload_summary_host"] = "<broadcast>"
            
        # If we are being called as part of a unit test, just return the config now.
        if no_sdr_test:
            return auto_rx_config

        # Now we enumerate our SDRs.
        auto_rx_config["sdr_settings"] = {}

        if auto_rx_config["sdr_type"] == "RTLSDR":
            # Multiple RTLSDRs in use - we need to read in each SDRs settings.
            for _n in range(1, auto_rx_config["sdr_quantity"] + 1):
                _section = "sdr_%d" % _n
                try:
                    _device_idx = config.get(_section, "device_idx")
                    _ppm = round(config.getfloat(_section, "ppm"))
                    _gain = config.getfloat(_section, "gain")
                    _bias = config.getboolean(_section, "bias")

                    if (auto_rx_config["sdr_quantity"] > 1) and (_device_idx == "0"):
                        logging.critical(
                            "Config - RTLSDR Device ID of 0 used with a multi-SDR configuration. Go read the warning in the config file!"
                        )
                        return None

                    # See if the SDR exists.
                    _sdr_valid = test_sdr(sdr_type = "RTLSDR", rtl_device_idx = _device_idx)
                    if _sdr_valid:
                        auto_rx_config["sdr_settings"][_device_idx] = {
                            "ppm": _ppm,
                            "gain": _gain,
                            "bias": _bias,
                            "in_use": False,
                            "task": None,
                        }
                        logging.info("Config - Tested RTLSDR #%s OK" % _device_idx)
                    else:
                        logging.warning("Config - RTLSDR #%s invalid." % _device_idx)
                except Exception as e:
                    logging.error(
                        "Config - Error parsing RTLSDR %d config - %s" % (_n, str(e))
                    )
                    continue

        elif auto_rx_config["sdr_type"] == "SpyServer":
            # Test access to the SpyServer
            _sdr_ok = test_sdr(
                sdr_type=auto_rx_config["sdr_type"],
                sdr_hostname=auto_rx_config["sdr_hostname"],
                sdr_port=auto_rx_config["sdr_port"],
                ss_iq_path=auto_rx_config["ss_iq_path"],
                ss_power_path=auto_rx_config["ss_power_path"],
                check_freq=1e6*(auto_rx_config["max_freq"]+auto_rx_config["min_freq"])/2.0,
                timeout=60
            )

            if not _sdr_ok:
                logging.critical(f"Config - Could not contact SpyServer {auto_rx_config['sdr_hostname']}:{auto_rx_config['sdr_port']}. Exiting.")
                return None

            for _n in range(1, auto_rx_config["sdr_quantity"] + 1):
                _sdr_name = f"SPY{_n:02d}"
                auto_rx_config["sdr_settings"][_sdr_name] = {
                    "ppm": 0,
                    "gain": 0,
                    "bias": 0,
                    "in_use": False,
                    "task": None,
                }

        elif auto_rx_config["sdr_type"] == "KA9Q":
            # Test access to the SpyServer
            _sdr_ok = test_sdr(
                sdr_type=auto_rx_config["sdr_type"],
                sdr_hostname=auto_rx_config["sdr_hostname"],
                sdr_port=auto_rx_config["sdr_port"],
                timeout=60
            )

            if not _sdr_ok:
                logging.critical(f"Config - Could not contact KA9Q Server {auto_rx_config['sdr_hostname']}:{auto_rx_config['sdr_port']}. Exiting.")
                return None

            for _n in range(1, auto_rx_config["sdr_quantity"] + 1):
                _sdr_name = f"KA9Q-{_n:02d}"
                auto_rx_config["sdr_settings"][_sdr_name] = {
                    "ppm": 0,
                    "gain": 0,
                    "bias": 0,
                    "in_use": False,
                    "task": None,
                }
            
        
        else:
            logging.critical(f"Config - Unknown SDR Type {auto_rx_config['sdr_type']} - exiting.")
            return None

        # Sanity checks when using more than one SDR
        if (len(auto_rx_config["sdr_settings"].keys()) > 1) and (
            auto_rx_config["aprs_object_id"] != "<id>"
        ):
            logging.critical(
                "Fixed APRS object ID used in a multi-SDR configuration. Go read the warnings in the config file!"
            )
            return None

        if (len(auto_rx_config["sdr_settings"].keys()) > 1) and (
            auto_rx_config["rotator_enabled"]
        ):
            logging.critical(
                "Rotator enabled in a multi-SDR configuration. Go read the warnings in the config file!"
            )
            return None

        # TODO: Revisit this limitation once the OziPlotter output sub-module is complete.
        if (len(auto_rx_config["sdr_settings"].keys()) > 1) and auto_rx_config[
            "ozi_enabled"
        ]:
            logging.critical("Oziplotter output enabled in a multi-SDR configuration.")
            return None

        if len(auto_rx_config["sdr_settings"].keys()) == 0:
            # We have no SDRs to use!!
            logging.error("Config - No working SDRs! Cannot run...")
            raise SystemError("No working SDRs!")
        else:
            # Create a global copy of the configuration file at this point
            global_config = copy.deepcopy(auto_rx_config)

            # Excise some sensitive parameters from the global config.
            global_config.pop("email_smtp_login")
            global_config.pop("email_smtp_password")
            global_config.pop("email_smtp_server")
            global_config.pop("email_smtp_port")
            global_config.pop("email_from")
            global_config.pop("email_to")
            global_config.pop("email_smtp_authentication")
            global_config.pop("sondehub_contact_email")
            global_config.pop("web_password")

            web_password = auto_rx_config["web_password"]

            return auto_rx_config
    except SystemError as e:
        raise e
    except:
        traceback.print_exc()
        logging.error("Could not parse config file.")
        return None


if __name__ == "__main__":
    """Quick test script to attempt to read in a config file."""
    import sys, pprint

    logging.basicConfig(
        format="%(asctime)s %(levelname)s:%(message)s", level=logging.DEBUG
    )

    config = read_auto_rx_config(sys.argv[1])

    pprint.pprint(global_config)
