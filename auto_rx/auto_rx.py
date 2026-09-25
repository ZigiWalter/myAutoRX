    # Payload Summary output (Chasemapper)
    if config["payload_summary_enabled"]:
        if config["payload_summary_host"]:
            _summary_host = config["payload_summary_host"]
        else:
            _summary_host = None

        if config["payload_summary_enabled"]: # Causes port to be set to None which disables the export.
            _summary_port = config["payload_summary_port"]
        else:
            _summary_port = None

        _ozimux = OziUploader(
            payload_summary_host=_summary_host,
            payload_summary_port=_summary_port,
            payload_summary_update_rate=config["payload_summary_update_rate"],
            station=config["habitat_uploader_callsign"],
        )

        exporter_objects.append(_ozimux)
        exporter_functions.append(_ozimux.add)
