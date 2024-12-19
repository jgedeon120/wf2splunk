# wf2splunk

Weatherflow data to Splunk
This script will sniff for data coming from the weatherflow system over udp and then send the data to Splunk via HEC.

## Requirements

Python 3.8 or newer should work. The following python modules/libraries are also needed.

- configparser
- os
- sys
- time
- datetime
- json
- socket
- queue
- threading
- requests
- urllib3
- signal

## How to run

Configure the weatherflow.config file for your settings.

Once the config file has been configured you can then run the script either in you console or in a screen console, of configure your script to run at startup. This can be done via various methods depending on system.
