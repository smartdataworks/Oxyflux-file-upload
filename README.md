# Oxyflux file-upload

This repositiory contains the files necessary to upload the chamber sensor and gas analyser data from a local machine to a local or remote InfluxDB server.

## Options
The script can be run from terminal simply by calling `python3 upload-raw-data.py`. It can also be called through a cron job to enable automatic uploads at regular intervals.

Running the script with no further options selected will result in the script finding any new data files in the source folders and then parsing uploading the files to the InfluxDB server.

Passing the '-h' or '--help' option will print a brief description of posibble optional arguments to terminal before the script exits gracefully. The same happens if an unknown option is passed on to the script.

Passing either the option '-f' or '--flush' will result in the script re-processing and re-uploading all files to InfluxDB. However, the script will not delete the old data from the InfluxDB database as this should be done by the user with elevated privileges. Instead any new data with the same timestamp as the old data will overwrite the old data with that same timestamp.

## Examples

Run script normally (upload newest data only):  

```bash
python3 upload_raw_data.py
```

Run script to see possible options:

```bash
python3 upload_raw_data.py -h
```

 or

 ```bash
python3 upload_raw_data.py --help
```

Run script reprocess all data again:

```bash
python3 upload_raw_data.py -f
```

 or

 ```bash
python3 upload_raw_data.py --flush
```

## Compatability

The script was written for Python 3.6.8 and does not contain any code or packages that are known to have breaking changes coming soon (as of the time of this writing).

## Configuration

Configuration is done in a local cfg-file. This way folder structures, database access and--most importantly--passwords are not hardcoded into the script. An empty cfg-config file is supplied with the repository.
