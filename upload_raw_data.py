#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Emanuel Blei
Created: 2020
Licence: MIT Licence
Python version: 3.6
"""

# =============================================================================
# Import packages
# =============================================================================
import os
import sys
import getopt
from glob import glob
import datetime
import re
import numpy as np
import pandas as pd
import influxdb
from typing import Union, Callable, NamedTuple, List, Dict
import sqlite3
from itertools import compress
from io import StringIO
from configparser import ConfigParser, BasicInterpolation


# =============================================================================
# Set up configuration
# =============================================================================
working_dir = '/media/shared/CloudStation/test_bed/new_system/step_1_upload'
config = ConfigParser(interpolation=BasicInterpolation())
config.read(working_dir + '/local.cfg')

# Settings for InfluxDB connection
INFLUX_HOST = config.get('INFLUX', 'HOST')
INFLUX_PORT = config.getint('INFLUX', 'PORT')
INFLUX_USER_NAME = config.get('INFLUX', 'USER_NAME')
INFLUX_PASSWORD = config.get('INFLUX', 'PASSWORD')
INFLUX_DB_NAME = config.get('INFLUX', 'DB_NAME')

# current working directory
WORKING_DIRECTORY = config.get('LOCAL', 'WORKING_DIRECTORY')

SQLITE_LOCATION = config.get('SQLITE', 'PATH')

# Glob patterns used to find the raw data files
GLOB_PATTERNS = {
             'base': [config.get('GLOB', 'BASE_FILE_PATH'),
                      config.get('GLOB', 'BASE_FILE_PATTERN')],
             'branch': [config.get('GLOB', 'BRANCH_FILE_PATH'),
                        config.get('GLOB', 'BRANCH_FILE_PATTERN')],
             'calvin': [config.get('GLOB', 'CALVIN_FILE_PATH'),
                        config.get('GLOB', 'CALVIN_FILE_PATTERN')],
             'soil': [config.get('GLOB', 'SOIL_FILE_PATH'),
                      config.get('GLOB', 'SOIL_FILE_PATTERN')],
             'stem': [config.get('GLOB', 'STEM_FILE_PATH'),
                      config.get('GLOB', 'STEM_FILE_PATTERN')],
             }

# Set the number of files exported to InfluxDB in each iteration
CHUNK_SIZE = 10

# The dates from which onwards data should be read in.
START_DATES = {'base': '2020-05-12',
               'branch': '2020-05-30',
               'calvin': '202005',
               'soil': '2020-05-12',
               'stem': '2020-05-12'}

# List of raw data file types.
TYPE_LIST = ['base', 'branch', 'calvin', 'soil', 'stem']

# List of column names to pass to file that loads Calvin data files. The
# original file headers cannot be used as they are too long and have special
# symbols.
CALVIN_HEADER = ['date_time',
                 'flags',
                 'cylinder_id',
                 'intake',
                 'ddO2_value',
                 'ddO2_sd',
                 'O2_ppm_value',
                 'O2_ppm_sd',
                 'O2_perMeg_value',
                 'O2_perMeg_sd',
                 'dCO2_average',
                 'dCO2_sd',
                 'CO2_average',
                 'CO2_sd',
                 'O2_calib_date_time',
                 'CO2_calib_date_time',
                 'CO2_ZT_date_time']

# Names of tag columns that will be used for InfluxDB. Tag columns are stored
# as strings and not a numbers.Their main purpose is to filter out rows.
TAGS = {'base': ['Cylinder', 'CylAir', 'TopSwitch',
                 *map('SK{}'.format, range(1, 5)),
                 *map('BK{}'.format, range(1, 9)),
                 *map('AK{}'.format, range(1, 5)),
                 'Buffer'],
        'branch': None,
        'calvin': ['flags',
                   'cylinder_id',
                   'intake',
                   'O2_calib_date_time',
                   'CO2_calib_date_time',
                   'CO2_ZT_date_time'],
        'soil': None,
        'stem': None}


"""
In the following sections we only define the various support functions that
will be later used. To see the actual usage please go to the main function
where all the support functions are carried out.
"""
# =============================================================================
# # Define utility functions
# =============================================================================
"""
General purpose functions used for this project.
"""


def usage():
    """ This function prints a help message to terminal when the script
    file is called. """

    print('There are two possible arguments:')
    print('-h or --help will show this message.')
    print('Passing "-f" or "--flush" will reload all data from the start.')
    return


def todays_date() -> str:
    """ This function simply returns a formated string of today's
    date to be used for filenames."""
    today = datetime.date.today()
    today_string = today.strftime('%Y-%m-%d')

    return today_string


def flatten_list(nested_list: List) -> List:
    """
    This function takes a nested list and returns a flattend list without
    messing up strings.
    """
    flat_list = []
    for rec in nested_list:
        if isinstance(rec, list):
            flat_list.extend(rec)
            flat_list = flatten_list(flat_list)
        else:
            flat_list.append(rec)
    return flat_list


# =============================================================================
# File management
# =============================================================================
"""
These functions are responsible for finding files, checking the file names
against a regsiter or already processed files names and passing on the names
of any new files for further processing.
"""


def find_input_files(start_dates, glob_patterns):
    """ Search for a list of hardcoded file name patterns, load the names
    of the files found and then sort the containing dictionary keys and the
    dictioanry values alphabetically."""

    chamber_date_string = '20[1-2][0-9]-[0-1][0-9]-[0-3][0-9]'
    calvin_date_string = '20[1-2][0-9][0-1][0-9]'

    # Read in the lists of input file names.
    input_files = {k: glob(v[0] + calvin_date_string + v[1])
                   if k == 'calvin'
                   else glob(v[0] + chamber_date_string + v[1])
                   for k, v in glob_patterns.items()}

    # Generate a dictionary with the names of the starting files.
    start_dates = {k: (v[0] + start_dates[k] + v[1])
                   for k, v in glob_patterns.items()}

    # Compare the names of the input files with the starting dates to
    # filter out files generated before that date.
    input_files = {k: list(compress(v, [x >= start_dates[k] for x in v]))
                   for k, v in input_files.items()}

    # Sort dictionary elements by key names
    input_files = dict(sorted(input_files.items()))

    # Convert all lists to pandas series
    # Sort input file names alphabetically.
    input_files = {k: pd.Series(v, name='file_name', dtype='str').
                   sort_values() for
                   k, v in input_files.items()}

    return input_files


def remove_todays_file(series):
    """
    This function checks whether a list (of file names) is empty.
    If the list is not empty, it sorts the list elements in their natural
    order and then removes the last list element. It then also filters out
    any data file with today's date in its name.
    """

    if not series.empty:
        series = series.sort_values()
        output = series[:-1]
        pattern = re.compile(todays_date())
        output = output[~output.str.contains(pattern)]
    else:
        output = series

    return output


def add_names_to_old_files(db_location, input_files):
    """
    This function adds the names of the last files to the local
    sqlite database
    :param input_files: dictionary of series with file names
    :return:
    """
    # Open connection to local SQLite database
    local_db = sqlite3.connect(db_location)

    for k, v in input_files.items():
        if not v.empty:
            df = pd.DataFrame({'type': k, 'file_name': v})
            df.to_sql(name='old_files',
                      con=local_db,
                      if_exists='append',
                      index='false')
    # Close the database connection.
    local_db.close()


def flush_old_file_names(db_location):
    """
    This function wipes the local database of old file names.
    This can be useful when we need to reprocess the whole dataset.
    :return:
    """
    # Open connection to local SQLite database
    local_db = sqlite3.connect(db_location)

    # Create cursor for connection.
    cur = local_db.cursor()

    # Send drop command.
    cur.execute(('DROP TABLE IF EXISTS old_files;'))

    # Confirm the deletions. Without this the process will not actually
    # be acted upon by the database.
    local_db.commit()

    # Close the database connection.
    local_db.close()


def get_names_of_old_files(db_location, type_list):
    """ This function generates a dictionary containig the names of the files
    that contain the previously loaded files."""
    # Open connection to local SQLite database
    local_db = sqlite3.connect(db_location)

    # Test if the table actually exists (we might have deleted it)
    sqlite_tables = pd.read_sql_query(
           ('SELECT name FROM sqlite_master WHERE type =\'table\' AND name ' +
            'NOT LIKE \'sqlite_%\';'), local_db).iloc[:, 0]

    if not any(sqlite_tables.str.contains('old_files', regex=False)):
        return {k: pd.Series(dtype='str') for k in type_list}

    # Read actual data from database
    output = {x: pd.read_sql_query(('SELECT file_name FROM old_files WHERE ' +
                                   'type=:type ORDER BY file_name;'),
                                   local_db, params={'type': x})
              for x in type_list}
    # Close SQLite connection to unlock database
    local_db.close()

    # We need to ensure that a pandas series is returned rather than a
    # dataframe or a single string.
    output = {k: (v.iloc[:, 0] if not v.empty else pd.Series(dtype='str'))
              for k, v in output.items()}

    # Order list of files by name
    output = dict(sorted(output.items()))

    return output


def extract_new_files_only(all_files, old_files):
    """
    Compares two dictionaries of series containing files names
    and returns only files in "all_files" dictionary not contained
    within "old_files" dictionary".
    """
    new_files = {k: all_files[k][~all_files[k].isin(old_files[k])]
                 for k in all_files.keys()}

    return new_files

# =============================================================================
# File filtering
# =============================================================================
"""
    In order to remove some of the causes that will bring down the read-in
    mechanism I will have to remove any rows that contain garbled or
    false allocated data. To this end I will describe each file type
    (except for Calvin's output) by the number of columns and the type
    of column content for each of these columns.
    The column content type is defined separately.
    These two definitions together will hopefully help me to filter out
    any rows in the files where something has gone wrong.
"""

# %% Column description:


def check_for_NAs(func: Callable) -> Callable:
    """
    This decorator function checks whether the input string qualifies as an
    NA. If it does it will return True immediately. Otherwise it will run
    the function it decorates.
    """

    def inner(string: str, *args, **kwargs) -> bool:
        if re.fullmatch("^|0|NA$", string) is not None:
            return True
        else:
            return func(string, *args, **kwargs)

    return inner


def check_for_integer(func: Callable) -> Callable:
    """
    This decorator function checks whether or not a string can be transformed
    into a floating point numerical value. If not, it will return False
    immediately. Otherwise it will call the function it decorates so that
    it in turn can examine the input number further.
    """

    def inner(string: str, *args, **kwargs) -> bool:
        try:
            int_value = int(string)
        except ValueError:
            return False
        return func(int_value, *args, **kwargs)

    return inner


def check_for_float(func: Callable) -> Callable:
    """
    This decorator function checks whether or not a string can be transformed
    into a floating point numerical value. If not, it will return False
    immediately. Otherwise it will call the function it decorates so that
    it in turn can examine the input number further.
    """

    def inner(string: str, *args, **kwargs) -> bool:
        try:
            float_value = float(string)
        except ValueError:
            return False
        return func(float_value, *args, **kwargs)

    return inner


def int_checker_factory(min_value: int,
                        max_value: int) -> Callable:
    """
    Creates an output function that will check for a float being in a certain
    range.
    """
    @check_for_NAs
    @check_for_integer
    def inner(int_value: int) -> bool:
        if min_value <= int_value <= max_value:
            return True
        else:
            return False

    return inner


def float_checker_factory(min_value: float,
                          max_value: float) -> Callable:
    """
    Creates an output function that will check for a float being in a certain
    range.
    """
    @check_for_NAs
    @check_for_float
    def inner(float_value: float) -> bool:
        if min_value <= float_value <= max_value:
            return True
        else:
            return False

    return inner


def regex_checker_factory(regex_string: str) -> Callable:
    """
    Creates an output function that will check if an input value matches
    a regular expression. This function in turn will only return true if
    there is a full match between input string and pattern.
    """

    # Compile string to regex pattern.
    pattern = re.compile(regex_string)

    @check_for_NAs
    def inner(string: str) -> bool:
        if re.fullmatch(pattern, string) is None:
            return False
        else:
            return True

    return inner


column_dict = {
    # Licor Li-840 measures between 0 to 20,000 ppm CO2.
    'CO2_pattern': float_checker_factory(0, 20_000),

    # Galltec measures 0 to 100% humidity.
    'Galltec_hum_pattern': float_checker_factory(0, 100),

    # Galltec measures -30 to +70 degC.
    'Galltec_temp_pattern': float_checker_factory(-30, 70),

    # The H2O measurements range of the Licor Li-840 is
    # between 0 to 60 mmol/mol.
    'H2O_pattern': float_checker_factory(-2, 60),

    # Custom made PAR sensors measure between -1 to 2200 microEinstein. This
    # is my guess going by the data. The maximum I found was just over 2000.
    'PAR_pattern': float_checker_factory(-2, 2_200),

    # Rotronic sensor measures from 0 to 100 % humidity.
    'Rotronic_hum_pattern': float_checker_factory(0, 105),

    # Rotronic sensor measures from 0 to 50 degC.
    'Rotronic_temp_pattern': float_checker_factory(0, 50),

    # I am not sure what values we should use here.
    'air_temp_pattern':  float_checker_factory(-20, 60),

    # Barometer measures from 800 to 1060 hPa
    'barometer_pattern': float_checker_factory(800, 1060),

    # Flows for branch chambers go from 0 to 15 SLM.
    'branch_flow_pattern': float_checker_factory(-0.1, 15.1),

    # All values start with the year and what I guess is the quarter
    # plus the cylinder marking.
    'cylinder_id_pattern':  regex_checker_factory(("^20[0-9]{3,4}[A-Z]{1,3}"
                                                   "[0-9]{3,6}$|^Buffer$")),

    # Flow through cylinder should be between 0 to 8 SLM.
    'cylinder_flow_pattern': float_checker_factory(-0.1, 8.1),

    # Dates are in ISO pattern to the nearest full second.
    'date_pattern': regex_checker_factory(
            ("^20[0-9]{2}-[0-1]{1}[0-9]{1}-[0-3]{1}[0-9]{1} [0-2]{1}[0-9]{1}"
             ":[0-5]{1}[0-9]{1}:[0-5]{1}[0-9]{1}$")),

    # The Sensirion ASP1400 measures  differential pressures between -100
    # to +100 Pa
    'diff_press_pattern': float_checker_factory(-1, 1),

    # The Sensirion ASP1400 measures temperatures between 0 to 70 degC.
    'diff_press_temp_pattern': float_checker_factory(0, 70),

    # Bio Instruments LT-4M measures from 0 to 50 degC.
    'leaf_temp_pattern': float_checker_factory(0, 50),

    # Main flow to analysers goes from 0 to 1 SLM.
    'main_flow_pattern': float_checker_factory(-0.1, 2.1),

    # Flows for soil and stem chambers go from 0 to 4 SLM.
    'soil_flow_pattern': float_checker_factory(-0.1, 4.1),

    # Stevens Hydra 2 is assumed to measure soil moisture from 0 to 100%.
    'soil_moisture_pattern': float_checker_factory(0, 1),

    # Stevens Hydra 2 measures soil temperature from -30 to +65 degC.
    'soil_temp_pattern': float_checker_factory(-30, 65),

    # Stevens Hydra 2 measures soil temperature from -30 to +65 degC.
    'stem_CO2_pattern': float_checker_factory(-0.1, 20.1),

    # Switches show 0 for OFF and 1 for ON.
    'switch_pattern': regex_checker_factory("^0|1$")
}


# %% File descriptions
class FileType(NamedTuple):
    """
    Definition for the structure of file types exported by the Oxyflux system.
    """
    file_type: str
    column_no: int
    data_types: List[Callable]
    header: str

    def check_consistency(self) -> bool:
        return len(self.data_types) == self.column_no


file_types = {
    'base': FileType(file_type='base',
                     column_no=41,
                     data_types=flatten_list(
                          [column_dict['date_pattern'],
                           column_dict['cylinder_id_pattern'],
                           column_dict['CO2_pattern'],
                           column_dict['H2O_pattern'],
                           [column_dict['switch_pattern'] for _ in range(19)],
                           column_dict['main_flow_pattern'],
                           [column_dict['branch_flow_pattern']
                           for _ in range(4)],
                           [column_dict['soil_flow_pattern']
                           for _ in range(12)],
                           column_dict['cylinder_flow_pattern']]),
                     header=("Date_Time,Cylinder,CO2,H2O,CylAir,TopSwitch,"
                             "SK1,SK2,SK3,SK4,BK1,BK2,BK3,BK4,BK5,BK6,BK7,"
                             "BK8,AK1,AK2,AK3,AK4,Buffer,FlowSLM_Main,"
                             "FlowSLM_AK1,FlowSLM_AK2,FlowSLM_AK3,FlowSLM_AK4,"
                             "FlowSLM_BK1,FlowSLM_BK2,FlowSLM_BK3,FlowSLM_BK4,"
                             "FlowSLM_BK5,FlowSLM_BK6,FlowSLM_BK7,FlowSLM_BK8,"
                             "FlowSLM_SK1,FlowSLM_SK2,FlowSLM_SK3,FlowSLM_SK4,"
                             "FlowSLM_CylAir")),

    'branch': FileType(file_type='branch',
                       column_no=20,
                       data_types=flatten_list(
                          [column_dict['date_pattern'],
                           column_dict['barometer_pattern'],
                           [[column_dict['Rotronic_hum_pattern'],
                             column_dict['Rotronic_temp_pattern']]
                            for _ in range(5)],
                           [column_dict['PAR_pattern'] for _ in range(4)],
                           [column_dict['leaf_temp_pattern']
                           for _ in range(4)]]),
                       header=("Datum_Zeit_GMT,Ground_press_hPa,Hum_Ak1,"
                               "Temp_Ak1,Hum_Ak2,Temp_Ak2,Hum_Ak3,Temp_Ak3,"
                               "Hum_Ak4,Temp_Ak4,Hum_Buffer,Temp_Buffer,"
                               "PAR_Ak1,PAR_Ak2,PAR_Ak3,PAR_Ak4,LeafTemp_Ak1,"
                               "LeafTemp_Ak2,LeafTemp_Ak3,LeafTemp_Ak4")),

    'soil': FileType(file_type='soil',
                     column_no=42,
                     data_types=flatten_list(
                      [column_dict['date_pattern'],
                       column_dict['barometer_pattern'],
                       [column_dict['diff_press_pattern'] for _ in range(8)],
                       [column_dict['diff_press_temp_pattern']
                       for _ in range(8)],
                       [column_dict['air_temp_pattern'] for _ in range(8)],
                       [column_dict['soil_temp_pattern'] for _ in range(8)],
                       [column_dict['soil_moisture_pattern']
                       for _ in range(8)]]),
                     header=",".join([
                             "DatumZeit_GMT", "Ground_press_hPa",
                             *map("Press_Bk{}".format, range(1, 9)),
                             *map("PressTmp_Bk{}".format, range(1, 9)),
                             *map("Temp_Bk{}".format, range(1, 9)),
                             *map("SoilTemp_Bk{}".format, range(1, 9)),
                             *map("Moisture_Bk{}".format, range(1, 9))])),

    'stem': FileType(file_type='stem',
                     column_no=14,
                     data_types=flatten_list(
                      [column_dict['date_pattern'],
                       column_dict['barometer_pattern'],
                       [column_dict['stem_CO2_pattern'] for _ in range(4)],
                       [column_dict['Galltec_temp_pattern'] for _ in range(4)],
                       [column_dict['Galltec_hum_pattern'] for _ in range(4)]
                       ]),
                     header=("Datum_Zeit_GMT,Ground_press_hPa,CO2_Sk1,CO2_Sk2,"
                             "CO2_Sk3,CO2_Sk4,Temp_Sk1,Temp_Sk2,Temp_Sk3,"
                             "Temp_Sk4,Hum_Sk1,Hum_Sk2,Hum_Sk3,Hum_Sk4"))
}


def make_it_fit(cell_list: List[str], limit: int) -> Union[None, List[str]]:
    """
    This function attempts to shorten a list of strings by finding and
    elimininating empty string elements from left to rigth. If succesfull
    it will return the modified list. Otherwise it will return None.
    """

    if len(cell_list) <= limit:
        return cell_list
    else:
        while sum([len(x) == 0 for x in cell_list]):
            cell_list.remove("")
            if len(cell_list) == limit:
                return cell_list
        else:
            return None


def move_string_to_the_end(row: List[str], string: str) -> List[str]:
    """This function simply shuffles any blank strings to the
    end of the list. The number of list elements does not change,
    only their position.
    """

    length_before = len(row)
    row = [x for x in row if x != string]
    length_after = len(row)
    appendix = [string for _ in range(length_before - length_after)]

    return row + appendix


def correct_column_position(row: List[str],
                            file_type: FileType) -> List[str]:
    """
    This checks the file type. If the file type matches, it
    will adjust the column position for time periods where we
    know that there was a mix-up. It then returns the corrected
    list of strings
    """

    if file_type.file_type != 'soil':
        return row

    # Here we put the variables we will need:
    time_period = ("2020-05-12 00:00:00", "2020-06-30 12:16:50")
    time_period = pd.to_datetime(time_period,
                                 format='%Y-%m-%d %H:%M:%S',
                                 utc=True)

    timestamp = pd.to_datetime(row[0],
                               format='%Y-%m-%d %H:%M:%S',
                               utc=True)

    if time_period[0] <= timestamp < time_period[1]:
        return row[0: 2] + ["NA" for _ in range(16)] + row[2:26]
    else:
        return row


def row_processor(row: str,
                  file_type: FileType) -> Union[None, str]:
    """
    This function uses the previously defined patterns and matches them
    against each row in a file. It then either returns the entire row or just
    None.
    """

    # Split row into individual cells along commas.
    cells = re.split(pattern=",|;", string=row)
    # Test if number of cells is greater than the expected number of columns.
    # If so, chuck it out.
    if len(cells) > file_type.column_no:
        cells = make_it_fit(cells, file_type.column_no)
    if cells is None:
        return cells

    # Move blank spaces to the back
    cells = move_string_to_the_end(cells, "")

    # Test if number of cells is smaller than the expected number of columns.
    # If so, pad it with the appropriate number of NAs
    if 1 <= len(cells) < file_type.column_no:
        if column_dict['date_pattern'](cells[0]):
            diff = file_type.column_no - len(cells)
            cells += ["" for _ in range(diff)]
        else:
            return None

    # Sort out mixed-up columns:
    cells = correct_column_position(cells, file_type)

    # Check if the cell content conforms with expectations.
    if not all([f(text) for f, text in zip(file_type.data_types, cells)]):
        return None
    else:
        return ",".join(cells)


# %% Input filter function
def filter_input(file_name: str,
                 file_type: FileType) -> str:
    """
    This function sits between the input files and the usual csv reader.
    The data are first read into this function, which will remove any rows
    which fail any of these three tests:
    1) Number of columns match expected value.
    2) The variable type for each column matches with expectation.
    3) Where applicable the values match the expected value range.
    The function will also remove the header as that should not fit the
    pattern either. However, the headers themselves have been incorrect on
    some occasions. So I figured it is best to automatically replace them
    on my own anyway. The function adds a predefined header to each file.
    """
    with open(file_name, 'r') as input_file:
        # Read in the whole file as string.
        data = input_file.read()
        # Split data into list of strings along the line breaks. Remove header.
        row_list = re.split(pattern='[\n]', string=data)[1:]
        # Calculate number of row elements.
        prefilter_length = len(row_list)
        # Process the row either returning it or None.
        processed_list = [row_processor(row, file_type) for row in row_list]
        # Calculate number of row elements that are not None.
        postfilter_length = sum([x is not None for x in processed_list])
        # Join the rows back together into one virtual csv-file.
        filtered_string = "\n".join(filter(None, processed_list))
        # Add header to the top of the file.
        output = "\n".join([file_type.header, filtered_string])

        return output, prefilter_length, postfilter_length


def find_correct_file_type(file_type: Dict[str, FileType]) -> Callable:
    """
    This function takes a dictionary with file type definitions and returns
    a function that will take a file name to match up with a file type.
    """

    def inner(file_name: str) -> FileType:
        pattern = re.compile((r"20[0-9]{2}-[0-1][0-9]-[0-3][0-9]_(?P<ext>.*)"
                              "\.txt$"))
        file_extension = re.search(pattern, file_name).group("ext")

        lookup = {'Data': 'base',
                  'BranchChamber': 'branch',
                  'SoilChamber': 'soil',
                  'StemChamber': 'stem'}

        file_type = file_types[lookup[file_extension]]

        return file_type

    return inner


# Make curried function
get_file_type = find_correct_file_type(file_types)


def load_chamber_file(file_name: str) -> pd.DataFrame:
    """ Load chamber files. This includes removing any rows with duplicate
    timestamps and enforcing 10 second time-steps between each time-stamp."""

    # Find the file_type from the file_name
    file_type = get_file_type(file_name)

    # Read and filter chamber file.
    input_string, unfiltered, filtered = filter_input(file_name, file_type)

    # Print file name and number of rows to terminal
    print((f"Loaded {file_name}:\nStarting with {unfiltered} rows. After "
           f"filtering only {filtered} rows remain."))

    # Read the text string into pandas as if it were a file.
    df = pd.read_csv(StringIO(input_string),
                     na_values=['', 'NA', '-999'],
                     skip_blank_lines=True)

    # Fix funky column names
    df.columns = df.columns.str.strip().str.replace(' ', '_')

    # Remove rows with only NAs
    df = df.dropna(how='all')

    # Ensure a uniform data-time column name.
    time_col_names = ["Date_Time", "DatumZeit_GMT", "Datum_Zeit_GMT", "Time"]
    time_col = [col for col in df.columns if col in time_col_names][0]
    df = df.rename(columns={time_col: 'date_time'})

    # Remove any data with duplicated timestamps (only the last occurring
    # element remains).
    df.drop_duplicates(subset=['date_time'], keep='last', inplace=True)

    # Make index object
    df['date_time'] = pd.to_datetime(df['date_time'],
                                     format='%Y-%m-%d %H:%M:%S',
                                     utc=True)

    # Set new index and ensure that base and chamber data use the
    # exact same time step. This is necessary as there can be a one
    # second discrepancy.
    df = df.set_index('date_time')
    df.index = df.index.floor(freq='10S')

    if filtered != len(df.index):
        print((f"Dataframe only has {len(df.index)} rows!"))

    return df


def clean_base_data(df: pd.DataFrame, tag_columns: List[str]) -> pd.DataFrame:
    """
    Notes:
    Empty Cylinder rows are already NAs.
    Change zeros for CO2 and H2O to NAs.
    """

    df = df.copy(deep=True)
    df.loc[df['CO2'] == 0, 'CO2'] = np.nan
    df.loc[df['H2O'] == 0, 'H2O'] = np.nan
    tags = df.loc[:, tag_columns]
    tags = tags == 1
    tags = tags.astype(int)
    df.loc[:, tag_columns] = tags

    return df


def correct_cylinder_markers(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[:, 'CylAir'] = 1
    df.loc[:, ['BK2', 'BK4']] = 0

    return df


def find_and_correct_cylinder_activity_marker(
        df: pd.DataFrame) -> pd.DataFrame:
    """
    Find any rows where soil chamber 2 (BK2) and soil chamber 4 (BK4) were
    marked as active (1) when in fact these were used in lieu for the cylinder
    (CylAir) between July 24th 2020 and August 25th 2020 and correct the
    activity markers so that the chambers appear as off (0) and the cylinder
    as on (1).
    """
    # First find the rows that need correction
    time_filter_1 = df.index > pd.Timestamp('2020-07-24', tz='UTC')
    time_filter_2 = df.index < pd.Timestamp('2020-08-25', tz='UTC')
    BK2_or_BK4_active = np.logical_or(df['BK2'] == 1, df['BK4'] == 1)

    # Combine the filters
    rows_with_wrong_marker = np.logical_and.reduce(
            (time_filter_1, time_filter_2, BK2_or_BK4_active))

    df.loc[rows_with_wrong_marker, :] = correct_cylinder_markers(
            df.loc[rows_with_wrong_marker, :])

    return df


def robust_date_converter(input_string: str) -> pd.Timestamp:
    """ This function checks the date-time format for Calvin's timestamp
    strings and then converts them to date-time format with the right
    arguments."""
    # First pattern matches "01Jan20 11:59:59 PM"
    pattern_1 = re.compile(r'^\d{2}[A-Z]{1}[a-z]{2}\d{2} '
                           r'\d{1,2}:\d{2}:\d{2} (AM|PM){1}$')
    # Second pattern matches "01/01/2020 23:59"
    pattern_2 = re.compile(r'^\d{2}/\d{2}/20\d{2} \d{2}:\d{2}$')

    if re.search(pattern=pattern_1, string=input_string):
        output = pd.to_datetime(input_string, format='%d%b%y %I:%M:%S %p')
    elif re.search(pattern=pattern_2, string=input_string):
        output = pd.to_datetime(input_string + ':45',
                                format='%d/%m/%Y %H:%M:%S')
    else:
        output = pd.to_datetime(input_string, format='%d%b%y %H:%M:%S')

    return output


def load_calvin_file(file_name: str, header: List[str]) -> pd.DataFrame:
    """ Load Calvin files. We then remove all unnecessary columns,
    convert the date-time columns to date_time format and add 15 seconds."""

    df = pd.read_csv(file_name,
                     sep=',',
                     header=None,
                     skiprows=2,
                     names=header,
                     na_values=['', 'NaN'],
                     skip_blank_lines=True)

    # Format Calvin's time column to POSIXct and add 15 seconds to Calvin's
    # timestamps so that the it coincides with the centre of the sampling
    # periods. This is because the 2 minute timestamp is actually the time
    # of the start of one 2-minute sampling period delayed by 45 seconds.
    df['date_time'] = df['date_time'].apply(robust_date_converter)

    df.loc[:, 'date_time'] = df.loc[:, 'date_time'] + pd.Timedelta(seconds=15)

    df.set_index('date_time', inplace=True)

    # Remove any rows that contain missing numeric data
    # Without this the ingestion into InfluxDB fails.
    # I have not found any other workaround for that.
    df.dropna(axis=0, how='any', inplace=True)

    return df


def chop(lst, n):
    """ This function returns a list of lists derived from a list that was
    chopped up into pieces with n elements each. The last element might be
    any length between 1 and n."""
    return [lst[i:i+n] for i in range(0, len(lst), n)]


def load_data(file_name_dict: Dict, base_tags: List[str]) -> Callable:
    """This file calls most of the previously defined functions to load
    the data inside the files into python."""

    def inner_func(data_type):
        """ The if statement is there to prevent errors when there is no more
        data to load. Otherwise the script loads the list of dataframes and
        merges them into one, make conversions of numerical columns and
        removes columns which are marked 'empty'."""

        if not file_name_dict[data_type].empty:
            lst = list(map(lambda x: load_chamber_file(x),
                           file_name_dict[data_type]))
            df = pd.concat(lst)
            if data_type == 'base':
                df = clean_base_data(df, base_tags)
                df = find_and_correct_cylinder_activity_marker(df)
            print(f"Merged {data_type} dataframe has {len(df.index)} rows.")
        else:
            df = pd.DataFrame()
            print("Empty dataframe.")

        return df

    return inner_func


# =============================================================================
# %% Main function
# =============================================================================
def main(safety_on=False):

    # Set the working directory.
    os.chdir(WORKING_DIRECTORY)

    # Check for options passed to script
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hf", ["help", "flush"])
    except getopt.GetoptError as err:
        # print help information and exit:
        print(err)  # will print something like "option -a not recognized"
        usage()
        sys.exit(2)

    flush = False
    for o, a in opts:
        if o in ('-h', '--help'):
            usage()
            sys.exit()
        elif o in ('-f', '--flush'):
            flush = True
        else:
            assert False, 'unhandled option'
            usage()
            sys.exit()

    print("\n")
    print("Oh hello there! Let's see if there are new files.")

    # Open connection to InfluxDB
    influxdb_client = influxdb.DataFrameClient(
        host=INFLUX_HOST,
        port=INFLUX_PORT,
        username=INFLUX_USER_NAME,
        password=INFLUX_PASSWORD,
        database=INFLUX_DB_NAME)

    # Check if old files should be flushed.
    if flush and not safety_on:
        flush_old_file_names(SQLITE_LOCATION)
        flush = False
        print("I will flush the old file register first.")

    # Make a dictionary that contains list of the different file types
    old_files = get_names_of_old_files(SQLITE_LOCATION, TYPE_LIST)

    # Load names of all available input files
    input_files = find_input_files(START_DATES, GLOB_PATTERNS)

    input_files = {k: (remove_todays_file(v) if k != 'calvin' else v)
                   for k, v in input_files.items()}

    # Extract new files
    new_files = extract_new_files_only(input_files, old_files)

    # Only load the first x elements of each file list.
    # Remove the other names from the new_file dictionaries.
    export = {}

    for i in new_files.keys():
        export[i] = new_files[i][0:CHUNK_SIZE].copy()
        new_files[i] = new_files[i][CHUNK_SIZE:]

    # Declare curried function:
    load_chamber_data = load_data(export, TAGS['base'])
    # This is the step where we load the actual data into Python.
    raw_data = {i: load_chamber_data(i) for i in new_files.keys()
                if i != 'calvin'}

    # Make Calvin dataframe
    raw_data['calvin'] = list(map(lambda x: load_calvin_file(x,
                              CALVIN_HEADER), export['calvin']))
    raw_data['calvin'] = pd.concat(raw_data['calvin'])

    # Sort dictionary elements by key names
    raw_data = dict(sorted(raw_data.items()))

    # Write chamber data to InfluxDB.
    for i in raw_data.keys():
        if i not in 'calvin':
            if not len(raw_data[i].index) == 0:
                influxdb_client.write_points(dataframe=raw_data[i],
                                             measurement=('raw_'+i),
                                             tag_columns=None,
                                             batch_size=1000,
                                             protocol='line')
        else:  # If there is data from Calvin write to InfluxDB.
            if not len(raw_data['calvin'].index) == 0:
                influxdb_client.write_points(dataframe=raw_data['calvin'],
                                             measurement='raw_calvin',
                                             tag_columns=TAGS['calvin'],
                                             batch_size=1000,
                                             protocol='line')

    # Remove last item from new_files['calvin']
    if len(new_files['calvin']) == 0:
        export['calvin'] = export['calvin'][:-1]

    # Write the names of the files that were loaded into InfluxDB to the
    # local database.
    add_names_to_old_files(SQLITE_LOCATION, export)

    # Run function recursively if there still elements in the new file list
    if any([not x.empty for x in new_files.values()]):
        main(safety_on=True)

    return


# =============================================================================
# Run if file is called as script from terminal
# =============================================================================
if __name__ == '__main__':
    main()
