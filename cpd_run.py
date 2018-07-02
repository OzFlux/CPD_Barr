import csv
import datetime
import glob
import numpy
import os
import pandas
import time

import QCCPD

def get_rows_to_skip(file_name, token="TIMESTAMP_START"):
    # find the row containing the string TIMESTAMP_START, this will be the number
    # of header lines to skip
    with open(file_name, 'rb') as f:
        reader = csv.reader(f, delimiter=',')
        for n, row in enumerate(reader):
            if token in row:
                break
    return n

df_list = []
file_dir = "/home/peter/OzFlux/Respiration/Barr/data/*.csv"
file_name_list = sorted(glob.glob(file_dir))
for file_name in file_name_list:
    print file_name
    # read the CSV file into a dataframe
    skiprows = get_rows_to_skip(file_name)
    df = pandas.read_csv(file_name, skiprows=skiprows, na_values=-9999)
    dt = [datetime.datetime.strptime(str(ldt), "%Y%m%d%H%M") for ldt in df["TIMESTAMP_END"]]
    df.index = dt
    df.rename(columns={"NEE":"Fc", "TA":"Ta", "USTAR":"ustar", "SW_IN":"Fsd"}, inplace=True)
    df.drop(["TIMESTAMP_START", "TIMESTAMP_END"], axis=1, inplace=True)
    df_list.append(df)

df = pandas.concat(df_list)

# get CPD from observational run only
#usth = QCCPD.change_point_detect(df, resample=False, insolation_threshold=5, season_routine='barr')
#result = usth.get_change_points()
#print result

# number of bootstraps
n_trials = 1
if n_trials == 1:
    resample = False
else:
    resample = True
keep_trial_results = True
path, name = os.path.split(file_name)
parts = name.split("_")
base_name = parts[0] + "_CPD_Results_" + str(n_trials) + ".xls"
xl_name = os.path.join(path, base_name)

t0 = time.time()
usth = QCCPD.change_point_detect(df, insolation_threshold=5, resample=resample, season_routine='barr')
result = usth.get_change_points(n_trials = n_trials, keep_trial_results = keep_trial_results)
xl_writer = pandas.ExcelWriter(xl_name)
result["summary_statistics"].to_excel(xl_writer, sheet_name="Annual")
result["trials_summary"].to_excel(xl_writer, sheet_name="Trials summary")
if keep_trial_results:
    result["trial_results"].to_excel(xl_writer, sheet_name="Trials results")
xl_writer.save()
t1 = time.time()
print "Time taken ", t1-t0