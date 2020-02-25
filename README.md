upifinder contains sub command that allows operators to check the consistency of the [hadock](https://github.com/busoc/hadock) archive.

Its has three sub commands to:

1. report the number of files (total number and uniq files)
2. report the gaps in the archive
3. report basic information about files available in the hadock archive

upifinder can read files from the different locations that are supported by hadock:

* the filesystem
* tar archive
* lst files. Even if this kind of files is not created by hadock, this kind of files is supposed to be a list of files generated with, eg, the find command

## upifinder walk

The walk sub command provides the amount of files available in the hadock archive. It gives the following count per UPI:

* total number of files
* total number of uniq files
* total number of invalid files

```
$ upifinder walk [options] <archive,...>

where options are:

  -u UPI     only count files for the given UPI
  -s START   only count files created after START
  -e END     only count files created before END
  -d DAYS    only count files created during a period of DAYS
  -c         print the results as csv
  -z         discard UPI that have no missing files
  -h         show the help message and exit
```
Examples:
```
#count files for all UPI on the last seven days for all sources:
$ upifinder walk -d 7 /data/images/playback/*

#count files between two dates for a specific UPI:
$ upifinder walk -u XYZ -s 2018-06-04 -e 2018-06-11 /data/images/realtime/*

#count files at a date for a specific UPI:
$ upifinder walk -u XYZ -s 2018-06-04 -e 2018-06-11 /data/images/realtime/38/2018/175/10

#count files between two dates for a specific UPI and a specific source:
$ upifinder walk -u XYZ -s 2018-06-04 -e 2018-06-11 /data/images/playback/38
```

the columns of the output (whatever if -c option is set) are:

| column | description |
| ---    | ---         |
| UPI    | source and UPI |
| total  | total number of files |
| uniq   | total number of uniq files |
| size   | total size for all the files |
| invalid | number of invalid files found |
| ratio   | ratio between the total number of files and the number of invalid files |
| acq start | timestamp of the first file |
| acq end   | timestamp of the last file |
| seq start | sequence counter of the first file |
| seq end   | sequence counter of the last file |
| missing   | number of missing sequence counter |

## upifinder check-upi
The check-upi sub command provides the number of missing files in the hadock archive either by source or by UPI. Its output
gives one gap per pair source/UPI

```
$ upifinder (check-upi|check) [options] <archive,...>

where options are:

  -b BY      check gaps by upi or by source (default by upi)
  -u UPI     only count files for the given UPI
  -s START   only count files created after START
  -e END     only count files created before END
  -d DAYS    only count files created during a period of DAYS
  -i TIME    only consider gap with at least TIME duration
  -c         print the results as csv
  -a         keep all gaps even when a later playback/replay refill those
  -k         keep invalid files in the count of gaps
  -g         print the ACQTIME as seconds elapsed since GPS epoch
  -h         show the help message and exit
```
Examples:
```
$ upifinder check -d 7 /data/images/playback/*
```
the columns of the output (whatever if -c option is set) are:

| column | description |
| ---    | ---         |
| UPI    | source and UPI |
| acq start | timestamp of last file before gap |
| acq end   | timestamp of first file after gap |
| duration  | duration of the gap |
| seq start | sequence counter of last file before gap |
| seq end   | sequence counter of first file after gap |
| missing   | number of missing files |


## upifinder digest

Initially, the digest sub command only computes a checksum for each files found in the archive. However, the current implementation also gives other informations about the files and the data they contain

```
$ upifinder (digest|sum|cksum) [options] <archive,...>

where options are:

 -c  print the results as csv
 -h  show the help message and exit
```
the columns of the output (whatever if -c option is set) are:
| column | description |
| ---    | ---         |
| format | the data type of a file |
| seq    | sequence counter of a file |
| acqtime | acquisition time of the data of a file |
| digest  | checksum (xxhash of a file) |
| filename | filename |
