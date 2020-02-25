#! /bin/bash

runUPI() {
  datadir=$(/usr/bin/realpath $1)
  cmd=$2
  archive=$3
  host=$4
  source=$(/usr/bin/basename $datadir)
  #
  if [[ ! -d $datadir ]]; then
    return 0
  fi
  /bin/mkdir -p $5
  if [ $? -ne 0 ]; then
    return 125
  fi

  NOW=$SECONDS
  /usr/bin/upifinder $cmd -c $datadir > $5/$host-upi-$cmd-$archive-$source.lst
  if [ $? -ne 0 ]; then
    exit 1
  fi
  elapsed=$((SECONDS-NOW))
  /bin/echo "$elapsed seconds - $(/usr/bin/du -sh $datadir)"
}

HOST=$(/bin/hostname)
ARCDIR=$PWD
DATDIR=$PWD
WHERE="main"
JOBS=8

while getopts :a:n:d:h:j: OPT; do
  case $OPT in
    j)
      JOBS=$OPTARG
      ;;
    h)
      HOST=$OPTARG
      ;;
    n)
      WHERE=$OPTARG
      ;;
    a)
      if [[ ! -d $OPTARG ]]; then
        /bin/echo "$OPTARG: not a directory"
        exit 124
      fi
      ARCDIR=$OPTARG
      ;;
    d)
      DATDIR=$OPTARG
      /bin/mkdir -p $DATDIR
      if [ $? -ne 0 ]; then
        /bin/echo "fail to create directory $DATDIR"
        exit 124
      else
        if [[ ! -d $DATDIR ]] || [[ ! -w $DATDIR ]]; then
          /bin/echo "$DATDIR: is not a writable directory"
          exit 124
        fi
      fi
      ;;
    *)
      /bin/echo "$(/usr/bin/basename $0) traverse the full archive to run upifinder on each source in parallel"
      /bin/echo ""
      /bin/echo "options:"
      /bin/echo ""
      /bin/echo "-n NAME  archive name (default: $WHERE)"
      /bin/echo "-a DIR   archive directory (default: $ARCDIR)"
      /bin/echo "-d DIR   data directory (default: $DATDIR)"
      /bin/echo "-h HOST  hostname (default: $HOST)"
      /bin/echo "-j JOBS  number of parallel jobs (default: $JOBS)"
      /bin/echo ""
      /bin/echo "example:"
      /bin/echo ""
      /bin/echo "$ $(/usr/bin/basename $0) -n share -a /archives -d /var/www/html -h testme {39..41} 51"
      /bin/echo ""
      /bin/echo "usage: $(/usr/bin/basename $0) [-j obs] [-a arcdir] [-n name] [-d datadir] [-h host] <source...>"
      exit 1
      ;;
  esac
done
shift $(($OPTIND - 1))

if [[ ! -x /usr/bin/upifinder ]]; then
  /bin/echo "upifinder is not an executable"
  exit 123
fi

if [[ ! -x /usr/bin/parallel ]]; then
  /bin/echo "parallel is not an executable"
  exit 123
fi

export -f runUPI
/usr/bin/parallel --will-cite -j $JOBS runUPI $ARCDIR/{1} {2} $WHERE $HOST $DATDIR ::: $@ ::: walk check
if [ $? -ne 0 ]; then
  /bin/echo "unexpected error when running upifinder"
  exit 125
fi
