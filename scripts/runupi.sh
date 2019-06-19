#! /bin/bash

function runUPI {
  datadir=$(realpath $1)
  cmd=$2
  archive=$3
  host=$4
  source=$(basename $datadir)

  # if [[ ! -d $datadir ]]; then
  #   echo "hello"
  #   return 0
  # fi

  NOW=$SECONDS
  echo "upifinder $cmd -c $datadir > $5/$host-upi-$cmd-$archive-$source.lst"
  if [ $? -ne 0 ]; then
    exit 1
  fi
  elapsed=$((SECONDS-NOW))
  echo "$elapsed seconds - $(du -sh $datadir)"
}

which upifinder &> /dev/null
if [ $? -ne 0 ]; then
  echo "upifinder not found in $PATH"
  exit 123
fi

HOST=$(hostname)
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
        echo "$OPTARG: not a directory"
        exit 124
      fi
      ARCDIR=$OPTARG
      ;;
    d)
      DATDIR=$OPTARG
      mkdir -p $DATDIR
      if [ $? -ne 0 ]; then
        echo "fail to create directory $DATDIR"
        exit 124
      else
        if [[ ! -d $DATDIR ]] || [[ ! -w $DATDIR ]]; then
          echo "$DATDIR: is not a writable directory"
          exit 124
        fi
      fi
      ;;
    *)
      echo "$(basename $0) traverse the full archive to run upifinder on each source in parallel"
      echo ""
      echo "options:"
      echo ""
      echo "-n NAME  archive name (default: $WHERE)"
      echo "-a DIR   archive directory (default: $ARCDIR)"
      echo "-d DIR   data directory (default: $DATDIR)"
      echo "-h HOST  hostname (default: $HOST)"
      echo "-j JOBS  number of parallel jobs (default: $JOBS)"
      echo ""
      echo "example:"
      echo ""
      echo "$ $(basename $0) n share -a /archives -d /var/www/html -h testme {39..41} 5"
      echo ""
      echo "usage: $(basename $0) [-j obs] [-a arcdir] [-n name] [-d datadir] [-h host] <source...>"
      exit 1
      ;;
  esac
done
shift $(($OPTIND - 1))

export -f runUPI
parallel --will-cite -j $JOBS runUPI $ARCDIR/{1} {2} $WHERE $HOST $DATDIR ::: $@ ::: walk check
if [ $? -ne 0 ]; then
  echo "unexpected error when running upifinder"
  exit 125
fi
