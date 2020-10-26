#!/bin/bash

set -e
# Variable declaration and initialization
ROOT=$(pwd)


##################
# HELPER METHODS #
##################

function get_abs_path {
  echo $(readlink -m $1)
}

function print_config {
  cat ldbc.cfg
}

# Prints the usage of the script
function print_usage {
	echo "ldbc_snb.sh [command] [options]"
	echo "commands: "
	echo "		install "
	echo "		load "
	echo "		run "
	echo "		validate "
	echo "for help on a particular command run:"
	echo "ldbc_snb.sh command --help"
}

# Prints the usage of install command
function print_install_usage {
	echo "ldbc_snb.sh install [options]"
	echo "		-h/--help <help info> "
}

# Prints the usage of load command
function print_load_usage {
	echo "ldbc_snb.sh load [options]"
	echo "options: "
	echo "		-sf/--scalefactor <the scalefactor of the source data (e.g. 0001, 0003, 0010, 0030, etc.> "
	echo "		-nt/--numthreads <the number of threads that were used to generate the dataset> "
	echo "		-np/--numpartitions <the number of partitions that were used to generate the dataset> "
	echo "		-t/--tag <the tag used to identify this version. Not necessarily the official one, user can chose it> "
	echo "		-s/--sparksee <path to sparksee library root folder. If not specified, default repository will be used with the provided tag> "
  echo "    -c|--commit The sparksee commit/tag/branch to use if the sparksee git"
}

# Prints the usage of run command
function print_run_usage {
	echo " ldbc_snb.sh run [options]: "
	echo " -sf|--scalefactor <the scale factor to run> "
	echo " -st|--serverthreads <the number of threads in the server> "
	echo " -dt|--driverthreads <the number of threads in the driver> "
	echo " -m|--maxspeed the driver runs at max speed by ignoring scheduled times "
	echo " -tcr|--tcratio the tcr of the run"
	echo " -t|--tag the tag identifying the sparksee version. If SPRAKSEE_GIT_REPOSITORY was set, this must match a valid commit/branch/tag"
	echo " -o|--operations the number of operations to run"
	echo " -wo|--warmupoperations the number of warmup operations to run"
  echo " -s|--sparksee path to the sparksee distribution root foler"
  echo " -c|--commit The sparksee commit/tag/branch to use if the sparksee git
  repository was specified"
	exit
}

# Prints the progress
function print_progress {
	echo ".... $1"
}

function print_error {
	echo "ERROR: $1!!!"
}

# checks if ldbc has been installed. If so, import configuration variables
function check_installed {
  if test ! -f ldbc.success
  then
    printf_error "ldbc.cfg not found. Please run \"ldbc_snb.sh install\" first"
    exit 1
  fi
  set -o allexport
  source ldbc.cfg
  set +o allexport
  print_config
}

function compile_with_repository {

  if test ! -z $SPARKSEE_GIT_REPOSITORY
  then
    BRANCH=$1
    pushd $SPARKSEE_GIT_REPOSITORY
    git checkout $BRANCH
    git pull
    rm -rf build
    mkdir -p build
    pushd build
    cmake $SPARKSEE_CMAKE_FLAGS ..
    make -j${COMPILATION_JOBS} sparkseecpp
    make -j${COMPILATION_JOBS} GDBBackup
    make -j${COMPILATION_JOBS} GDBRestore
    popd
    popd

    rm -rf $LDBC_ROOT/build
    mkdir -p $LDBC_ROOT/build
    pushd $LDBC_ROOT/build
    cmake -DSPARKSEE_ROOT=${SPARKSEE_GIT_REPOSITORY} $LDBC_CMAKE_FLAGS ..
    make -j${COMPILATION_JOBS}
    popd
  else
    print_error "Sparksee repository is not set. Cannot compile"
    exit
  fi
}

function compile_with_path {
  if test ! -z $1
  then
    SPARKSEE_PATH=$(get_abs_path $1)
    rm -rf $LDBC_ROOT/build
    mkdir -p $LDBC_ROOT/build
    pushd $LDBC_ROOT/build
    cmake -DSPARKSEE_INCLUDES=${SPARKSEE_PATH}/includes/sparksee -DSPARKSEE_LIB_DIR=${SPARKSEE_PATH}/lib/linux64 $LDBC_CMAKE_FLAGS ..
    make -j${COMPILATION_JOBS}
    popd
  else
    print_error "Sparksee path is not set. Cannot compile"
    exit
  fi
}

function compile {

  if test ! -z $2
  then
    compile_with_path $2
  elif test ! -z $SPARKSEE_GIT_REPOSITORY
  then
    compile_with_repository $1
  else
    print_error "Neither sparksee path nor repository are specified!"
    exit
  fi
}

# patches the ldbc_snb_driver with the patch from LDBCpp
function patch_ldbc_snb_driver {
	if [[ ! -z $LDBC_ROOT ]]
	then 
		mkdir -p ldbc_snb_driver/src/main/java/com/ldbc/driver/sparksee/workloads/ldbc/snb/interactive/db
		cp $LDBC_ROOT/driverPatch/*.java ldbc_snb_driver/src/main/java/com/ldbc/driver/sparksee/workloads/ldbc/snb/interactive/db/
		cp $LDBC_ROOT/driverPatch/pom.xml ldbc_snb_driver/
		cp $LDBC_ROOT/driverPatch/configurations/* ldbc_snb_driver/src/main/resources/configuration/ldbc/snb/interactive/
	fi
}

# syncs the driver with the latest version in the repository
function synch_ldbc_snb_driver {
	print_progress "Syncing LDBC Driver"
	cd ldbc_snb_driver 
	git pull
	cd $ROOT
}

# installs the ldbc_snb_driver
function install_ldbc_snb_driver {
  print_progress "Installing LDBC Driver"
  if test ! -f 0.3.3.tar.gz
  then
    wget https://github.com/ldbc/ldbc_snb_driver/archive/0.3.3.tar.gz
  fi
  tar xvfz 0.3.3.tar.gz
  mv ldbc_snb_driver-0.3.3 ldbc_snb_driver

  pushd ldbc_snb_driver
  popd	
  patch_ldbc_snb_driver
  pushd ldbc_snb_driver
  mvn -DskipTests package
  popd
  echo "LDBC_DRIVER=$ROOT/ldbc_snb_driver" >> ldbc.cfg
}

# installs sparksee
function install_sparksee {
  print_progress "Please specify sparksee git repository url (Optional. Leave it blank if you have  a compiled sparkseecpp distribution): "
  read SPARKSEE_GIT_REPOSITORY
  if test ! -z SPARKSEE_GIT_REPOSITORY
  then
    git clone $SPARKSEE_GIT_REPOSITORY
    echo "SPARKSEE_GIT_REPOSITORY=$ROOT/sparksee" >> ldbc.cfg
  else
    echo "SPARKSEE_GIT_REPOSITORY=" >> ldbc.cfg
  fi
}

# configure repositories
function configure_repositories {
  print_progress "Please specify path to datasets repository: "
  read LDBC_DATASETS_REPOSITORY
  LDBC_DATASETS_REPOSITORY=$(get_abs_path $LDBC_DATASETS_REPOSITORY)
  if test ! -d $LDBC_DATASETS_REPOSITORY
  then
    print_error "${LDBC_DATASETS_REPOSITORY} does not exist"
    exit 
  fi
  echo "LDBC_DATASETS_REPOSITORY=$LDBC_DATASETS_REPOSITORY" >> ldbc.cfg

  print_progress "Please specify path to loaded images repository (where loaded images will be stored): "
  read LDBC_IMAGES_REPOSITORY
  LDBC_IMAGES_REPOSITORY=$(get_abs_path $LDBC_IMAGES_REPOSITORY)
  if test ! -d $LDBC_IMAGES_REPOSITORY
  then
    print_error "${LDBC_IMAGES_REPOSITORY} does not exist"
    exit 
  fi
  echo "LDBC_IMAGES_REPOSITORY=$LDBC_IMAGES_REPOSITORY" >> ldbc.cfg

  print_progress "Please specify path to loaded resuls repository (where benchmark results will be stored): "
  read LDBC_RESULTS_REPOSITORY
  LDBC_RESULTS_REPOSITORY=$(get_abs_path $LDBC_RESULTS_REPOSITORY)
  if test ! -d $LDBC_RESULTS_REPOSITORY
  then
    print_error "${LDBC_RESULTS_REPOSITORY} does not exist"
    exit 
  fi
  echo "LDBC_RESULTS_REPOSITORY=$LDBC_RESULTS_REPOSITORY" >> ldbc.cfg



  LDBC_ROOT=$(dirname $0)
  LDBC_ROOT=$(get_abs_path "$LDBC_ROOT/../")
  echo "LDBC_ROOT=$LDBC_ROOT" >> ldbc.cfg

  echo "COMPILATION_JOBS=8" >> ldbc.cfg
}

# uninstalls the benchmark
function uninstall {
	rm -rf sparksee
	rm -rf ldbc_snb_driver
}

# initializes a repository for a loaded image
function initialize_repository {
	mkdir -p $1
	mkdir -p $1/social_network
	cp -r $2/social_network/updateStream* $1/social_network/
	cp -r $2/substitution_parameters/ $1/
}


######################
# SCRIPT STARTS HERE #
######################

if [[ $1 != install  &&  $1 != run && $1 != uninstall && $1 != load && $1 != synch && $1 != validate && $1 != patch ]]
then
	echo "Invalid command"
	print_usage
fi

if [[ $1 == install ]]
then

		while [[ $# > 0 ]]
		do
			key="$1"

			case $key in
				-h|--help)
					HELP=YES
					;;
			esac
			shift
		done

		if [[ ! -z $HELP ]]
		then
			print_install_usage
			exit
		fi

    echo "STARTING INSTALLATION ..."
    rm -f ldbc.success
    rm -f ldbc.cfg
    rm -rf ldbc_snb_driver
    rm -rf sparksee 
    echo "" > ldbc.cfg
    configure_repositories
    install_ldbc_snb_driver
    install_sparksee
    echo "LDBC_CMAKE_FLAGS=-DCMAKE_BUILD_TYPE=RELEASE" >> ldbc.cfg
    echo "SPARKSEE_CMAKE_FLAGS=-DCMAKE_BUILD_TYPE=RELEASE" >> ldbc.fg
    touch ldbc.success
    echo "INSTALLATION FINISHED"
    cat ldbc.cfg
fi

##### RUN SECTION #######

if [[ $1 == run ]]
then
  check_installed

  NON_CONSUMED=""
  while [[ $# > 0 ]]
  do
    key="$1"
    case $key in
      -h|--help)
        HELP=YES
        ;;
      -t|--tag)
        TAG="$2"
        shift # past argument
        ;;
      -sf|--scalefactor)
        SCALEFACTOR="$2"
        shift # past argument
        ;;
      -s|--sparksee)
        SPARKSEE_PATH="$2"
        shift # past argument
        ;;
      -c|--commit)
        SPARKSEE_COMMIT="$2"
        shift # past argument
        ;;
      *)
      NON_CONSUMED="$NON_CONSUMED $key"
      ;;
    esac
    shift
  done

  if [[ ! -z $HELP ]]
  then
    print_run_usage
    exit
  fi


  if [[ -z $SCALEFACTOR ]]
  then
    print_error "--scalefactor option is missing"
    exit
  fi

  if [[ -z $TAG ]]
  then
    print_error "--tag option is missing"
    exit
  fi

  if [[ -z $SPARKSEE_PATH ]]
  then
    if [[ -z $SPARKSEE_GIT_REPOSITORY ]]
    then
      print_error "Neither SPARKSEE git repository not SPARKSEE library path have been provided"
      exit
    elif [[ -z $SPARKSEE_COMMIT ]]
    then
      print_error "--commit option is missing"
      exit
    fi
  fi

  if test ! -f $LDBC_IMAGES_REPOSITORY/$SCALEFACTOR/$TAG/snb.gdb
  then
		print_error "The required image $LDBC_IMAGES_REPOSITORY/$SCALEFACTOR/$TAG/snb.gdb does not exist. Load it first with ldbc_snb.sh load command"
    exit
  fi

  compile $SPARKSEE_COMMIT $SPARKSEE_PATH

	if [[ ! -z $LDBC_ROOT && -f $LDBC_ROOT/build/server ]]
	then
		$LDBC_ROOT/scripts/test_server.sh --server $LDBC_ROOT \
                              -d $LDBC_DRIVER \
                              -w $ROOT \
                              -sf $SCALEFACTOR \
                              -t $TAG \
                              -r $LDBC_IMAGES_REPOSITORY \
                              -f $LDBC_DRIVER/src/main/resources/configuration/ldbc/snb/interactive/ldbc_snb_interactive_SF-${SCALEFACTOR}.properties \
                              -rp $LDBC_RESULTS_REPOSITORY \
                              $NON_CONSUMED
	else
		print_error "Could not find server"
	fi
	exit
fi

##### LOAD SECTION #######

if [[ $1 == load ]]
then
  check_installed 

  ## DEFAULT OPTIONS
  NUMTHREADS=1
  NUMPARTITIONS=1

while [[ $# > 0 ]]
do
	key="$1"
	case $key in
		-h|--help)
			HELP=YES
			;;
		-sf|--scalefactor)
			SCALEFACTOR="$2"
			shift # past argument
			;;
		-nt|--numthreads)
			NUMTHREADS="$2"
			shift # past argument
			;;
		-np|--numpartitions)
			NUMPARTITIONS="$2"
			shift # past argument
			;;
		-t|--tag)
			TAG="$2"
			shift # past argument
			;;
		-s|--sparksee)
			SPARKSEE_PATH="$2"
			shift # past argument
			;;
		-c|--commit)
			SPARKSEE_COMMIT="$2"
			shift # past argument
			;;
	esac
	shift
done


if [[ ! -z $HELP ]]
then
	print_load_usage
	exit
fi

if [[ -z $TAG ]]
then
	print_error "--tag option is missing"
	exit
fi

if [[ -z $SPARKSEE_PATH ]]
then
  if [[ -z $SPARKSEE_GIT_REPOSITORY ]]
  then
    print_error "Neither SPARKSEE git repository not SPARKSEE library path have been provided"
    exit
  elif [[ -z $SPARKSEE_COMMIT ]]
  then
    print_error "--commit option is missing"
    exit
  fi
fi


if [[ -z $SCALEFACTOR ]]
then
	print_error "--scalefactor option is missing"
	exit
fi

if [[ -z $NUMTHREADS ]]
then
	print_error "--numthreads option is missing"
	exit
fi

if [[ -z $NUMPARTITIONS ]]
then
	print_error "--numpartitions option is missing"
	exit
fi

if test ! -d $LDBC_DATASETS_REPOSIOTRY 
then
	print_error "Dataset repository does not exist"
	exit
fi

if test ! -d $LDBC_IMAGES_REPOSIOTRY 
then
	print_error "Image repository does not exist"
	exit
fi

compile $SPARKSEE_COMMIT $SPARKSEE_PATH

initialize_repository $LDBC_IMAGES_REPOSITORY/$SCALEFACTOR/$TAG $LDBC_DATASETS_REPOSITORY/$SCALEFACTOR

cp $LDBC_ROOT/scripts/load_data.sh ./
./load_data.sh $LDBC_DATASETS_REPOSITORY/$SCALEFACTOR/social_network $NUMTHREADS $NUMPARTITIONS $LDBC_ROOT
mv snb.gdb* $LDBC_IMAGES_REPOSITORY/$SCALEFACTOR/$TAG

if test ! -z SPARKSEE_GIT_REPOSITORY
then
  ${SPARKSEE_GIT_REPOSITORY}/build/GDBBackup $LDBC_IMAGES_REPOSITORY/$SCALEFACTOR/$TAG/snb.gdb $LDBC_IMAGES_REPOSITORY/$SCALEFACTOR/$TAG/snb.gdb.backup
fi

fi

if [[ $1 == uninstall ]]
then
	uninstall
	exit
fi

if [[ $1 == validate ]]
then
  check_installed
	
	#if [[ ! -z $LDBC_SPARKSEE && -f $LDBC_SPARKSEE/build/server ]]
	#then
	#	$LDBC_SPARKSEE/scripts/run_validation_dataset.sh $@ -w /tmp --ldbcpp
  #  $LDBC_SPARKSEE -dd ./ldbc_snb_driver
	#else
	#	print_error "Need to speficy sparksee version at the first run parameter"
	#fi
	#exit
fi

if [[ $1 == patch ]]
then
  check_installed
	#shift
	#if [[ ! -z $LDBC_SPARKSEE ]]
	#then 
	#	if [[ ! -z $1 ]]
	#	then
	#	mkdir -p $1/src/main/java/com/ldbc/driver/sparksee/workloads/ldbc/snb/interactive/db
	#	cp $LDBC_SPARKSEE/driverPatch/*.java $1/src/main/java/com/ldbc/driver/sparksee/workloads/ldbc/snb/interactive/db/
	#	cp $LDBC_SPARKSEE/driverPatch/pom.xml $1/
	#	cp $LDBC_SPARKSEE/driverPatch/configurations/* $1/configuration/ldbc/snb/interactive/
	#fi
	#fi
fi
