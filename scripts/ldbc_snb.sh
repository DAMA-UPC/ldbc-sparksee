#!/bin/bash

# Variable declaration and initialization

SPARKSEE_BASE_REPOSITORY="https://circe.ac.upc.edu/dex/"
ROOT=$(pwd)

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
	echo "		-l/--ldbc-sparksee <The root folder of ldbc-sparksee> "
}

# Prints the usage of load command
function print_load_usage {
	echo "ldbc_snb.sh load [options]"
	echo "options: "
	echo "		-s/--source <the source data folder> "
	echo "		-r/--repository <the base repository where the loaded data will be installed> "
	echo "		-sf/--scalefactor <the scalefactor of the source data (e.g. 0001, 0003, 0010, 0030, etc.> "
	echo "		-nt/--numthreads <the number of threads that were used to generate the dataset> "
	echo "		-np/--numpartitions <the number of partitions that were used to generate the dataset> "
	echo "		-t/--tag <the tag used to identify this version. Not necessarily the official one, user can chose it> "
}


# Prints the progress
function print_progress {
	echo "....$1"
}

function print_error {
	echo "ERROR: $1!!!"
}

# syncs ldbcpp with the latest version in the repository
function synch_ldbcpp {
	print_progress "Syncing LDBCpp"
	cd LDBCpp
	svn update
	cd $ROOT
}

# patches the ldbc_driver with the patch from LDBCpp
function patch_ldbc_driver {
	if [[ ! -z $LDBC_SPARKSEE ]]
	then 
		mkdir -p ldbc_driver/src/main/java/com/ldbc/driver/sparksee/workloads/ldbc/snb/interactive/db
		cp $LDBC_SPARKSEE/driverPatch/*.java ldbc_driver/src/main/java/com/ldbc/driver/sparksee/workloads/ldbc/snb/interactive/db/
		cp $LDBC_SPARKSEE/driverPatch/pom.xml ldbc_driver/
		cp $LDBC_SPARKSEE/driverPatch/configurations/* ldbc_driver/configuration/ldbc/snb/interactive/
	fi
}

# syncs the driver with the latest version in the repository
function synch_ldbc_driver {
	print_progress "Syncing LDBC Driver"
	cd ldbc_driver 
	git pull
	cd $ROOT
}

# installs the ldbc_driver
function install_ldbc_driver {
	if [[ -z $LDBC_SPARKSEE ]]
	then
		print_error "Cannot install ldbc_driver, set LDBC_SPARKSEE first"
	else
		print_progress "Installing LDBC Driver"
		if [[ ! -d ldbc_driver ]]
		then
			git clone https://github.com/ldbc/ldbc_driver.git
		else
			synch_ldbc_driver
		fi

		patch_ldbc_driver
		cd ldbc_driver
		mvn -DskipTests package
	fi
	cd $ROOT
}

# syncs the reporter with the latest version in the repository
function synch_ldbc_report {
	print_progress "Syncing LDBC report"
	cd ldbc_snb_report 
	git pull
	cd $ROOT
}


# installs the ldbc reporter tool
function install_ldbc_report {
	print_progress "Installing LDBC Reporter"
	if [[ ! -d ldbc_snb_report ]]
	then
		git clone https://github.com/ldbc-dev/ldbc_snb_report.git
	else
		synch_ldbc_report
	fi
}

# sets up the required folder structure
function setup_folder_structure {
	print_progress "Setting up folder structure"
	mkdir sparksee
}

# uninstalls the benchmark
function uninstall {
	rm -rf sparksee
	rm -rf LDBCpp
	rm -rf ldbc_driver
	rm -rf ldbc_snb_report
}

function initialize_repository {
	mkdir -p $1
	mkdir $1/social_network
	cp -r $2/social_network/updateStream* $1/social_network/
	cp -r $2/substitution_parameters/ $1/
}


# SCRIPT STARTS HERE

if [[ $1 != install  &&  $1 != run && $1 != uninstall && $1 != load && $1 != synch && $1 != validate ]]
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

    install_ldbc_driver
fi

##### RUN SECTION #######

if [[ $1 == run ]]
then
	if [[ ! -z $LDBC_SPARKSEE && -f $LDBC_SPARKSEE/build/server ]]
	then
		$LDBC_SPARKSEE/scripts/test_server.sh $@ --server $LDBC_SPARKSEE --driver ./ldbc_driver
		#python2 ldbc_snb_report/create_report.py -i detail.csv -w ./ -o report
		#mv execution* sparksee/$SPARKSEE/results/
		#mv report.pdf sparksee/$SPARKSEE/results/
		#mv detail.csv sparksee/$SPARKSEE/results/
		#mv results/* sparksee/$SPARKSEE/results/
		#rm *.pdf
		#rm *.tex
		#rm *.aux
		#rm -rf results
	else
		print_error "Need to speficy sparksee version at the first run parameter"
	fi
	exit
fi

##### LOAD SECTION #######

if [[ $1 == load ]]
then
	if [[ -z $LDBC_SPARKSEE || ! -f $LDBC_SPARKSEE/build/snbLoaderStandalone ]]
	then
		print_progress "Run ./ldbc_snb.sh install --ldbcpp first before loading"
		exit
	fi

while [[ $# > 0 ]]
do
	key="$1"
	case $key in
		-h|--help)
			HELP=YES
			;;
		-s|--source)
			SOURCE="$2"
			shift # past argument
			;;
		-r|--repository)
			REPOSITORY="$2"
			shift # past argument
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
	exit
fi

if [[ -z $SOURCE ]]
then
	print_error "--source option is missing"
	exit
fi

if [[ -z $REPOSITORY ]]
then
	print_error "--repository option is missing"
	exit
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

if [[ -z $REPOSITORY ]]
then
	print_error "Repository does not exist"
	exit
fi
initialize_repository $REPOSITORY/$SCALEFACTOR/$TAG $SOURCE
cp $LDBC_SPARKSEE/scripts/load_data.sh ./
./load_data.sh $SOURCE/social_network $NUMTHREADS $NUMPARTITIONS $LDBC_SPARKSEE
mv snb.gdb* $REPOSITORY/$SCALEFACTOR/$TAG
fi

if [[ $1 == uninstall ]]
then
	uninstall
	exit
fi

if [[ $1 == validate ]]
then
	
	if [[ ! -z $LDBC_SPARKSEE && -f $LDBC_SPARKSEE/build/server ]]
	then
		$LDBC_SPARKSEE/scripts/run_validation_dataset.sh $@ -w /tmp --ldbcpp
    $LDBC_SPARKSEE -dd ./ldbc_driver
	else
		print_error "Need to speficy sparksee version at the first run parameter"
	fi
	exit
fi
