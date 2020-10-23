# ldbc-sparksee

## Dependencies
This project depends on
- Sparksee
- Boost

## Setting up environment

This project is meant to be used through the scripts/ldbc_snb.sh script. Call
the script from your desired workspace (${WORKSPACE}). Do not copy the script elsewhere since
its current directory is used to find the path to the root ldbc-sparksee folder. 

Call: 

```
${LDBC_SPARKSEE_ROOT}/scripts/ldbc_snb.sh install
```

and follow the instructions. This will setup the running environment, including the downloading of the LDBC driver and its patching and compilation. You'll need to provide:
* A datasets repository folder ($LDBC_DATASETS_REPOSITORY) with the ldbc datasets. The folder should follow the following structure:
```
  $LDBC_DATASETS_REPOSITORY/
    |____ 0001
        |--- social_network
        |--- substitution_parameters
    |____ 0003
    |____ 0010
    ...
```

where each subfolder contains the desired scale factors datasets. 

The other two repositories to provide are the $LDBC_IMAGES_REPOSITORY where the loaded databases will be stored and the $LDBC_RESULTS_REPOSITORY where the results of the benchmark runs will be stored. In this case you'll only be requested to provide the root folder to these repositories, the structure will be created by the script. 

You'll also be prompted for the sparksee source code repository. This is optional and only used by Sparksee developers, so leave it blank. 

## Loading data

You need a **sparksee.cfg** file in $WORKSPACE with the corresponding license. Then, to load such dataset run:


```
${LDBC_SPARKSEE_ROOT}/scripts/ldbc_snb.sh load --help
```
which will show the different options required

```
	ldbc_snb.sh load [options]"
	options: "
			-sf/--scalefactor <the scalefactor of the source data (e.g. 0001, 0003, 0010, 0030, etc.> 
			-nt/--numthreads <the number of threads that were used to generate the dataset. Generally 1> 
			-np/--numpartitions <the number of partitions that were used to generate the dataset. Generally 1> 
			-t/--tag <the tag used to identify this version. Meant to differentiate data loaded between different sparksee versions> 
			-s/--sparksee <path to sparksee library root folder. If not specified, default repository will be used with the provided tag> 
```

## Running the benchmark


Run the following command to know how to run the benchmark

```
${LDBC_SPARKSEE_ROOT}/scripts/ldbc_snb.sh run --help
```

which will show the different options required

```
	ldbc_snb.sh run [options]: 
	-sf|--scalefactor <the scale factor to run> 
	-st|--serverthreads <the number of threads in the server> 
	-dt|--driverthreads <the number of threads in the driver> 
	-m|--maxspeed the driver runs at max speed by ignoring scheduled times 
	-tcr|--tcratio the tcr of the run
	-t|--tag the tag identifying the sparksee version. If SPRAKSEE_GIT_REPOSITORY was set, this must match a valid commit/branch/tag
	-o|--operations the number of operations to run
	-wo|--warmupoperations the number of warmup operations to run
  -s|--sparksee path to the sparksee distribution root folder
```


