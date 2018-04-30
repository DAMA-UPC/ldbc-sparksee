# ldbc-sparksee

## Dependencies
This project depends on
- Sparksee
- Boost

## Building

First of all, you must set the environemnt variable SPARKSEE_ROOT to point to the sparkseecpp distribution root folder.

Then,

```
mkdir build
cd build
cmake ..
make
```

## Setting up environment

We provide a helper script to ease the execution of the benchmark, including setting up the environment. This script is found at scripts/ldbc_snb.sh. Copy this script to your desired workspace. Before running the script, set the variable **LDBC_SPARKSEE** to point to the root folder of the ldbc-sparksee project with an **absolute path**. Then,

```
./ldbc_snb.sh install
```
This will setup the running environment, including the downloading of the LDBC driver and its patching and compilation.

## Loading data

We assume you have some dataset generated with the official LDBC data genreator, which is found in $DATA. $DATA, contains a social_network folder with the dataset, and a substitution_parameters folder with the parameters. 
Additionally, you need a **sparksee.cfg** file in the workspace with the corresponding license. Then, to load such dataset run:

```
ldbc-sparksee/scripts/ldbc_snb.sh load --tag data --source $DATA --repository $REPOSITORY --scalefactor <scale_factor> --numthreads 1 --numpartitions 1
```
where $DATA is where the dataset is, $REPOSITORY is the base folder used as repository for loaded images, and <scale_factor> is the scale factor this dataset corresponds to. Finally, numthreads and numpartitions are those used to generate the dataset, and tag is tag used to identify families of loaded images (for example, for different versions of sparksee). Both tag and scalefactor will be used later to identify the dataset to run the benchmark with.

## Running the benchmark

To run the benchmark, type ./ldbc_snb.sh run --help to see the different options. As an example of running an scale factor 0010 bi workload, with 100 warmup opeartions and 250 benchmark operations, using 1 driver and 1 server thread, see the following command. $WORKSPACE is the folder where the sparksee image will be copied to (this is specially useful if we want to use a specific distk, for instance, a SSD). -b option specifies that we want to run the BI workload (by default, it runs the interactive workload). Finally, the -m option tells the driver to run at maximum speed, thus ignoring the scheduled start times.

```
./ldbc_snb.sh run -r $REPOSITORY -w $WORKSPACE -t data -f $PWD/ldbc_driver/configuration/ldbc/snb/bi/ldbc_snb_bi_SF-0001.properties -b -o 250 -wo 100 -st 1 -dt 1 -sf 0010 -m
```

