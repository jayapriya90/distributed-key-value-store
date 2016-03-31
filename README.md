# DistributedFileSystem
DFS that uses Gifford's Quorum Based Protocol for file consistency. 

How To Compile:
--------------
1) To compile this project simply run compile.sh script found in root directory.
2) After the build is successful, use the following instructions to run the program

Scripts for launching different components
------------------------------------------
Following 3 scripts are used for launching different services. Use the scripts in
same following order
1) coordinator
2) fileserver
3) clients

Options for coordinator
-----------------------
coordinator script should be run first before running any scripts. The usage for
coordinator script is
usage: coordinator
 -h          Help
 -nr <arg>   Size of read quorum (default chosen by nr + nw > n rule)
 -nw <arg>   Size of write quorum (default chosen by nw > n/2 rule)
 -si <arg>   Sync interval (default: 5 seconds)

To run with defaults,
> ./coordinator
To run with custom read and write quorum size
> ./coordinator -nr 4 -nr 5
To run with custom sync interval (time interval between which the sync task are run)
> ./coordinator -si 10

Options for fileserver
----------------------
fileserver script accepts one optional argument i.e, hostname of coordinator. 
If no argument is specified, by default localhost will be used for coordinator. 
This script can be run multiple times on a single host (will use different
ports for every run) or can be run on different machines. When coordinator
and fileserver scripts are run on different machines it is mandatory to provide
the hostname for coordinator

To run (coordinator running on same host),
> ./fileserver
To run when coordinator running on different host,
> ./node <hostname-for-coordinator>

Options for clients
-------------------
clients script can mimic multiple concurrent clients. Internally, it uses multiple
threads one for each independent client.

Following is the usage guide for clients script.
usage: clients
 -f <arg>   File containing each line with read/write request in
            filename=contents format.
            Example: input.txt contents for RRW sequence
            file1
            file2
            file3=value3
 -h <arg>   Hostname for coordinator (default: localhost)
 -help      Help
 -i <arg>   CSV list of request sequence in filename=contents format.
            Example: file1,file2,file3=value3 is RRW (read, read, write)
            sequence.
 -n <arg>   Number of concurrent clients (Each client will run in a
            thread)
 -p         Print file servers metadata

To run,

Example: 3 read requests with 3 clients
> ./clients -n 3 -i foo1.txt,foo2.txt,foo3.txt

Example: 2 writes followed by 2 reads on 3 clients

> ./clients -n 3 -i foo1.txt=bar1,foo2.txt=bar2,foo1,foo2

To print all file servers metadata after a read,

> ./clients -i foo1.txt -p

To print all file servers metadata alone

> ./clients -p

If clients script and coordinator runs on different hosts then coordinator's
hostname can be specified like below

> ./clients -h <coordinator-hostname> -r foo1.txt -p

To specify read and write sequences from input file and print metadata of all servers

> ./clients -f request.txt -p
