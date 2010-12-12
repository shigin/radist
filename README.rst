======================================================
Radist: a really simple multiply server command runner
======================================================

The really simple configuration file:

    # http://conf/config/ixServers.cfg
    server1     -build -log
    server2     -frontend -log
    server3     -dev_frontend
    server4     -dev_build -backup -log

And you can run some commands on the sets of machine:

    $ ./rrun -E /ix/log df -h /
    server1 : Filesystem            Size  Used Avail Use% Mounted on
    server1 : /dev/sda1              23G   21G  1.1G  95% /
    server1 :
    server2 : Filesystem            Size  Used Avail Use% Mounted on
    server2 : /dev/sda3              20G   19G  1.9G  95% /
    server3 : Filesystem            Size  Used Avail Use% Mounted on
    server3 : /dev/sda2              20G   10G   10G  50% /
    server2 :
    server3 :

There is a various methods in radist library. Just try it.
