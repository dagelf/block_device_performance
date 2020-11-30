**Skip zero blocks for populating new blank drives, as well as sparse files.**

A simple dd-clone to experiment with block devices' performance which includes threaded/parallel writes.

Forked from https://github.com/erbth/block_device_performance with skip zero blocks functionality added. 


#### Compile 
    mkdir build
    cd build
    cmake ..
    make

#### Sparse file example:

    dd if=/dev/zero of=sparse-file bs=1M count=100 conv=sparse # or
    dd if=/dev/zero of=sparse-file bs=1 count=0 seek=100M
    
    sudo ./ddnz /dev/sda sparse-file 100M
    ls -lhs sparse-file 

The listed file will only be the size of the number of written blocks, if your filesystem supports sparse files. 

#### Clone a drive onto a new SSD in /dev/sdb:

    sudo ./ddnz /dev/sda /dev/sdb 

If the drive isn't blank be prepared to have a corrupt clone!

Of course it makes no sense to write output to a pipe or stream, as a steam can't be "seeked", so it will fail. You will have to encode your own receiver, or possibly add functionality to Rsync if you want it to work over a tunneling protocol.  

#### Alternatives

Functionally equivalent: ````dd if=/dev/sda of=/dev/sdb bs=1M conv=sparse````

Even more optimal: ````rsync -avz --sparse source.img dest.img````
*But can't extract smaller parts*

Can you think of any other ways? Can you pipe something to `rsync` or use `tar`? Do tell. 

#### Why?

Rusty. Coding practice. I forgot about `dd conv=sparse`. Also, this has parallelization which could add to speed improvements on networked filesystems, RAID and large RAM drives. 

#### Contribute?

Still lots to be done. 

`grep fixme *`

#### License

Ask https://github.com/erbth
