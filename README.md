# What is this?
* Most MySQL heartbeat implementations require an external script or daemon process.  I would rather avoid that if possible.  As such, this is a pure MySQL implementation. 
* Tested on Percona Server 5.7, however should work with any MySQL variant down to 5.6.

# How do I use it?
* The only prerequisite is that you have the MySQL event scheduler enabled.
* You may consider modifying object names or set the replicate-wild-ignore-table param to %.%_norep
  * If %_norep tables are ignored be sure to create those objects on the slave side. 
