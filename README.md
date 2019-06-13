# opscenter_ha_sync.py

**versioning**

0.6.3

**about**

* this script syncs the opscenter config folder on the active and passive nodes in an HA deployment.    
* it writes all files from the active node into Cassandra and pulls them down onto the passive node.    
* redundant files are timestamp renamed (not deleted).    
* the primary_opscenter_location file is renamed on a switch over to avoid manual intervention.    
* the required Cassandra table is made by the script.

**rsync**

* the functionality of this script is usually performed by rsync.    
* this script was developed for a customer where this was not an option.       

**prerequisites**    

* dse python driver is installed on both opscenter nodes.    
* necessary authentication is setup for the driver to talk to the Opscenter storage cluster.    
* the Opscenter service is managing the metrics for the Opscenter storage cluster.    
* --> as the datastax agent local to the node is queried for its 'stomp_address' to determine the active server.    

**instructions**    

1) on both nodes: install dse python driver.    
2) on both nodes: add opscenter_ha_sync.py and edit the user defined section.   
3) on both nodes: setup a cronjob to run script every 5 mins.    
4) go down pub.               
