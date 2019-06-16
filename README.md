# python_scripts    

## opscenter_ha_sync.py

**version**

0.6.4

**about**

* this script syncs the opscenter config folder on the active and passive nodes in an HA deployment.    
* syncs the active node's opscenter config folder(s) into Cassandra and pulls down into corresponding passive node folder.    
* --> i.e. /etc/opscenter/ and /var/lib/opscenter/ssl (package install default folders).    
* --> additional folders may be specified.      
* redundant files are timestamp renamed (not deleted).    
* the primary_opscenter_location file is renamed on a switch over to avoid manual intervention.    
* the required Cassandra table is made by the script.

**rsync**

* the functionality of this script is usually performed by rsync.    
* this script was developed for a customer where this was not an option.       

**prerequisites**    

* dse python driver is installed on both Opscenter nodes.    
* necessary authentication is setup for the driver to talk to the Opscenter storage cluster.    
* the Opscenter service is managing the metrics for the Opscenter storage cluster.    
* --> during a fail over event, the passive server will reach out and update the 'address.yaml' of every agent.    
* --> so reading the 'stomp_address' value within this local file is a reliable means of determining the active server.   

**instructions**    

1) on both nodes: install dse python driver.    
2) on both nodes: add opscenter_ha_sync.py and edit the user defined section.   
3) on both nodes: setup a cronjob to run script every 5 mins.                
