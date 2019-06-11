# opscenter_ha_sync.py

## about

This script syncs the opscenter config folder on the active and passive nodes in an HA deployment.    
It writes all files from the active node into Cassandra and pulls them down onto the passive node.    
Redundant files are timestamp renamed (not deleted).    
The primary_opscenter_location file is renamed on a switch over to avoid manual intervention.    
The required Cassandra table is made by the script.    

**Instructions**    

1) On both nodes: install dse python driver.    
2) On both nodes: add opscenter_ha_sync.py and edit the user defined section.   
3) On both nodes: setup a cronjob to run script every 5 mins.    
4) Go down pub.               

## versioning

0.5.1
