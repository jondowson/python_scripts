import os
import time
import subprocess
import hashlib
import datetime
from dse.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from dse.query import tuple_factory

profile = ExecutionProfile(row_factory=tuple_factory)
cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile})
session = cluster.connect()

# ******************************************************
# ABOUT: version - 0.6.3
# ******************************************************
# this script is run simultaneously on both the active and passive Opscenter servers (fired by a cron job every 5 mins).
# this script determines if host machine is the 'active' or 'passive' one.
# on the active server it will push any new / updated config files from opscenter folder to Cassandra + remove from Cassandra any deleted file entries.
# on passive server it will 'remove' any files (by renaming with a timestamp) if they no longer exist in Cassandra table + pull down any new / updated files.
# md5checksums are used to compare file versions.

# ******************************************************
# FILL THESE IN:
# ******************************************************
# [A] local opscenter config folder path.
local_opsConfigFolderPath = '/etc/opscenter'
# [B] path to active opscenter ip file - file will exist on passive server only.
local_polPath = '/var/lib/opscenter/failover/primary_opscenter_location'
# [C] local datastax-agent address.yaml path
local_agentAddressYamlPath = '/var/lib/datastax-agent/conf/address.yaml'
# [D] the reachable ip of this machine from the other opscenter node.
local_serverIp = "x.x.x.x"
# [E] choose + amend appropriate create keyspace command from these three templates.
keyspace_cmd = "CREATE KEYSPACE IF NOT EXISTS ha WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
#keyspace_cmd = "CREATE KEYSPACE IF NOT EXISTS ha WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1' : 1};"
#keyspace_cmd = "CREATE KEYSPACE IF NOT EXISTS ha WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1' : 3, 'DC2' : 3};"
# ******************************************************

# check required files exist - if not bail out with helpful error message
if not os.path.isdir(local_opsConfigFolderPath) \
or not os.path.isfile(local_agentAddressYamlPath):
    print "Required file not found - check fs and entries at top of script"
if not os.path.isfile(local_polPath) \
and not os.path.isfile(local_polPath + '.dormant'):
    print "Required 'primary_opscenter_location' file not found - check fs and entry at top of script"

# helper function for reading file into bytes
def file_as_bytes(file):
    with file:
        return file.read()

# ======================================================
# [1] CREATE CASSANDRA HA KEYPACE AND TABLE:
session.execute(keyspace_cmd);
session.execute('CREATE TABLE IF NOT EXISTS ha.active_files(filepath text, contents text, md5sum text, PRIMARY KEY (filepath))')

# ======================================================
# [2] ESTABLISH SERVER TYPE (ACTIVE or PASSIVE):
# --> check the stomp_interface ip in the local datastax-agent's address.yaml.
# ----> if it is pointing to the local ip - then this is the active server.
local_agentStompIp = subprocess.check_output(["sed", "-n", "-e",  's/^.*stomp_interface: //p', local_agentAddressYamlPath])
local_agentStompIp = local_agentStompIp.replace("\n", "")
if local_agentStompIp == local_serverIp:
    local_isActive = True
else:
    local_isActive = False

# ======================================================
# [3] ON ACTIVE NODE:
# --> if 'primary_opscenter_location' file exists on active server, then disable it by renaming it with the '.dormant' extension.
# --> push any new or updated config files to Cassandra.
# --> remove any cassandra rows referencing files that no longer exist on this active server.
if local_isActive == True:
    if os.path.isfile(local_polPath):
        newName = local_polPath + '.dormant'
        os.rename(local_polPath, newName)
    # walk through all local opscenter config files including those in sub-folders.
    for root, d_names, f_names in os.walk(local_opsConfigFolderPath):
        for f in f_names:
            # get the full local path of the file.
            local_configFilePath = (os.path.join(root, f))
            # ignore files that have been backed and the pol file.
            if "primary_opscenter_location" in f \
            or ".backup" in f:
                pass
            else:
                # get the md5sum of the file.
                local_md5sum = hashlib.md5(file_as_bytes(open(local_configFilePath, 'rb'))).hexdigest()
                lookup_stmt = "SELECT filepath, contents, md5sum FROM ha.active_files WHERE filepath=%s"
                result = session.execute(lookup_stmt, [local_configFilePath])
                r = result.one();
                # if response is empty - then file must be new on active server - so write it into Cassandra.
                # - or if local_md5sum is different to that stored in Cassandra, write updated file.
                if r is None or local_md5sum != r[2]:
                    thisFile = open(local_configFilePath, 'rb')
                    local_contents = thisFile.read()
                    CQLString = "INSERT INTO ha.active_files (filePath,contents,md5sum) VALUES (%s,%s,%s)"
                    session.execute(CQLString, (local_configFilePath,local_contents,local_md5sum))
                    thisFile.close
    # if config file does not exist locally then remove it from cassandra.
    rows = session.execute('SELECT * FROM ha.active_files')
    for row in rows:
        configFilePath = row[0]
        contents = row[1]
        md5sum   = row[2]
        # if it exists locally do nothing.
        if os.path.isfile(configFilePath):
            pass
        else:
            CQLString = "DELETE FROM ha.active_files WHERE filepath=%s"
            session.execute(CQLString, [configFilePath])

# ======================================================
# [4] ON PASSIVE NODE:
# --> pause a short time to allow propogation of any changes, prompted by the running of this script on the active node, to this DC.
# --> check all local files exist in cassandra - if not, 'delete' (rename) them locally as they do not exist on the active node.
# --> pull any updated or new config files from cassandra onto the local file system.
if local_isActive == False:
    time.sleep( 10 )
    local_dormantPOLF = local_polPath + '.dormant'
    if os.path.isfile(local_dormantPOLF):
        os.rename(local_dormantPOLF, local_polPath)
    # walk through all files including those in sub-folders and check against Cassandra table.
    for root, d_names, f_names in os.walk(local_opsConfigFolderPath):
        for f in f_names:
            # get the full path of the file.
            local_configFilePath = (os.path.join(root, f))
            # ignore files that have been backed up and the pol file.
            if "primary_opscenter_location" in f \
            or ".backup" in f:
                pass
            else:
                # get the md5sum of the file.
                local_md5hash = hashlib.md5(file_as_bytes(open(local_configFilePath, 'rb'))).hexdigest()
                lookup_stmt = "SELECT * FROM ha.active_files WHERE filepath=%s"
                result = session.execute(lookup_stmt, [local_configFilePath])
                r = result.one();
                # if response is empty - then file is not listed in cassandra and therefore must not exist on active server.
                # - so timestamp rename it to disable it on local file system.
                if r is None:
                    dt = str(datetime.datetime.now())
                    newName = local_configFilePath +'_'+ dt + '.backup'
                    os.rename(local_configFilePath, newName)
                else:
                    pass
    # if a filepath in Cassandra does not exist locally or its md5sum differs, then pull it down.
    rows = session.execute('SELECT * FROM ha.active_files')
    for row in rows:
        configFilePath = row[0]
        contents = row[1]
        md5sum   = row[2]
        if os.path.isfile(configFilePath) and md5sum == hashlib.md5(file_as_bytes(open(configFilePath, 'rb'))).hexdigest():
            pass
        else:
            thisFile = open( configFilePath, 'w' )
            thisFile.write( contents )
            thisFile.close()
