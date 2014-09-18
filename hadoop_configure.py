import boto
import boto.ec2
from time import sleep

# Declare Literals
__MASTER__ = 'master'
__SLAVE1__ = 'slave1'
__SLAVE2__ = 'slave2'
__DELIM__ = '-'
__IP__ = 'ip'
__ID__ = 0

# Declare Constants - temp fix - make more generic later
master_instance_id = 'i-33fefd6f'
slave1_instance_id = 'i-42fffc1e'
slave2_instance_id = 'i-71fdfe2d'
region_id = 'us-west-1'

# Declare Global vars
metadata = {}

def setup_metadata():
	# this is really about setting up the metadata dict
	# the metadata dict contains a list of items in each entry (key/value)
	# index 0 -> instance_id
	# index 1 -> public ip address (to be added later)
	# [Sure, there is a more efficient to use list generators but fine for now]
	masterList = []
	masterList.append(master_instance_id)
	metadata[__MASTER__] = masterList
	slave1List = []
	slave1List.append(slave1_instance_id)
	metadata[__SLAVE1__] = slave1List
	slave2List = []
	slave2List.append(slave2_instance_id)
	metadata[__SLAVE2__] = slave2List
	#print metadata

def start_hadoop_instances():
	
	# here go thru' the metadata dict, one key at a time
	for key in metadata.keys():
		#print key, metadata[key]
		instance = get_instance(metadata[key][__ID__])
		if instance.state == 'stopped':
			start_instance(instance,key)
			#for the ip_address retrieve the instance again. Cached value is useless
			instance = get_instance(metadata[key][__ID__],True)
		#print instance.ip_address
		metadata[key].append(instance.ip_address)
	#print 'all done'

def start_instance(instance, inst_type = __MASTER__, region_name='us-west-1'):
	# idea is to start the instance and then wait till the instance is completely started
    	print '\tStarting the %s instance %s' % (inst_type,instance.id)

	print '\t=============================================================='
	instance.start()
	print '\tInstance %s is %s' % (instance.id,instance.state)

	started = False
	while started == False:
		# setup a basic sleep of 5 seconds to allow for some shutdown time. AWS is not instantaneous :)
		sleep(5)
		# find the instance again
		inst = get_instance(metadata[inst_type][__ID__])
		if inst.state == 'running':
			started = True
	print '\t=============================================================='

def get_instance(instance_id, suppress=False, region_name=region_id):
	#print 'in get_instance: params %s %s' % (instance_id,region_name)
	
	# cached instances doesn't work so try the long route
	#print region_name
	conn = boto.ec2.connect_to_region(region_name)
	#print conn
	reservations = conn.get_all_instances(instance_id)
	#print reservations
	for r in reservations:
		#print r.instances
		for inst in r.instances:
			if not suppress: 
				print '\tInstance %s is %s' % (inst.id,inst.state)
			return inst


if __name__ == '__main__':
	# Step 0: Setup the metadata
	setup_metadata()
	# Step 1: Start all M/S instances
	# Step 2: Get the Public IP of master, and slaves --- for efficiency part of same function
    	start_hadoop_instances()
    	print metadata
	# Step 3: Update File 1 with required info
	# Step 4: Update remaining files with info. needed
	# Step 5: Copy files into master and slaves
	# Step 6: Start required hadoop processes in master
