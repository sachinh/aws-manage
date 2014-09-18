"""
Step 1: Start all M/S instances
Step 2: Get the Public IP of master, and slaves
Step 3: Update File 1 with required info
Step 4: Update remaining files with info. needed
Step 5: Copy files into master and slaves
Step 6: Start required hadoop processes in master
"""

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
instances = {}
instance_list = []
metadata = {}

def setup_metadata():
	# this is really about setting up the metadata dict
	# the metadata dict contains a list of items in each entry (key/value)
	# index 0 -> instance_id
	# index 1 -> public ip address (to be added later)
	masterList = []
	masterList.append(master_instance_id)
	metadata[__MASTER__] = masterList
	slave1List = []
	slave1List.append(slave1_instance_id)
	metadata[__SLAVE1__] = slave1List
	slave2List = []
	slave2List.append(slave2_instance_id)
	metadata[__SLAVE2__] = slave2List
	print metadata
	
def start_hadoop_instances_v2():
	#print 'in v2 version'
	# first get the slave1 instance
	slave1_instance = get_instance(metadata[__SLAVE1__][__ID__])
	if slave1_instance.state == 'stopped':
		start_instance_v2(slave1_instance,__SLAVE1__)
	# next get the slave2 instance
	slave2_instance = get_instance(metadata[__SLAVE2__][__ID__])
	if slave2_instance.state == 'stopped':
		start_instance_v2(slave2_instance,__SLAVE2__)
	# next get the master instance
	master_instance = get_instance(metadata[__MASTER__][__ID__])
	if master_instance.state == 'stopped':
		start_instance_v2(master_instance,__MASTER__)
	#print 'all instances are started'
		
def start_hadoop_instances():
	#print 'In start hadoop instances'
	conn = boto.ec2.connect_to_region(region_id)
	#print conn
	#filters={'instance-state-name' : 'running'}
	instance_list.append(master_instance_id)
	instance_list.append(slave1_instance_id)
	instance_list.append(slave2_instance_id)
	#print instance_list
	reservations = conn.get_all_instances(instance_list)
	#print reservations
	for r in reservations:
		#print r
		#print r.instances
		#print "in outer loop", instances
		for inst in r.instances:
			#print inst, inst.id, inst.state
			inst_type = ""
			#print "in inner loop", instances
			if inst.id == master_instance_id:
				inst_type = __MASTER__
				#print 'master found'
			elif inst.id == slave1_instance_id:
				inst_type = __SLAVE1__
				#print 'slave1 found'
			elif inst.id == slave2_instance_id:
				inst_type = __SLAVE2__
				#print 'slave2 found'
			instances[inst_type] = inst
			#print "at end of inner loop iteration", instances
    		
    	# now we should be completely populated with the instances	
    	print instances
    	# now start up the slave 1 and slave 2 instances
    	"""
    	instances['slave1'].start()
    	instances['slave2'].start()
    	print instances['slave1'].id, instances['slave1'].state
    	print instances['slave2'].id, instances['slave2'].state
    	"""
    	start_instance(instances[__SLAVE1__])
    	start_instance(instances[__SLAVE2__])
	start_instance(instances[__MASTER__])

def start_instance_v2(instance, inst_type = __MASTER__, region_name='us-west-1'):
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
		#print region_name
		conn = boto.ec2.connect_to_region(region_name)
		#print conn
		reservations = conn.get_all_instances()
		#print reservations
		for r in reservations:
			#print r.instances
			for inst in r.instances:
				if inst.id == instance.id:
					print '\tInstance %s is %s' % (instance.id,inst.state)
					if inst.state == 'running':
						started = True
					#else:
					#	print 'Not matched instance id: ', inst.id
	print '\t=============================================================='

def get_ip_address(region_name=region_id):
	print 'in get_ip_address'
	
	# cached instances doesn't work so try the long route
	print region_name
	conn = boto.ec2.connect_to_region(region_name)
	print conn
	reservations = conn.get_all_instances(instance_list)
	print reservations
	for r in reservations:
		print r.instances
		for inst in r.instances:
			if inst.id == master_instance_id:
				print '\tInstance %s is %s with IP: %s' % (inst.id,inst.state,inst.ip_address)
				instances['master-ip'] = inst.ip_address
			elif inst.id == slave1_instance_id:
				print '\tInstance %s is %s with IP: %s' % (inst.id,inst.state,inst.ip_address)
				instances['slave1-ip'] = inst.ip_address
			elif inst.id == slave2_instance_id:
				print '\tInstance %s is %s with IP: %s' % (inst.id,inst.state,inst.ip_address)
				instances['slave2-ip'] = inst.ip_address

def get_instance(instance_id, region_name=region_id):
	print 'in get_instance: params %s %s' % (instance_id,region_name)
	
	# cached instances doesn't work so try the long route
	print region_name
	conn = boto.ec2.connect_to_region(region_name)
	print conn
	reservations = conn.get_all_instances(instance_id)
	print reservations
	for r in reservations:
		print r.instances
		for inst in r.instances:
			print '\tInstance %s is %s with IP: %s' % (inst.id,inst.state,inst.ip_address)
			return inst


if __name__ == '__main__':
	# Step 0: Setup the metadata
	setup_metadata()
	# Step 1: Start all M/S instances
    	start_hadoop_instances_v2()
    	# Step 2: Get the Public IP of master, and slaves
    	#print 'Next step: get IPs'
    	#get_ip_address()
    	#print 'Test getting of instance'
    	#get_instance(master_instance_id)
    
