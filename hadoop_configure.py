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

master_instance_id = 'i-33fefd6f'
slave1_instance_id = 'i-42fffc1e'
slave2_instance_id = 'i-71fdfe2d'
region_id = 'us-west-1'

def start_hadoop_instances():
	#print 'In start hadoop instances'
	conn = boto.ec2.connect_to_region(region_id)
	#print conn
	#filters={'instance-state-name' : 'running'}
	instance_list = []
	instances = {}
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
				inst_type = 'master'
				#print 'master found'
			elif inst.id == slave1_instance_id:
				inst_type = 'slave1'
				#print 'slave1 found'
			elif inst.id == slave2_instance_id:
				inst_type = 'slave2'
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
    	start_instance(instances['slave1'])
    	start_instance(instances['slave2'])
    	
def start_instance(instance, region_name='us-west-1'):
	# idea is to start the instance and then wait till the instance is completely started
    	print '\tStarting the instance %s' % instance.id

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

if __name__ == '__main__':
    start_hadoop_instances()
 
 
