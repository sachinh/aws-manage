import boto
import boto.ec2
from time import sleep
from subprocess import call

# Declare Literals
__MASTER__ = 'master'
__SLAVE1__ = 'slave1'
__SLAVE2__ = 'slave2'
__DELIM__ = '-'
__IP__ = 'ip'
__ID__ = 0
__DNS_INDEX__ = 2
__SLAVE__ = 'slave'
__MAPRED__ = 'mapred'
__YARN__ = 'yarn'
__CORE__ = 'core'

# Declare Constants - temp fix - make more generic later
master_instance_id = 'i-33fefd6f'
slave1_instance_id = 'i-42fffc1e'
slave2_instance_id = 'i-71fdfe2d'
region_id = 'us-west-1'

# Declare Global vars
metadata = {}
"""
the metadata dict contains a list of items in each entry (key/value)
index 0 -> instance_id
index 1 -> public ip address
index 2 -> public dns address
"""

def setup_metadata():
	# this is really about setting up the metadata dict
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
		#print instance.public_dns_name
		metadata[key].append(instance.public_dns_name)
		
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

def save_config(file_type):
	print 'in save config with param %s' % file_type
	if file_type == __MASTER__:
		# now setup the path vars for the master file config
		template_file_name = "templates/masters.template"
		out_file_name = "config-files/masters"
	elif file_type == __SLAVE__:
		# now setup the path vars for the master file config
		template_file_name = "templates/slaves.template"
		out_file_name = "config-files/slaves"
	elif file_type == __MAPRED__:
		# now setup the path vars for the master file config
		template_file_name = "templates/mapred-site.xml.template"
		out_file_name = "config-files/mapred-site.xml"
	elif file_type == __YARN__:
		# now setup the path vars for the master file config
		template_file_name = "templates/yarn-site.xml.template"
		out_file_name = "config-files/yarn-site.xml"
	elif file_type == __CORE__:
		# now setup the path vars for the master file config
		template_file_name = "templates/core-site.xml.template"
		out_file_name = "config-files/core-site.xml"
		
	# open template for reading
	template_file = open(template_file_name, "r")
	out_file = open(out_file_name, "w")
	
	lines = template_file.readlines()
	l_no = 1
	for line in lines:
		print 'Line No: %s : Line is: %s' % (l_no,line)
		
		# now tokenize based on MASTER_DNS
		tokens = line.split()
		print tokens, len(tokens)
		out_line = ''
		first_token = True
		for token in tokens:
			if token.find('MASTER-DNS') != -1:
				print 'found a match', metadata[__MASTER__][__DNS_INDEX__]
				# now do a replace since the string may have occurred anywhere
				token = token.replace('MASTER-DNS',metadata[__MASTER__][__DNS_INDEX__])
				#token = metadata[__MASTER__][__DNS_INDEX__]
			elif token.find('SLAVE1-DNS') != -1:
				print 'found a match', metadata[__SLAVE1__][__DNS_INDEX__]
				# now do a replace since the string may have occurred anywhere
				token = token.replace('SLAVE1-DNS',metadata[__SLAVE1__][__DNS_INDEX__])
			elif token.find('SLAVE2-DNS') != -1:
				print 'found a match', metadata[__SLAVE2__][__DNS_INDEX__]
				# now do a replace since the string may have occurred anywhere
				token = token.replace('SLAVE2-DNS',metadata[__SLAVE2__][__DNS_INDEX__])
			# now add to the output line , the token, altered or not
			if first_token:
				out_line = out_line + token
				# reset first_token at end of iteration
				first_token = False
			else:
				out_line = out_line + ' ' + token
		print tokens, len(tokens)
		print 'Output Line: ', out_line
		out_file.write(out_line+'\n')
		
		l_no = l_no+1
	#print lines
	# close the file objects
	template_file.close()
	out_file.close()

	print 'all done here'

if __name__ == '__main__':
	# Step 0: Setup the metadata
	#setup_metadata()
	# Step 1: Start all M/S instances
	# Step 2: Get the Public IP+DNS of master, and slaves --- for efficiency part of same function
    	start_hadoop_instances()
    	print metadata
	# Step 3: Update Config Files with required info
	#save_config(__MASTER__)
	#save_config(__SLAVE__)
	#save_config(__MAPRED__)
	#save_config(__YARN__)
	#save_config(__CORE__)
	# Step 5: Copy files into master and slaves
	call(["ls", "-l", "-a"])

	# Step 6: Start required hadoop processes in master
