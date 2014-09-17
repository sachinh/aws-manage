import boto
import boto.ec2
from time import sleep

def print_running_instances(running_instances):
    print 'The following running instances were found'
    for account_name in running_instances:
        print '\tAccount: %s' % account_name
        d = running_instances[account_name]
        for region_name in d:
            print '\t\tRegion: %s' % region_name
            for instance in d[region_name]:
                print '\t\t\tAn %s instance: %s' % (instance.instance_type,
                                                    instance.id)
		print '\t\t\t\tInstance is %s' % instance.state
		print '\t\t\t\tTags=%s' % instance.tags

def stop_running_instances(running_instances):
    print 'Kick Starting the stopping of instances'

    for account_name in running_instances:
        print '\tAccount: %s' % account_name
        d = running_instances[account_name]
        for region_name in d:
            print '\t\tRegion: %s' % region_name
            for instance in d[region_name]:
                print '\t\t\tStopping instance: %s' % instance.id
		print '\t\t\t=============================================================='
		instance.stop()
		stopped = False
		while stopped == False:
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
						print '\t\t\tInstance %s is %s' % (instance.id,inst.state)
						if inst.state == 'stopped':
							stopped = True
					#else:
						#print 'Not matched instance id: ', inst.id
		print '\t\t\t=============================================================='


def find_all_running_instances(accounts=None, quiet=False):
    """
    Will find all running instances across all EC2 regions for all of the
    accounts supplied.

    :type accounts: dict
    :param accounts: A dictionary contain account information.  The key is
                     a string identifying the account (e.g. "dev") and the
                     value is a tuple or list containing the access key
                     and secret key, in that order.
                     If this value is None, the credentials in the boto
                     config will be used.
    """
    if not accounts:
        creds = (boto.config.get('Credentials', 'aws_access_key_id'),
                 boto.config.get('Credentials', 'aws_secret_access_key'))
        print creds
        accounts = {'main' : creds}
    running_instances = {}
    for account_name in accounts:
        running_instances[account_name] = {}
        ak, sk = accounts[account_name]
        print ak
        print sk
        for region in boto.ec2.regions():
        	if (region.name[:2] == 'us') and (not region.name[3:6] == 'gov'):
				conn = region.connect(aws_access_key_id=ak,
                                  aws_secret_access_key=sk)
				print 'conn= ', conn
				filters={'instance-state-name' : 'running'}
				#filters={}
				instances = []
				reservations = conn.get_all_instances(filters=filters)
				for r in reservations:
					instances += r.instances
				if instances:
					running_instances[account_name][region.name] = instances
    if not quiet:
        print_running_instances(running_instances)
	print '----------------------------------'
        print 'Now stopping all running instances'
	print '----------------------------------'
        stop_running_instances(running_instances)
    return running_instances
    
if __name__ == '__main__':
    find_all_running_instances()
