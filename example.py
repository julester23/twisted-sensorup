#!/usr/bin/python

import sensorup
import logging
from twisted.internet import defer, task, reactor

class fake_sensor(object):
	def get_temp_channel(args):
		return 25
	def get_flow(args):
		return 0

def main():

	logging.basicConfig(level=logging.DEBUG, format="%(asctime)-15s %(message)s")
	logging.info('Starting up')

	#sensor_pollers, buffer_unloaders, sqlite_unloaders are worker lists
	sensor_pollers, buffer_unloaders, sqlite_unloaders = [], [], []
	upload_tasks = defer.DeferredQueue()

	#Setup monitoring of fake sensors

	#Create sensor instance in sensor list
	fake_sensors = [fake_sensor()]
	#Iterate over sensors
	for source in fake_sensors:
		name = 'temp%d' % (1,)
		#Create a datapointBuffer which will call source_method (with optional source_method_args) to retrieve a datapoint)
		dp_buffer = sensorup.datapointBuffer(source_method=source.get_temp_channel,
										source_method_args=0,
										datastream_id=name)
		#Add this new datasource (datapointBuffer instance) to worker lists

		#sensorPollAndBuffer: poll for new data and store in an in-memory list
		sensor_pollers.append(task.LoopingCall(sensorup.sensorPollAndBuffer, dp_buffer))
		#bufferUnloadAndUpload: Flushes in-memory list of datapoints, and attempts uploading to Cosm. Failures go into sqlite.
		buffer_unloaders.append(task.LoopingCall(sensorup.bufferUnloadAndUpload, dp_buffer, upload_tasks))
	
	#Collect datapoint every 1s
	for poller in sensor_pollers:
		poller.start(1.0)
	#Upload every 30s
	for poller in buffer_unloaders:
		poller.start(30.0, False)

	#interval for sqlite_unloaders should be sufficiently large as to prevent double uploading
	# sqlite_unloader interval > buffer_unloaders interval
	# sqlite_unloader interval > client.request_timeout * sensor count

	#Flush old datapoints from the database every 86400s
	sqlite_purger = task.LoopingCall(sensorup.sqlite_purge)
	sqlite_purger.start(86400)

	#Only one concurrent upload task 
	task.cooperate(sensorup.worker(upload_tasks))
	reactor.run()
	

if __name__ == "__main__":
	main()

# vim: set ts=4 sw=4 noexpandtab syntax=on
