#!/usr/bin/python

import datetime
import logging
import sqlite3
import txcosm.HTTPClient
import json
from twisted.internet import defer
#Define API_KEY and FEED_ID in config/cosm_keys.py or simply define here and remove this:
from config.cosm_keys import API_KEY,FEED_ID,SQLITE_FILE




class datapointBuffer(object):
	def __init__(self, source_method=None, source_method_args=None, datastream_id=None):
		self._buffer = []
		self._source_method = source_method
		self._source_method_args = source_method_args
		self.datastream_id = datastream_id

	def store(self, value):
		now = datetime.datetime.utcnow().replace(microsecond=0)
		datapoint = {'at': '%sZ' % now.isoformat(), 'value': str(value)}
		if value != None:
			self._buffer.append(datapoint)
			logging.debug('storing: %s' % datapoint)
			return True
		else:
			logging.debug('ignoring value: %s' % (value,))

	def get_new_data(self):
		if self._source_method:
			if self._source_method_args:
				return self.store(self._source_method(*self._source_method_args))
			else:
				return self.store(self._source_method())
		return False

	def flush(self):
		data = self._buffer
		#Flush the buffer (assigns new empty array to self._buffer, doesn't erase the data)
		self._buffer = []
		return data


class sqlite(object):
	def __init__(self):
		self.conn = sqlite3.connect(SQLITE_FILE, detect_types=sqlite3.PARSE_DECLTYPES)
		sqlite3.register_adapter(datetime.datetime, self._adapt_to_sqlite_ts)
		#sqlite3.register_converter('DATETIME', _self._convert_to_ts)
		self.cursor = self.conn.cursor()
		self.workset_table = None
		self.workset = None
		self.workset_status = 0
		self.create_table = '''CREATE TABLE IF NOT EXISTS %s
				 (id INTEGER PRIMARY KEY, date DATETIME, value TEXT, status INTEGER DEFAULT 0, ntp INTEGER DEFAULT 0)'''

	def _adapt_to_sqlite_ts(self, datetime_object):
		return datetime_object.strftime('%Y-%m-%dT%H:%M:%SZ')

	def _convert_to_ts(self, text): 
		return datetime.datetime.strptime(text, '%Y-%m-%dT%H:%M:%SZ')

	def store(self, datastream_id, data):
		# Create table
		self.cursor.execute(self.create_table % (datastream_id,))
		#logging.info("Creating table %s" % datastream_id)

		# Insert a row of data
		self.cursor.executemany("INSERT INTO %s(date, value) VALUES (?, ?)" % (datastream_id,), [(row['at'], row['value']) for row in data])

		# Save (commit) the changes
		self.conn.commit()

	def purge(self, cutoff_date, datastream_id):
		logging.info('Purging archive data for %s prior to %s' % (datastream_id, cutoff_date.strftime('%Y-%m-%dT%H:%M:%SZ')))
		#Yes, string comparisons work for this format of date
		
		self.cursor.execute('DELETE FROM %s WHERE ntp == 0 OR date < ?' % (datastream_id,), (cutoff_date.strftime('%Y-%m-%dT%H:%M:%SZ'),))
		#Database compaction:
		self.cursor.execute('VACUUM')
		self.conn.commit()

	def fetch_table_names(self):
		self.cursor.execute('SELECT tbl_name FROM sqlite_master')
		return [str(row[0]) for row in self.cursor.fetchall()]

	def purge_all(self, cutoff_date):
		for tbl_name in self.fetch_table_names():
			self.purge(cutoff_date=cutoff_date, datastream_id=tbl_name)


	def fetch_failed_uploads(self, datastream_id):
		self.cursor.execute(self.create_table % (datastream_id,))
		#self.cursor.execute('''CREATE TABLE IF NOT EXISTS %s
		#		 (id INTEGER PRIMARY KEY, date DATETIME, value TEXT, status INTEGER DEFAULT 0, ntp INTEGER DEFAULT 0)''' % (datastream_id,))
		self.conn.commit()
		self.cursor.execute('SELECT id, date, value, status FROM %s WHERE status=0 LIMIT 400' % datastream_id)
		result = self.cursor.fetchall()
		self.workset = [str(row[0]) for row in result]
		self.workset_table = datastream_id
		self.set_workset_status(1)
		return [{'at': row[1], 'value': row[2]} for row in result]

	def set_workset_status(self, status):
		if self.workset and self.workset_table:
			self.cursor.execute('UPDATE %s SET status=%s WHERE id IN (%s)' % (self.workset_table, status, ','.join(self.workset)))
			self.conn.commit()
			logging.debug('SQLITE: updated status to %s for workset' % status)

	def delete_previous_fetch(self):
		if self.workset and self.workset_table:
			self.cursor.execute('DELETE FROM %s WHERE id IN (%s)' % (self.workset_table, ','.join(self.workset)))
			self.conn.commit()
			logging.debug('SQLITE: deleted successfully uploaded datapoints')
	#print row[0].strftime('%Y-%m-%dT%H:%M:%SZ'),row[1:]

	def __del__(self):
		# We can also close the connection if we are done with it.
		# Just be sure any changes have been committed or they will be lost.
		
		#Reset the status column to 0 before forgetting about the workset
		if self.workset_status != 0:
			self.set_workset_status(0)
		self.conn.close()

class uploadTask(object):
	def __init__(self, **kwargs):
		self.__dict__.update(kwargs)


@defer.inlineCallbacks
def uploadOrStore(upload_task):
	data_json = json.dumps({'datapoints': upload_task.data})
	#logging.debug('uploading to %s: %s' % (upload_task.datastream_id, data_json))
	client = txcosm.HTTPClient.HTTPClient(api_key=API_KEY, feed_id=FEED_ID)
	client.request_timeout = 5
	result = yield client.create_datapoints(datastream_id=upload_task.datastream_id, data=data_json)
	if result:
		#upload was successful
		if upload_task.sqlite:
			logging.info('Uploaded from (sqlite) archive %s' % upload_task.datastream_id)
			logging.debug('data: %s' % (data_json))
			#upload was for sqlite, let's purge what we uploaded
			upload_task.sqlite.delete_previous_fetch()
		else:
			logging.info('Uploaded new data from %s' % upload_task.datastream_id)
			logging.debug('data: %s' % (data_json))
		defer.returnValue(True)
	else:
		if upload_task.sqlite:
			logging.warn("Upload failure for archived data from %s" % upload_task.datastream_id)
		else:
			logging.warn("Upload failure for new data from %s" % upload_task.datastream_id)
			#upload was 1st attempt to cloud, store data locally in sqlite
			#logging.info("storing in sqlite %s: %s" % (upload_task.datastream_id,data_json))
			try:
				a = sqlite()
				a.store(upload_task.datastream_id, upload_task.data)
			except Exception, e:
				logging.warn('Archive (sqlite) failure: %s' % e)
		defer.returnValue(False)


@defer.inlineCallbacks
def worker(upload_queue):
	def failure(failure):
		logging.warn("uploadOrStore failure: %s" % (failure.getTraceback(),))
		#failure.trap(Error)
	while True:
		success = yield upload_queue.get().addCallback(uploadOrStore).addErrback(failure)
		#If last upload was successful, add a task to the upload_queue
		if success:
			backloaderAddOneTask(upload_queue)
		#logging.debug('RETURNED: %s' % success)

#Reads from sqlite database (on-disk) and adds a task to the upload_queue for the first backed up sensor data it finds
def backloaderAddOneTask(upload_queue):
	s = sqlite()
	for datastream_id in s.fetch_table_names():
		logging.debug(datastream_id)
		data = s.fetch_failed_uploads(datastream_id)
		if data:
			upload_queue.put(uploadTask(datastream_id=datastream_id, data=data, sqlite=s))
			logging.debug('Backloading %s' % datastream_id)
			#Only add one task at a time - best not to overwhelm current uploads
			break

#def getSqliteDataAndUpload(monitor_instance, upload_queue):
#	s = sqlite()
#	data = s.fetch_failed_uploads(monitor_instance.datastream_id)
#	if data:
#		upload_queue.put(uploadTask(datastream_id=monitor_instance.datastream_id, data=data, sqlite=s))


def sensorPollAndBuffer(monitor_instance):
	monitor_instance.get_new_data()

def bufferUnloadAndUpload(monitor_instance, upload_queue):
	data = monitor_instance.flush()
	if data:
		upload_queue.put(uploadTask(datastream_id=monitor_instance.datastream_id, data=data, sqlite=None))

def sqlite_purge():
	s = sqlite()
	s.purge_all(cutoff_date=(datetime.datetime.utcnow().replace(microsecond=0)-datetime.timedelta(days=10)))

