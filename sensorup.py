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

	def _adapt_to_sqlite_ts(self, datetime_object):
		return datetime_object.strftime('%Y-%m-%dT%H:%M:%SZ')

	def _convert_to_ts(self, text): 
		return datetime.datetime.strptime(text, '%Y-%m-%dT%H:%M:%SZ')

	def store(self, datastream_id, data):
		# Create table
		self.cursor.execute('''CREATE TABLE IF NOT EXISTS %s
				 (id INTEGER PRIMARY KEY, date DATETIME, value TEXT, uploaded INTEGER DEFAULT 0, ntp INTEGER DEFAULT 0)''' % (datastream_id,))
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

	def purge_all(self, cutoff_date):
		self.cursor.execute('SELECT tbl_name FROM sqlite_master')
		for row in self.cursor.fetchall():
			self.purge(cutoff_date=cutoff_date, datastream_id=row[0])


	def fetch_failed_uploads(self, datastream_id):
		self.cursor.execute('''CREATE TABLE IF NOT EXISTS %s
				 (id INTEGER PRIMARY KEY, date DATETIME, value TEXT, uploaded INTEGER DEFAULT 0, ntp INTEGER DEFAULT 0)''' % (datastream_id,))
		self.conn.commit()
		self.cursor.execute('SELECT id, date, value, uploaded FROM %s WHERE uploaded=0 LIMIT 400' % datastream_id)
		result = self.cursor.fetchall()
		self.workset = [str(row[0]) for row in result]
		self.workset_table = datastream_id
		return [{'at': row[1], 'value': row[2]} for row in result]

	def mark_previous_fetch_as_uploaded(self):
		if self.workset and self.workset_table:
			self.cursor.execute('UPDATE %s SET uploaded=1 WHERE id IN (%s)' % (self.workset_table, ','.join(self.workset)))
			self.conn.commit()
			logging.debug('SQLITE: marked successfully uploaded datapoints')

	def delete_previous_fetch(self):
		if self.workset and self.workset_table:
			self.cursor.execute('DELETE FROM %s WHERE id IN (%s)' % (self.workset_table, ','.join(self.workset)))
			self.conn.commit()
			logging.debug('SQLITE: deleted successfully uploaded datapoints')
	#print row[0].strftime('%Y-%m-%dT%H:%M:%SZ'),row[1:]

	def __del__(self):
		# We can also close the connection if we are done with it.
		# Just be sure any changes have been committed or they will be lost.
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
def worker(tasks):
	def failure(failure):
		logging.warn("uploadOrStore failure: %s" % (failure.getTraceback(),))
		#failure.trap(Error)
	while True:
		returnVal = yield tasks.get().addCallback(uploadOrStore).addErrback(failure)
		#returnVal = yield tasks.get().addCallback(uploadOrStore).addErrback(twisted.python.log.err)
		logging.debug('RETURNED: %s' % returnVal)

def getSqliteDataAndUpload(monitor_instance, tasks):
	s = sqlite()
	data = s.fetch_failed_uploads(monitor_instance.datastream_id)
	if data:
		tasks.put(uploadTask(datastream_id=monitor_instance.datastream_id, data=data, sqlite=s))

def sensorPollAndBuffer(monitor_instance):
	monitor_instance.get_new_data()

def bufferUnloadAndUpload(monitor_instance, upload_queue):
	data = monitor_instance.flush()
	if data:
		upload_queue.put(uploadTask(datastream_id=monitor_instance.datastream_id, data=data, sqlite=None))

def sqlite_purge():
	s = sqlite()
	s.purge_all(cutoff_date=(datetime.datetime.utcnow().replace(microsecond=0)-datetime.timedelta(days=30)))

