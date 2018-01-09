import numpy as np
from datetime import datetime


class PerSecPartitionsDerivedValue(object):
	def __init__(self, var, partition_field):
		self.var = var
		self.partition_field = partition_field
		self.last_time = None
		self.last_val = None
		self.history = []

	def add_history(self, time, data):
		self.history.append((time, data))

	def compute_1minavg(self):
		# Remove data older than one minute and compute mean
		now = datetime.now()
		self.history[:] = [(time, data) for (time, data) in self.history 
						   if (now - time).total_seconds() <= 60.0]
		# print('### history={}'.format(self.history))
		latest_data = [data for (time, data) in self.history]
		# print('### lastest_data={}'.format(latest_data))
		return np.mean(latest_data)

	def compute(self, partitions):
		# partitions = {"partition_0": {"tail": 1435, "lag": 48, "commited": 1387}, 
		#               "partition_1": {"tail": 1429, "lag": 0, "commited": 1429}, ...}

		empty_result = (self.var, {'persec': 0.0, 'persec_1minavg': 0.0})

		if partitions is None or partitions == {}:
			return empty_result

		val = sum([partitions[p][self.partition_field] for p in partitions])
		now = datetime.now()
		if self.last_val is None or self.last_time is None or \
		   val < self.last_val or now <= self.last_time:
			self.last_val = val
			self.last_time = now
			return empty_result

		# Compute per-second change per time
		persec = float(val - self.last_val) / (now - self.last_time).total_seconds()
		# print("#### last_val={}, val={}, delta_sec={}".
		# 	  format(self.last_val, val, (now - self.last_time).total_seconds()))
		self.last_val = val
		self.last_time = now
		self.add_history(now, persec)
		persec_1minavg = self.compute_1minavg()

		return (self.var, {'persec': persec, 'persec_1minavg': persec_1minavg})


class MsgsIn(PerSecPartitionsDerivedValue):
	def __init__(self):
		super(MsgsIn, self).__init__('msgsin', 'tail')


class MsgsOut(PerSecPartitionsDerivedValue):
	def __init__(self):
		super(MsgsOut, self).__init__('msgsout', 'commited')


class OffsetLags(object):
	def compute(self, partitions):
		if partitions is None or partitions == {}:
			return ('lags', {'min': 0.0, 'max': 0.0, 'mean': 0.0, 'num': 0})

		lags = [partitions[p]['lag'] for p in partitions]
		stats = {'max': max(lags), 'min': min(lags), 'mean': np.mean(lags), 'num': len(partitions)}

		return ('lags', stats)
	
