import uuid
import pandas as pd
import multiprocessing
from functools import partial
import random
import numpy as np
from pathlib import Path


class antiML():

	def __init__(self, transactions):

		self.transactions = transactions
		#self.transactions['TIMESTAMP'] = pd.to_datetime(self.transactions['TIMESTAMP'], format='%Y-%m-%d', errors ='coerce')
		#self.cpu_count = multiprocessing.cpu_count()
		self.unique_ids= list(set(self.transactions['RECEIVER']) | set(self.transactions['SENDER']))
		#self.random_ids = [random.choice(self.unique_ids) for _ in range(0,100)]

	@classmethod
	def from_csv(cls, string, year=None, sep = '|' ):

		temp_pd = pd.read_csv(string, sep = sep)
		temp_pd['TIMESTAMP'] = pd.to_datetime(temp_pd['TIMESTAMP'], format='%Y-%m-%d', errors ='coerce')
		if year != None:

			temp_pd = temp_pd[(pd.Timestamp(year,1,1)<= temp_pd['TIMESTAMP']) & (temp_pd['TIMESTAMP']<=pd.Timestamp(year+1,1,1)+pd.Timedelta(days=7))]
		
		return cls(temp_pd)

	def potential_ID_trans_pd_tuple(self, single_id):
		#print(type(single_id))
		#assert type(single_id) == type('str')

		sending_to_id = self.transactions[self.transactions['RECEIVER']==single_id]
		if len(sending_to_id) == 0:
			return None

		sending_from_id = self.transactions[self.transactions['SENDER']==single_id]
		if len(sending_from_id) == 0:
			return None

		return (single_id, sending_to_id, sending_from_id)

	@staticmethod
	def find_sus_trans_pairs(id_tup):

		if id_tup == None:
			return None

		sus_trans_pairs = []
		#pd.DataFrame(columns = ['FROM','TRANSACTION','BRIDGE','TRANSACTION','TO'])
		for _, in_row in id_tup[1].iterrows():
			for _, out_row in id_tup[2][(id_tup[2]['TIMESTAMP']>=in_row['TIMESTAMP']) & (id_tup[2]['TIMESTAMP']<=in_row['TIMESTAMP']+pd.Timedelta(days=7))].iterrows():
				if ( 0.75*in_row['AMOUNT'] <= out_row['AMOUNT']) & ( out_row['AMOUNT'] <= 0.95*in_row['AMOUNT']):

					sus_trans_pairs.append({'FROM':in_row['SENDER'],'FROM_TRANSACTION':in_row['TRANSACTION'],'BRIDGE':in_row['RECEIVER'],'TO_TRANSACTION':out_row['TRANSACTION'],'TO':out_row['RECEIVER']})
					#sus_trans_pairs.append((in_row['TRANSACTION'], out_row['TRANSACTION']))

		
		if len(sus_trans_pairs) == 0:
			return None

		return pd.DataFrame(sus_trans_pairs)
		#return (id_tup[0], sus_trans_pairs)


	def one_bridge_id_pd(self, single_id):

		return self.find_sus_trans_pairs(self.potential_ID_trans_pd_tuple(single_id))

	def sus_bridges_pd(self, nparr):

		#temp_file_name = str(abs(hash(''.join(list(nparr)))))
		temp_file_name = str(uuid.uuid4().hex)
		#n = 500

		#final = [nparr[i * n:(i + 1) * n] for i in range((len(nparr) + n - 1) // n )]

		

		list_of_sus = [i for i in list(map(self.one_bridge_id_pd, nparr)) if type(i) != type(None)]

		if len(list_of_sus) == 0:
			return None

		pd_csv = pd.concat(list_of_sus)
		'''
		with open(f'{temp_file_name}.csv', 'a') as f:
			pd_csv.to_csv(f, mode = 'a', header = f.tell()==0)
		'''
		file_exists = Path(temp_file_name).exists()
		pd_csv.to_csv(f'temp{temp_file_name}.csv', header=False, mode='a' if file_exists else 'w', index = False, line_terminator='\n')


		
		

		return 1
	@classmethod
	def glob(cls, glob):

		temp_pd = glob.transactions

		return cls(temp_pd)


	def find_bridges(self, arrs):

		pool = multiprocessing.Pool(self.cpu_count)
		split_nparr_unique_ids = np.array_split(arrs, self.cpu_count)
		result = pool.map(self.sus_bridges_pd, split_nparr_unique_ids)

		return result










