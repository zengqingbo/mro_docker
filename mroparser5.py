#!/usr/bin/env python
# -*- coding: utf-8 -*-
import xml.etree.cElementTree as ET
import sys
import os
import random
import gzip
import zipfile as zipfile
from time import time
from multiprocessing import Pool
from math import *
from mroCounter import mroCounter
from datetime import datetime , timedelta

_NcField = ['LteNcRSRP',
			'LteNcRSRQ',
			'LteNcEarfcn',
			'LteNcPci',
			'GsmNcellBcch',
			'GsmNcellNcc',
			'GsmNcellBcc',
			'GsmNcellCarrierRSSI',
			'TdsNcellUarfcn',
			'TdsCellParameterId',
			'TdsPccpchRSCP']
_GridSize = 100   # Unit:Meter

_ScInfo_Format = [	'LteScRSRP',
					'LteScRSRQ',
					'LteScTadv',
					'LteScAOA',
					'LteScPHR',
					'LteScSinrUL',
					'LteScRI1',
					'LteScRI2',
					'LteScRI4',
					'LteScRI8',
					'LteScPlrULQci1',
					'LteScPlrDLQci1',
					]

_NcInfo_Format = [	'LteNcEarfcn',
					'LteNcPci',
					'LteNcRSRP',
					'GsmNcellBcch',
					'GsmNcellNcc',
					'GsmNcellBcc',
					'GsmNcellCarrierRSSI'
					]

class sample():
	def __init__(self,MmeUeS1apId,TimeStamp = '2016-06-19T00:00:00.000',id = '', Ci = '',\
		 x = -1, y = -1, gridX = -1, gridY = -1 ,\
		ScInfo = {}, NcInfo_list = [], maxGsmRssi = -1):
		self.MmeUeS1apId = MmeUeS1apId
		self.TimeStamp = TimeStamp
		self.id = id
		self.Ci = Ci
		self.x = x
		self.y = y
		self.gridX = gridX
		self.gridY = gridY
		self.ScInfo = ScInfo
		self.NcInfo_list = NcInfo_list
		self.maxGsmRssi = maxGsmRssi
		

class mro():
	def __init__(self,fobj):
		self.filename = fobj.filename
		self.x = -1
		self.y = -1
		self.z = -1
		self.EnodebId = os.path.basename(self.filename).split('_')[4]
		scInfoCheck = False
		smrId = 0
		self.samples = {}
		f = fobj
		for event , elem in ET.iterparse(f):
			if event == 'end':
				if elem.tag == 'fileHeader' :
					self.startTime = elem.attrib['startTime'].replace('T',' ')
					self.endTime = elem.attrib['endTime'].replace('T',' ')
				#if elem.tag == 'eNB':
					#print elem.attrib['id']
					#self.EnodebId = elem.attrib['id']
				if elem.tag == 'smr':
					l =  elem.text.strip().replace('MR.','').split(' ')
					smr = {}
					for i in range(len(l)): smr.setdefault(l[i],i)
					self.smr_scinfo = [mri for mri in l if mri not in _NcField ]
					self.smr_ncinfo = [mri for mri in l if mri in _NcField]
					if 'LteScRSRP' in l : smrId = 1
					else : smrId = 2
				if smrId==1 and elem.tag == 'v':
					v = elem.text.strip().split(' ')
					if scInfoCheck == False :
						ScInfo = {}
						for mri in self.smr_scinfo:
							ScInfo[mri] = v[smr[mri]]
						NcInfo_list = []
						scInfoCheck = True
					NcInfo = {}
					for mri in self.smr_ncinfo:
						NcInfo[mri] = v[smr[mri]]
					NcInfo_list.append(NcInfo)
				if smrId==1 and elem.tag == 'object' :
					MmeUeS1apId = elem.attrib['MmeUeS1apId']
					TimeStamp = elem.attrib['TimeStamp'].replace('T',' ')
					id = elem.attrib['id']
					Ci = int(id)%256
					#print MmeUeS1apId
					maxGsmRssi = self._getMaxGsmRssi(NcInfo_list)
					key = self._genKey(MmeUeS1apId,TimeStamp,id) 
					self.samples.setdefault( key \
						,sample(MmeUeS1apId=MmeUeS1apId,TimeStamp=TimeStamp,id = id\
								,Ci=Ci,ScInfo=ScInfo,NcInfo_list = NcInfo_list\
								,maxGsmRssi = maxGsmRssi) ) 
					scInfoCheck = False
				if smrId==2 and elem.tag == 'v'	:
					v_value = elem.text.strip().split(' ')
					ScInfo = {}
					for k,v in smr.items():
						ScInfo[k] = v_value[v]
				if smrId==2 and elem.tag == 'object':
					MmeUeS1apId = elem.attrib['MmeUeS1apId']
					TimeStamp = elem.attrib['TimeStamp'].replace('T',' ')
					id = elem.attrib['id']
					key = self._genKey(MmeUeS1apId,TimeStamp,id)
					if key in self.samples : self.samples[key].ScInfo.update(ScInfo)
				#print elem.tag,elem.text
				elem.clear()
		f.close()

	def _genKey(self,MmeUeS1apId,TimeStamp,id):
		return 'MmeUeS1apId="%s" TimeStamp="%s" id="%s"' % (MmeUeS1apId,TimeStamp,id)

	def _getMaxGsmRssi(self,NcInfo_list):
		maxGsmRssi = -1
		GsmNcellCarrierRSSI = 'NIL'
		for nc in NcInfo_list:
			if 'GsmNcellCarrierRSSI' in nc  : GsmNcellCarrierRSSI = nc['GsmNcellCarrierRSSI']
			if GsmNcellCarrierRSSI !=  'NIL' and int(GsmNcellCarrierRSSI) > maxGsmRssi:
				maxGsmRssi = int(GsmNcellCarrierRSSI)
		if maxGsmRssi == -1 :
			maxGsmRssi = 'NIL'
		else :
			maxGsmRssi = str(maxGsmRssi)
		return maxGsmRssi	


	def toDB(self):
		pass

	def unzip(self,filename):
		ext = os.path.splitext(filename)[1]
		if ext == '.gz':
			return gzip.open(filename)
		elif ext == '.zip':
			z = zipfile.ZipFile(filename,'r')
			return z.open(z.namelist()[0])
		elif ext == '.xml':
			return open(filename,'r')
		print('unzip error:',filename )

	def taAoaLocation(self,x,y,z,g):
		self.x,self.y,self.z = x,y,z
		for k in self.samples:
			(self.samples[k].x,self.samples[k].y) = \
				self._taAoaLocation(x,y,self.samples[k].ScInfo['LteScTadv']\
				,self.samples[k].ScInfo['LteScAOA'],self.samples[k].Ci,g)


	def _taAoaLocation(self,x,y,LteScTadv,LteScAOA,Ci,g):
		#ts = 1.0/(15000.0*2048.0)
		#c = 299792458.0
		#c_ta = c*ts/2*16
		#此处x,y必需使用西安坐标系
		c_ta = 78.07095260416666
		loc_x = None
		loc_y = None
		if str(Ci) not in g:
			jiaodu=0
		else:
			jiaodu=g[str(Ci)]
		if x!=None and LteScTadv != 'NIL' and LteScAOA != 'NIL':
			loc_x = x+c_ta*(int(LteScTadv)+ random.uniform(-0.5,0.5))*sin(radians(int(LteScAOA)/2+jiaodu+random.uniform(-0.25,0.25)))
			loc_y = y+c_ta*(int(LteScTadv)+ random.uniform(-0.5,0.5))*cos(radians(int(LteScAOA)/2+jiaodu+random.uniform(-0.25,0.25)))
		return loc_x, loc_y
	
	def genGridXY(self,cityX,cityY, GridSize = _GridSize):
		for k in self.samples:
			self.samples[k].gridX,self.samples[k].gridY = \
				self._genGridXY(cityX,cityY,self.samples[k].x,self.samples[k].y,GridSize)

	def _genGridXY(self,cityX,cityY,x,y,GridSize = _GridSize):
		if x == None or y == None: return (None,None)
		return int((x-cityX)/GridSize),int((y-cityY)/GridSize)

	def toCsvScInfo(self,dir = ''):
		if dir == '' : 
			filename = self.filename + '.scinfo'
		else :
			filename = os.path.join(dir,os.path.basename(self.filename)+ '.scinfo')
		pdatestr = self.startTime[:self.startTime.find('T')]
		pdatestr = pdatestr[:pdatestr.find(' ')]
		print(pdatestr)
		pdate = datetime.strptime(pdatestr,'%Y-%m-%d')
		if pdate + timedelta(days=93) > datetime.now() :
			with open(filename,'w') as f:			
				for k ,v in self.samples.items():
					line = "%s,%s,%s,%s,%s,%s," % (pdatestr,v.TimeStamp,v.MmeUeS1apId,self.EnodebId, v.id,v.maxGsmRssi)
					line = line + ','.join([v.ScInfo[mri] \
						if mri in v.ScInfo else 'NIL' for mri in _ScInfo_Format])				
					f.write(line+'\n')
			if os.path.isfile(filename):
				os.rename(filename,filename+'.csv')
				filename=filename+'.csv'
			return filename
		else :
			return None

	def toCsvNcInfo(self,dir = ''):
		if dir == '' : 
			filename = self.filename + '.ncinfo'
		else :
			filename = os.path.join(dir,os.path.basename(self.filename)+ '.ncinfo')
		pdatestr = self.startTime[:self.startTime.find('T')]
		pdatestr = pdatestr[:pdatestr.find(' ')]
		print(pdatestr)
		pdate = datetime.strptime(pdatestr,'%Y-%m-%d')
		if pdate + timedelta(days=93) > datetime.now() :
			with open(filename,'w') as f:
				for k ,v in self.samples.items():
					for NcInfo in v.NcInfo_list:
						line = "%s,%s,%s,%s,%s," % (pdatestr,v.TimeStamp,v.MmeUeS1apId,self.EnodebId,v.id)
						line = line + ','.join([NcInfo[mri] \
							if mri in NcInfo else 'NIL' for mri in _NcInfo_Format])
						f.write(line+'\n')
			if os.path.isfile(filename):
				os.rename(filename,filename+'.csv')
				filename=filename+'.csv'
			return filename
		else :
			return None


if __name__ == '__main__' :
	filename = './TD-LTE_MRO_ERICSSON_OMC1_453308_20180321174500.xml'
	fobj = open(filename)
	setattr(fobj,'filename',filename)
	print(fobj.filename)
	m = mro(fobj)
	m.toCsvScInfo('./csv')
	m.toCsvNcInfo('./csv')
	print(len(m.samples))
	fobj.close()
	c = mroCounter(m)
	c.to_csv_nccmpcounter('./csv')
	c.to_csv_cmpcounter('./csv')
	c.to_csv_sccounter('./csv')
	c.to_csv_nccounter('./csv')
	c.to_csv_freqcounter('./csv')
	#siteParser(['e:\\0818\\TD-LTE_MRO_HUAWEI_010031151066_435545_20160518060000.xml'])
 