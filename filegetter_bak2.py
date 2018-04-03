#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ftpext
import csv
import re
import time
import os , shutil
from multiprocessing import Pool
from datetime import date, datetime, timedelta
from tasks import handleBigZipfile


#861/8611/MRO/20170909/TD-LTE_MRO_HUAWEI_010031151066-0100250890
#761/7610/MRO/2017-09-08/TD-LTE_MRO_ZTE_OMC1_20170908000000.zip
#20361/203611/MRO/2017-09-08/TD-LTE_MRO_ZTE_OMC1_20170908000000.zip


DEBUGLEVEL = 0
CUR_DIR = os.path.split(os.path.realpath(__file__))[0]
CP_DIR = '/gpdir/mro'

BAKDIR = '/databak/hubei/LTE-MR/201709/'
VENDERDIR={
	'HUAWEI':['861','862','863','864','865'],
	'ZTE':['761','762','763'],
	'POTEVO':['20361'],
}

VALIDDATE = [datetime(year=2017,month=9,day=7),
			datetime(year=2017,month=9,day=8),
			datetime(year=2017,month=9,day=9),
			]

subfoldernor = {
	'HUAWEI' : {'dateDirformat':'%Y%m%d','layer':1,'ext':'.zip'},
	'ZTE' : {'dateDirformat':'%Y-%m-%d','layer':1,'ext':'.zip'},
	'POTEVO' : {'dateDirformat':'%Y-%m-%d','layer':1,'ext':'.zip'},
	'ERICSSON' : {'dateDirformat':'%Y%m%d','layer':2,'ext':'.zip'},
	'NSN' : {'dateDirformat':'%Y%m%d','layer':2,'ext':'.gz'},
	'DATANG' : {'dateDirformat':'%Y-%m-%d','layer':2,'ext':'.gz'}
	}

def getVenderDate(vender):
	return [d.strftime(subfoldernor[vender]['dateDirformat']) for d in VALIDDATE]

def cpmrofile(filename,dt,nid):
	localdir = os.path.join(CP_DIR,dt.strftime('%Y%m%d'))
	if not os.path.isdir(localdir) : os.mkdir(localdir)
	localdir = os.path.join(localdir,nid)
	if not os.path.isdir(localdir) : os.mkdir(localdir)
	shutil.copy(filename,localdir)
	return os.path.join(localdir,os.path.basename(filename))


def fetchomc(vender,omc):
	validdates = getVenderDate(vender)
	for vd in validdates:
		d_omc = os.path.join(BAKDIR,omc)
		if os.path.isdir(d_omc):	
			for d in os.listdir(d_omc):
				nid = d
				d_ni = os.path.join(d_omc,d)
				if os.path.isdir(d_ni):
					d_mro = os.path.join(d_ni,'MRO')
					for d in os.listdir(d_mro):
						d_date = os.path.join(d_mro,d)
						if os.path.isdir(d_date) and d == validdate :
							dt = datetime.strptime(d,subfoldernor[vender]['dateDirformat'])
							for f in os.listdir(d_date):
								mro = os.path.join(d_date,f)
								if os.path.isfile(mro):
									if DEBUGLEVEL == 0 : 
										filename = cpmrofile(mro,dt,nid)
										handleBigZipfile.delay(filename,nid)
										print(mro,dt,nid) 
									else:
										print(mro,dt,nid) 

def getterfile():
	for vender , omclist in VENDERDIR.items():
		for omc in omclist :
			fetchomc(vender,omc)


if __name__ == '__main__':
	getterfile()
	