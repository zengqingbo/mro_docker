#from __future__ import absolute_import, unicode_literals
import os , time , shutil , gzip  , psycopg2 , csv
from zipfile import ZipFile , BadZipfile
from multiprocessing.pool import Pool as Pool
import billiard 
from multiprocessing.util import Finalize
from io import BytesIO as StringIO
from datetime import datetime

from mroparser5 import mro 
from mroCounter import mroCounter
from tinyCsvMerge import mergefile_all , MERG_DIR
from celery import Celery

DEBUG_LEVEL = 0
CSV_DIR = './csv/'
ZIP_DIR = './zip/'
#SP_ENB_CSV='./sp_enb.csv'
sp_enb_list = []
PG_CONN_TEXT = 'host=10.25.226.2 dbname=qc user=postgres password=r00t'


def getZipobj(filename):
	t1 = time.time()
	try:
		zipobj = ZipFile(filename,'r')
	except BadZipfile as e:
		print('zipfile.BadZipfile' , e )
		zipobj = None
	return zipobj

def getsingleobj(filename):
	file_ext = os.path.splitext(filename)[1].lower()
	if file_ext == '.zip' :
		zipobj = getZipobj(filename)
		siglefilename = zipobj.namelist()[0]
		fobj = StringIO(zipobj.read(siglefilename))
		setattr(fobj,'filename',siglefilename)
	elif file_ext == '.gz' :
		fobj = gzip.open(filename)
		#setattr(fobj,'filename',os.path.basename(filename))
	return fobj
		

def getSubFobj(zipobj,subname):
	if os.path.splitext(subname)[1].lower() == '.zip' :
		fzip = ZipFile(StringIO(zipobj.read(subname)))
		try:
			fobj = StringIO(fzip.read(fzip.namelist()[0]))
		except : 
			return None
		setattr(fobj,'filename',subname)			
	elif os.path.splitext(subname)[1].lower() == '.gz' :
		gz_fobj = StringIO(zipobj.read(subname))
		try : 
			fobj = StringIO(gzip.GzipFile(filename= subname , mode = 'rb', fileobj = gz_fobj).read())
		except : 
			return None
		setattr(fobj,'filename',subname)
	else :
		return None
	return fobj

def handleSub(fobj,nid):
	if DEBUG_LEVEL >= 1 : print(fobj.filename)
	m = mro(fobj)
	c = mroCounter(m)
	result=[]
	fn_sccounter = c.to_csv_sccounter(CSV_DIR);result.append(fn_sccounter)
	fn_nccounter = c.to_csv_nccounter(CSV_DIR);result.append(fn_nccounter)
	fn_cmpcounter = c.to_csv_cmpcounter(CSV_DIR);result.append(fn_cmpcounter)
	fn_nccmpcounter= c.to_csv_nccmpcounter(CSV_DIR);result.append(fn_nccmpcounter)
	fn_freqcounter = c.to_csv_freqcounter(CSV_DIR);result.append(fn_freqcounter)
	if DEBUG_LEVEL >= 1 : print(sp_enb_list)
	if m.EnodebId in sp_enb_list  : scfilename = m.toCsvScInfo(CSV_DIR);result.append(scfilename)
	#ncfilename = m.toCsvNcInfo(CSV_DIR)
	fobj.close()
	del(c)
	del(m)
	if DEBUG_LEVEL >= 1 : print(result)
	return result

_finalizers = []

def poolHandle(zip,nid):
	if DEBUG_LEVEL ==0 : 	
		p = Pool(80)
		for sub in zip.namelist():
			fobj = getSubFobj(zip,sub)
			if fobj != None : p.apply_async(handleSub,args=(fobj,nid))
		p.close()  
		p.join()
	elif DEBUG_LEVEL ==1 :
		p = billiard.Pool()
		_finalizers.append(Finalize(p, p.terminate))
		try:
			p.map_async(handleSub, [(getSubFobj(zip,sub),nid) for sub in zip.namelist()])
			p.close()
			p.join()
		finally:
			p.terminate()
	else :
		for sub in zip.namelist():
			fobj = getSubFobj(zip,sub)
			if fobj != None : handleSub(fobj,nid)
	zip.close()

def todb(filelist,nid):
	conn = psycopg2.connect(PG_CONN_TEXT)
	cur = conn.cursor()
	for filename in filelist:
		if filetype(filename) == 'cmpcounter' :
			cur.execute("copy cmpcounter from '{}' WITH DELIMITER AS ',' NULL AS 'NIL' CSV;".format(filename) )
			conn.commit()
		if filetype(filename) == 'nccmpcounter' :
			cur.execute("copy nccmpcounter from '{}' WITH DELIMITER AS ',' NULL AS 'NIL' CSV;".format(filename) )
			conn.commit()
	cur.close()
	conn.close()
	os.remove(filename)

def filetype(filename):
	return os.path.splitext(os.path.splitext(filename)[0])[1].replace('.','').replace('.','')

def mvtonfs(nfsfile,mergefilelist):
	filelist = []
	for filename in mergefilelist:
		d, fn = os.path.split(nfsfile)
		d, nid = os.path.split(d)
		d , date = os.path.split(d)
		targetfile = shutil.move(filename,os.path.join(d,'{}_{}_{}'.format(date,nid,os.path.basename(filename))))
		os.chmod(targetfile,0o664)
		filelist.append(targetfile)
	return filelist	

def handleAll(filename,nid):
	init()
	if DEBUG_LEVEL != 0 :
		tmpfile = shutil.copy(filename,ZIP_DIR)
	else:
		tmpfile = shutil.move(filename,ZIP_DIR)
	myzip = getZipobj(tmpfile)
	if myzip != None : 
		poolHandle(myzip,nid)
	del(myzip)
	os.remove(tmpfile)
	mergefilelist = mergefile_all(filename)
	targetfilelist = mvtonfs(filename,mergefilelist)
	return targetfilelist

def handleSingle(filename,nid):
	if DEBUG_LEVEL != 0 :
		tmpfile = shutil.copy(filename,ZIP_DIR)
	else:
		tmpfile = shutil.move(filename,ZIP_DIR)
	myzip = getsingleobj(tmpfile)
	if myzip != None : 
		filelist = handleSub(myzip,nid)
	del(myzip)
	os.remove(tmpfile)
	targetfilelist = mvtonfs(filename,filelist)
	return targetfilelist

def get_sp_enb():
	conn = psycopg2.connect(PG_CONN_TEXT)
	cur = conn.cursor()	
	cur.execute("select enbid from enb_sp;")
	rs = cur.fetchall()
	cur.close()
	conn.close()
	return [str(r[0]) for r in rs]


def init():
	tg_dir = [ZIP_DIR, CSV_DIR, MERG_DIR ]
	for d in tg_dir:
		for root, dirs, files in os.walk(d):
			for f in files:
				os.remove(os.path.join(root,f))
	global sp_enb_list 
	sp_enb_list = get_sp_enb()
	#print(sp_enb_list)
	


if __name__ == '__main__':
	t1 = datetime.now()
	handleSingle('./TD-LTE_MRO_ERICSSON_OMC1_453308_20180321174500.xml.zip',1)
	print('used time : {}'.format(datetime.now()-t1))



