"""
This script generates a field containing a clean version of the RTF markup text in FreeText.Text
@author:gislipals
"""

import pandas as pd
from urdar_dev import backend as db
from decouple import config

###########
# Login
###########
pg_dbname = 'urdar_overseer'
password = config('PASS')
con, engine = db.urdar_login(pg_dbname,password)
# Create list of databases
dfdbases = pd.read_sql('select site_archive from public.intrasis_archive_tasks where rtf_fixed is null',con)
dbases_all = dfdbases['site_archive'].tolist()
# Run the regex_replace and replace functions.
for dbase in dbases_all:
        con, eng = db.urdar_login(dbase,password)
        print(f'Converting RTF text in database {dbase}')
        sql =r"""
				UPDATE "FreeText" SET converted_text = replace(replace(replace(replace(replace(replace(
                array_to_string((regexp_match("Text",'(?<=\\fs1\d+).*')),','),
                '\''e4','ä'),
                '\''f6','ö'),
                '\''e5','å'),
                '\''c5','Å'),
                '\''c4','Ä'),
                '\''d6','Ö');
			
                UPDATE "FreeText" SET converted_text = replace(converted_text,'}','');
                UPDATE "FreeText" SET converted_text = replace(converted_text,' }','');
                UPDATE "FreeText" SET converted_text = replace(converted_text,'\cf0','');
	            UPDATE "FreeText" SET converted_text = replace(converted_text,'\pard','');
                UPDATE "FreeText" SET converted_text = replace(converted_text,'\par','');
                UPDATE "FreeText" SET converted_text = replace(converted_text,'\fs17','');
                UPDATE "FreeText" SET converted_text = replace(converted_text,'\f0','');
                UPDATE "FreeText" SET converted_text = replace(converted_text,'\f2','');
                UPDATE "FreeText" SET converted_text = replace(converted_text,'\plain','');
                UPDATE "FreeText" SET converted_text = replace(converted_text,'0\','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\cf2','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\bf1','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\fs2tab','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\cf1','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\fs16','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\fs22','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\nowidctlpar','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\sb109','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\tx616','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\tx2255','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\tqr','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\tx4185','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\tx6462','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\fs2tab','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\f1','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\cf3','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\fs16','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\b','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\b0','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\fs20 ','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\sb137','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\sb139','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\sb124','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\tx227tqr','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\tab','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,' \tx617','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\tx1589','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\sb94','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\tx6477','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,' \super','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\nosupersub','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\endash','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\ul','');
				UPDATE "FreeText" SET converted_text = replace(converted_text,'\fs18','');
	        	UPDATE "FreeText" SET converted_text = regexp_replace(converted_text, '\s+$', '');
				UPDATE "FreeText" SET converted_text = regexp_replace(converted_text, '^ ', '');
				UPDATE "FreeText" SET converted_text = regexp_replace(converted_text, '\s+d ', '');		
                """    
        with con.cursor() as cur:
                cur.execute(sql)
        con, engine = db.urdar_login(pg_dbname,password)
        with con.cursor() as cur:
                cur.execute("UPDATE public.intrasis_archive_tasks SET rtf_fixed = True where site_archive like '{}'".format(dbase))