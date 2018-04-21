import psycopg2
import datetime
import sys
from fabric.api import local

from tasks import PG_CONN_TEXT

yesterday = datetime.date.today() - datetime.timedelta(days=1)

def hello(date = yesterday):
    if isinstance(date,str) : date = datetime.datetime.strptime(date,'%Y%m%d').date()
    print("Hello Fabric!'{0:%Y-%m-%d}'".format(date))

def insertcmpfiles(date = yesterday,delcase=False):
    if isinstance(date,str) : date = datetime.datetime.strptime(date,'%Y%m%d').date()
    conn = psycopg2.connect(PG_CONN_TEXT)
    with  conn.cursor() as cur:
        cmpcounter_d(cur,date,delcase)
        eutruncelltdd_upperall_d(cur,date,delcase)
        city_country_m(cur,date,delcase)
        city_country_opponent_m(cur,date,delcase)
    conn.commit()
    conn.close()    

def getcmpfiles(date = yesterday,delcase=False):
    if isinstance(date,str) : date = datetime.datetime.strptime(date,'%Y%m%d').date()
    insertcmpfiles(date,delcase)
    cmpfiles=[
        ('city_country_opponent_m','统计级指标（竞对）{0:%Y%m%d}.csv'),
        ('city_country_m','统计级指标（本网）{0:%Y%m%d}.csv'),
        ('eutruncelltdd_upperall_d','小区级上层应用表（含本网与竞对）{0:%Y%m%d}.csv'),
    ]
    conn = psycopg2.connect(PG_CONN_TEXT)
    with  conn.cursor() as cur:
        for tn,fn in cmpfiles:
            with open(fn.format(date)) as f :
                sql = """
                    copy (select * from {1} where starttime = '{0:%Y-%m-%d}')  to STDOUT 
                """
                cur.copy_expert(sql.format(date,tn), f)
    conn.close()
    local('tar -czf {0:%Y%m%d}.tgz '.format(date) + ' '.join([fn.format(date) for tn,fn in cmpfiles]))

def cmpcounter_d(cur,date,delcase = False):
    if delcase : cur.execute("delete from cmpcounter_d where starttime = '{0:%Y-%m-%d}';".format(date))
    sql = """
        insert into cmpcounter_d 
        select 
        '{0:%Y-%m-%d}'::timestamp,
        enbid,
        eci,
        sum(rsrp_avg_cmcc),
        sum(rsrp_count_cmcc),
        sum(rsrp_avg_chun_nume),
        sum(rsrp_avg_chte_nume),
        sum(rsrp_count_chun),
        sum(rsrp_count_chte),
        sum(rsrp_weak_cmcc),
        sum(rsrp_weak_chun_110),
        sum(rsrp_weak_chte_110),
        sum(rsrp_weak_chun_113),
        sum(rsrp_weak_chte_113),
        sum(rsrp_avg_inter_cmcc_nume),
        sum(rsrp_count_inter_cmcc),
        sum(rsrp_weak_inter_cmcc),
        null 
        from cmpcounter where starttime >= '{0:%Y-%m-%d}' and starttime < '{1:%Y-%m-%d}' 
        group by enbid,eci 
    """.format(date,date+datetime.timedelta(days=1))
    cur.execute(sql)



def eutruncelltdd_upperall_d(cur,date,delcase=False):
    if delcase : cur.execute("delete from eutruncelltdd_upperall_d where starttime = '{0:%Y-%m-%d}';".format(date))
    sql = """
        insert into eutruncelltdd_upperall_d
        with a as (select starttime,enbid,eci,
        rsrp_count_cmcc,
        rsrp_avg_cmcc/rsrp_count_cmcc  rsrp_avg,
        rsrp_count_chun,
        rsrp_avg_chun_nume/rsrp_count_chun rsrp_avg_chun,
        rsrp_count_chte,
        rsrp_avg_chte_nume/rsrp_count_chte  rsrp_avg_chte,
        rsrp_avg_inter_cmcc_nume/rsrp_count_inter_cmcc rsrp_avg_inter_cmcc,
        case when rsrp_count_chun>1000 then True  else False end    efct_cell_chun,
        case when rsrp_count_chte>1000 then True  else False end    efct_cell_chte,
        rsrp_weak_cmcc,
        1-(rsrp_weak_cmcc/rsrp_count_cmcc) rsrp_coverage_rate_cmcc,
        1-(rsrp_weak_inter_cmcc/rsrp_count_inter_cmcc) rsrp_coverage_rate_inter_cmcc,
        rsrp_weak_chun_110,
        1-(rsrp_weak_chun_110/rsrp_count_chun) rsrp_coverage_rate_chun_110,
        rsrp_weak_chte_110,
        1-(rsrp_weak_chte_110/rsrp_count_chte) rsrp_coverage_rate_chte_110,
        rsrp_weak_chun_113,
        1-(rsrp_weak_chun_113/rsrp_count_chun) rsrp_coverage_rate_chun_113,
        rsrp_weak_chte_113,
        1-(rsrp_weak_chte_113/rsrp_count_chte) rsrp_coverage_rate_chte_113
        from cmpcounter_d where starttime = '{0:%Y-%m-%d}' )

        select
        a.starttime, a.enbid, a.eci, 
        a.rsrp_count_cmcc,   --移动总采样点
        case when a.rsrp_count_cmcc > 300 then 1 else 0 end  efct_cell_cmcc,  --有效小区-移动
        a.rsrp_avg,    --移动平均电平
        a.rsrp_coverage_rate_cmcc,       --移动覆盖率
        a.rsrp_weak_cmcc,      --移动弱覆盖采样点(小于-110采样点)
        case when a.rsrp_coverage_rate_cmcc < 0.8 then true else false end weak_cell_cmcc , --弱覆盖小区判别
        a.rsrp_avg_chun ,   --联通平均电平
        a.rsrp_avg_chte ,   --电信平均电平
        a.rsrp_avg_inter_cmcc ,  --移动优于竞对平均电平
        case when  a.rsrp_count_chte > 300 then true else false end efct_cell_chte,    --有效小区-电信
        case when  a.rsrp_count_chun > 300 then true else false end efct_cell_chun  ,    --有效小区-联通
        a.rsrp_count_chun ,   --联通总采样点
        a.rsrp_count_chte,    --电信总采样点
        a.rsrp_weak_chun_110, --联通弱覆盖采样点(小于-110采样点)
        a.rsrp_weak_chte_110, --电信弱覆盖采样点(小于-110采样点)
        a.rsrp_weak_chun_113, --联通弱覆盖采样点(小于-113采样点)
        a.rsrp_weak_chte_113, --电信弱覆盖采样点(小于-113采样点)
        a.rsrp_coverage_rate_inter_cmcc,    --移动优于竞对覆盖率
        a.rsrp_coverage_rate_chun_110,   --联通覆盖率（RSRP≥-110dBm的采样点占比）
        a.rsrp_coverage_rate_chte_110,   --电信覆盖率（RSRP≥-110dBm的采样点占比）
        a.rsrp_coverage_rate_chun_113,   --联通覆盖率（RSRP≥-113dBm的采样点占比）
        a.rsrp_coverage_rate_chte_113,   --电信覆盖率（RSRP≥-113dBm的采样点占比）

        case when rsrp_coverage_rate_cmcc<0.8 and (rsrp_coverage_rate_chun_110>0.8 or rsrp_coverage_rate_chte_110>0.8) then 1
            when  rsrp_coverage_rate_cmcc>0.8 and (rsrp_coverage_rate_chun_110 - rsrp_coverage_rate_cmcc > 0.05 or rsrp_coverage_rate_chte_110 - rsrp_coverage_rate_cmcc > 0.05) then 2
            else 0 end  opponent_level_110,   --opponent_level（小于-110）劣于联通或劣于电信
        case when rsrp_coverage_rate_cmcc<0.8 and (rsrp_coverage_rate_chun_113>0.8 or rsrp_coverage_rate_chte_113>0.8) then 1
            when  rsrp_coverage_rate_cmcc>0.8 and (rsrp_coverage_rate_chun_113 - rsrp_coverage_rate_cmcc > 0.05 or rsrp_coverage_rate_chte_113 - rsrp_coverage_rate_cmcc > 0.05) then 2
            else 0 end  opponent_level_113,   --opponent_level（小于-113）劣于联通或劣于电信
        case when rsrp_coverage_rate_cmcc<0.8 and (rsrp_coverage_rate_chun_113>0.8 ) then 1
            when  rsrp_coverage_rate_cmcc>0.8 and (rsrp_coverage_rate_chun_113 - rsrp_coverage_rate_cmcc > 0.05 ) then 2
            else 0 end  opponent_level_113_chun,   --opponent_level（小于-113）单独劣于联通
        case when rsrp_coverage_rate_cmcc<0.8 and (rsrp_coverage_rate_chte_113>0.8) then 1
            when  rsrp_coverage_rate_cmcc>0.8 and (rsrp_coverage_rate_chte_113 - rsrp_coverage_rate_cmcc > 0.05) then 2
            else 0 end  opponent_level_113_chte,    --opponent_level（小于-113）单独劣于电信
        case when a.rsrp_coverage_rate_cmcc-a.rsrp_weak_chun_113 < a.rsrp_coverage_rate_cmcc-a.rsrp_coverage_rate_chte_113 
            then a.rsrp_coverage_rate_cmcc-a.rsrp_coverage_rate_chun_113  
            else a.rsrp_coverage_rate_cmcc-a.rsrp_coverage_rate_chte_113 end opponent_coverage_rate_min,  --最小竞对优势
        mod(mod(a.eci,256),3) cell_coverage_id  --小区同覆盖编号
        from a 
    """.format(date)
    cur.execute(sql)





def city_country_m(cur,date,delcase=False):
    if delcase : cur.execute("delete from city_country_m where starttime = '{0:%Y-%m-%d}';".format(date))
    sql = """
        insert into  city_country_m 
        with a as(
                select a.* ,b.city, b.country, 
                case when b.cover_type='室外' and a.rsrp_coverage_rate_cmcc<0.8 then 1 else 0 end is_weakcell
                from eutruncelltdd_upperall_d a join eutrancell_res b
                on a.eci = b.eci and b.atu_database is true and a.starttime = '{0:%Y-%m-%d}')
        select 
        starttime , city,country,
        sum(rsrp_count_cmcc) sum_rsrp_count_cmcc,
        sum(rsrp_weak_cmcc) sum_rsrp_weak_cmcc,
        count(*) sum_mro_cell_cmcc,
        sum(efct_cell_cmcc) sum_efct_cell_cmcc,
        1-sum(rsrp_weak_cmcc)/sum(rsrp_count_cmcc) sta_rsrp_coverage_rate_cmcc,
        sum(is_weakcell)
        from a group by starttime, city, country    
    """.format(date)
    cur.execute(sql)

def city_country_opponent_m(cur,date,delcase=False):
    if delcase : cur.execute("delete from city_country_opponent_m where starttime = '{0:%Y-%m-%d}';".format(date))
    sql = """
        insert into  city_country_opponent_m  
        with a as(
                    select a.starttime, b.city, b.country, 	
                    count(*) sum_mro_cell_cmcc,  --MRO移动小区数量
                    1-sum(rsrp_weak_cmcc)/sum(rsrp_count_cmcc) sta_rsrp_coverage_rate_cmcc, --本月本网覆盖率
                    1-sum(rsrp_weak_chun_110)/sum(rsrp_count_chun)  sta_rsrp_coverage_rate_chun_110,  --联通覆盖率（-110）
                    1-sum(rsrp_weak_chte_110)/sum(rsrp_count_chte) sta_rsrp_coverage_rate_chte_110,   --电信覆盖率（-110）
                    1-sum(rsrp_weak_chun_113)/sum(rsrp_count_chun)  sta_rsrp_coverage_rate_chun_113,  --联通覆盖率（-113）
                    1-sum(rsrp_weak_chte_113)/sum(rsrp_count_chte) sta_rsrp_coverage_rate_chte_113,   --电信覆盖率（-113）
                    sum(case when opponent_level_113 > 0 then 1 else 0 end ) opponent_level_113_cell_num,  --劣于竞对小区数量（电信或联通）
                    sum(case when opponent_level_113_chun > 0 then 1 else 0 end ) opponent_level_113_chun_cell_num,  --劣于竞对小区数量-联通
                    sum(case when opponent_level_113_chte > 0 then 1 else 0 end ) opponent_level_113_chte_cell_num,  --劣于竞对小区数量-电信	
                    sum(rsrp_count_cmcc) sum_rsrp_count_cmcc ,   --移动总采样点
                    sum(rsrp_count_chun) sum_rsrp_count_chun ,   --联通总采样点
                    sum(rsrp_count_chte) sum_rsrp_count_chte     --电信总采样点
                from eutruncelltdd_upperall_d a join eutrancell_res b
                on a.eci = b.eci and a.starttime = '{0:%Y-%m-%d}'
                group by b.city, b.country , a.starttime),
                b as(
                select city,country,count(*) sum_base_cell_cmcc from eutrancell_res group by city,country)
        select a.starttime,a.city,a.country,
        sum_base_cell_cmcc, --数据库移动小区数量
        sum_mro_cell_cmcc,  --MRO移动小区数量
        sum_mro_cell_cmcc::double precision /sum_base_cell_cmcc cell_base_num_proportion,   --移动小区数量与基站数据库小区比值
        sta_rsrp_coverage_rate_cmcc,    --本月本网覆盖率
        sta_rsrp_coverage_rate_chun_110,  --联通覆盖率（-110）
        sta_rsrp_coverage_rate_chte_110,   --电信覆盖率（-110）
        sta_rsrp_coverage_rate_chun_113,  --联通覆盖率（-113）
        sta_rsrp_coverage_rate_chte_113,   --电信覆盖率（-113）
        sta_rsrp_coverage_rate_cmcc - sta_rsrp_coverage_rate_chun_110  dif_sta_rsrp_coverage_rate_cmcc_chun_110, --移动本月强于联通（-110）
        sta_rsrp_coverage_rate_cmcc - sta_rsrp_coverage_rate_chte_110  dif_sta_rsrp_coverage_rate_cmcc_chte_110, --移动本月强于电信（-110）
        case when sta_rsrp_coverage_rate_cmcc - sta_rsrp_coverage_rate_chun_110 < sta_rsrp_coverage_rate_cmcc - sta_rsrp_coverage_rate_chte_110 
            then sta_rsrp_coverage_rate_cmcc - sta_rsrp_coverage_rate_chun_110
            else sta_rsrp_coverage_rate_cmcc - sta_rsrp_coverage_rate_chte_110 end opponent_coverage_110_min,  --本月竞对最小优势（-110）
        sta_rsrp_coverage_rate_cmcc - sta_rsrp_coverage_rate_chun_113  dif_sta_rsrp_coverage_rate_cmcc_chun_113, --移动本月强于联通（-113）
        sta_rsrp_coverage_rate_cmcc - sta_rsrp_coverage_rate_chte_113  dif_sta_rsrp_coverage_rate_cmcc_chte_113, --移动本月强于电信（-113）
        case when sta_rsrp_coverage_rate_cmcc - sta_rsrp_coverage_rate_chun_113 < sta_rsrp_coverage_rate_cmcc - sta_rsrp_coverage_rate_chte_113 
            then sta_rsrp_coverage_rate_cmcc - sta_rsrp_coverage_rate_chun_113
            else sta_rsrp_coverage_rate_cmcc - sta_rsrp_coverage_rate_chte_113 end opponent_coverage_113_min,  --本月竞对最小优势（-113）
        opponent_level_113_cell_num::double precision /sum_mro_cell_cmcc  opponent_level_113_cell_proportion,  --劣于竞对小区占比（电信或联通）
        opponent_level_113_chun_cell_num::double precision /sum_mro_cell_cmcc  opponent_level_113_chun_cell_proportion,  --劣于竞对小区占比-联通
        opponent_level_113_chte_cell_num::double precision /sum_mro_cell_cmcc  opponent_level_113_chte_cell_proportion,  --劣于竞对小区占比-电信	
        case when opponent_level_113_chun_cell_num<opponent_level_113_chte_cell_num 
            then opponent_level_113_chun_cell_num::double precision/sum_mro_cell_cmcc
            else opponent_level_113_chte_cell_num::double precision/sum_mro_cell_cmcc  end opponent_level_113_cell_proportion_min, --劣于竞对小区占比-联通或劣于竞对小区占比-电信的min
        opponent_level_113_cell_num,  --劣于竞对小区数量（电信或联通）
        opponent_level_113_chun_cell_num,  --劣于竞对小区数量-联通
        opponent_level_113_chte_cell_num,  --劣于竞对小区数量-电信
        sum_rsrp_count_cmcc ,   --移动总采样点
        sum_rsrp_count_chun ,   --联通总采样点
        sum_rsrp_count_chte     --电信总采样点
        from a,b where a.city=b.city and a.country = b.country     
    """.format(date)
    cur.execute(sql)