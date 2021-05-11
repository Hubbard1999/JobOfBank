def get_other_features(spark, basic_sql, train_start_date, train_end_date, data_path):
    moudules_list = ['转账', '信用卡', '我的账户', '债市宝', '无卡取款', '慈善捐款',
                     '基金', '存金通', '大额存单', '摩拜单车', '贷款', '农银智投',
                     '我的商城', '首页脚本索引', '微寻宝', '银证转账', '存款专区',
                     '保险', '质押贷款', '银医直通', '新版基金', '二维码', '双向宝',
                     '安全中心', '联系客户', '我的优惠券', '我的负债', '银期转账', '活利丰',
                     '理财产品', '线下人脸支付', '自动理财', '新版投资理财索引菜单', '客户端支付',
                     '语音导航', '账户贵金属', '现钞', '外币', '客户经理', '定活互转', '云闪付',
                     '企业年金', '商城支付', '理财计算器', '信息与设置', '弱客户升级',
                     '我的页面索引菜单', '私人银行', '商城页面', '银利多', '电子账户', '银联优惠',
                     '电子社保卡', '结售汇', '消息服务', '他行账户', '行情数据', '移动K码', '排队',
                     '年度账单', '单位结算卡', '网点定位', '登录', '党费', '农银快e付', '跨境电汇',
                     '服务', '手机号注册', '双利丰', '西联汇款', '嗨豆乐园', '根', '投资理财索引菜单',
                     '存款', '消息中心', '模块', '数据网贷', '密码重置', '自助注册', '纪念币预约',
                     '金融日历', '交叉营销', '存单存折', '贷款征信授权', '特色', '我的页面',
                     '我的生活', '分行特色', '工资单', '客户端登录控制', '弱客户绑他行卡升级',
                     '投资', '首页索引菜单', '自建云闪付', '查询', '附近页面', '生活缴费', 'HCE',
                     '饭卡充值', '首页面', '分行特色New', '本地', '存款利率', '我的资产', '生活',
                     '附近', '个性化全量菜单', '外汇', '网点服务', '月度账单', '手机充值', '我的钱包',
                     '上次登录时间', '银证E站通', '常用', '隐藏菜单根节点', '商城', '我的中奖纪录',
                     'OAUTH登录', '软token支付', '拍拍支付', 'K码支付', '军人专区']
    
    modules_sql = ''
    for i in range(len(moudules_list)):
        modules_sql = modules_sql + '''case when module='{}' then module_count else 0 end as '{}','''.format(moudules_list[i],'module'+moudules_list[i])
    
    modules_sum_sql = ''''''
    for i in range(len(moudules_list)):
        modules_sum_sql = modules_sum_sql + '''sum('{}')/all_times as '{}', '''.format('module'+moudules_list[i],'module'+moudules_list[i])
        
    module_count_sql = '''
    (select id,sum(module_count) as all_times,{} from
    (select id,module,sum(times) as module_count,{} from
    (select id,event_code,count(event_code) as times from {} group by id,event_code) a global inner join
    ftabcch.event_param_distribute b on a.event_code=b.event_code group by id,module) group by id) t
    '''.format(modules_sum_sql[:-1],modules_sql[:-1],basic_sql)
    
    module_count = read_ck_data(spark,module_count_sql)
    module_count.cache()
    module_count.show(5)
    
    weekday = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday']
    weekday_count_sql = '''
    (select id,sum(times) as all_times,sum(monday)/all_times as monday,
    sum(tuesday)/all_times as tuesday,
    sum(wednesday)/all_times as wednesday,
    sum(thursday)/all_times as thursday,
    sum(friday)/all_times as friday,
    sum(saturday)/all_times as saturday,
    sum(sunday)/all_times as sunday from
    (select id,weekday,times,case when weekday=1 then times else 0 end as monday,
    case when weekday=2 then times else 0 end as tuesday,
    case when weekday=3 then times else 0 end as wednesday,
    case when weekday=4 then times else 0 end as thursday,
    case when weekday=5 then times else 0 end as friday,
    case when weekday=6 then times else 0 end as saturday,
    case when weekday=7 then times else 0 end as sunday from
    (select id,toDayOfWeek(app_date) as weekday,count(event_code) as times from {} group by id,weekday)) group by id) t
    '''.format(basic_sql)
    weekday_count = read_ck_data(spark,weekday_count_sql).drop('all_times')
    weekday_count.cache()
    weekday_count.show(5)    
    
    events_list = ['M3-0000-00','softtoken_pay','M3-DQ00-00','M3-7000-00','M3-2500-00','M3-2500-03','M3-F500-00',
                  'M3-4B00-03','M3-N400-00','M3-Z604-00','M3-4B00-02','ANDROID','M3-N010-00','M3-N100-01','M3-D901-00']
    
    events_sql = ''
    for i in range(len(events_list)):
        events_sql = events_sql + '''case when event_code='{}' then event_count else 0 end as '{}','''.format(events_list[i],'event'+events_list[i])
        
    event_sum_sql = ''''''
    for i in range(len(events_list)):
        event_sum_sql = event_sum_sql + '''sum('{}')/all_times as '{}','''.format('event'+events_list[i],'event'+events_list[i])
    
    event_count_sql = '''
    (select id,sum(event_count) as all_times,{} from
    (select id,event_code,count(event_code) as event_count,{} from {} group by id,event_code) group by id) t
    '''.format(events_sum_sql[:-1],events_sql[:-1],basic_sql)
    
    events = read_ck_data(spark,event_count_sql).drop('all_times')
    events.cache()
    events.show(5)
    
    last_two_event_datediff_sql = '''
    (select id,datediff('day',app_dates[2],app_dates[1]) as last_two_event_datediff from
    (select id,groupArray(app_date) as app_dates from
    (select distinct id,app_date from {} order by id,app_date desc) group by id)) t
    '''.format(basic_sql)
    
    last_two_event_datediff = read_ck_data(spark,last_two_event_datediff_sql).fillna(90)
    last_two_event_datediff.cache()
    last_two_event_datediff.show(5)    
    
    order_basic_sql = '''
    (select id,trc_type,total_amount from ftabcch.ods_order_middle_distribute
    where create_date between toDate('{}') and toDate('{}') and id global in
    (select arrayJoin(group_user) from ftabcch.ads_plan_user_group_distribute
    where operation_id={} and group_name!='对照组'))
    '''.format(train_start_date,train_end_date,oper[0])
    
    all_order_sql = '''
    (select id,countIf(trc_type in ('0104','0105','0111')) as lc_order_times,
    sumIf(total_amount,trc_type in ('0104','0105','0111')) as lc_order_amount,
    countIf(trc_type in ('0106','0107','0108','0109','0110')) as jj_order_times,
    sumIf(total_amount,trc_type in ('0106','0107','0108','0109','0110')) as jj_order_amount,
    countIf(trc_type in ('1','0','-')) as gjs_order_times,
    sumIf(total_amount,trc_type in ('1','0','-')) as gjs_order_amount
    from {} group by id) t
    '''.format(order_basic_sql)
    
    all_order = read_ck_data(spark,all_order_sql).fillna(0)
    all_order.cache()
    all_order.show(5)    
    
    return [all_order,module_count,weekday_count,last_two_event_datediff,events]

sql = '''
(select * from
(select user_id,id,gender,user_level,population,
case when nation=1 then 1 else 2 end as nation,
case when education in (40.0,9.0,16.0,24.0,34.0) then 1
when education in (30.0,43.0,5.0,18.0) then 2
when education in (25.0,6.0,20.0,37.0,28.0,35.0,19.0,27.0,46.0,47.0,36.0,21.0,3.0,10.0) then 3
when education in (23.0,15.0,32.0,29.0,2.0,11.0,1.0,14.0,38.0,13.0,45.0,12.0) then 4
when education in (42.0,7.0,17.0,33.0,39.0) then 5
when education in (51.0,44.0,26.0,50.0,41.0) then 6 else 7 end as education,
datediff('day'),account_opening_date,toDate('{}'))/30 as account_opening_date,
case when country=156 then 0 else 1 end as country,
case when job_level=1 then 1
when job_level=2 then 2 when job_level=3 then 3
when job_level=4 then 4 when job_level=5 then 5
when job_level=6 then 6 when job_level=7 then 7
when job_level=8 then 8 when job_level=9 then 9
when job_level=10 then 10 when job_level=11 then 11
when job_level=12 then 12 else 13 end as job_level,
age_range as age,relationship_type,marriage,toInt32(inline_customer) as inline_customer,
case when sub_branch='广州三元里支行(汇总)' then 1
case when sub_branch='广州东城支行(汇总)' then 2
case when sub_branch='广州从化支行(汇总)' then 3
case when sub_branch='广州分行后台处理中心(汇总)' then 4
case when sub_branch='广州北秀支行(汇总)' then 5
case when sub_branch='广州华南支行(汇总)' then 6
case when sub_branch='广州南沙支行(汇总)' then 7
case when sub_branch='广州城南支行(汇总)' then 8
case when sub_branch='广州增城支行(汇总)' then 9
case when sub_branch='广州天河支行(汇总)' then 10
case when sub_branch='广州开发区支行(汇总)' then 11
case when sub_branch='广州流花支行(汇总)' then 12
case when sub_branch='广州海珠支行(汇总)' then 13
case when sub_branch='广州淘金支行(汇总)' then 14
case when sub_branch='广州珠江支行(汇总)' then 15
case when sub_branch='广州琶洲支行(汇总)' then 16
case when sub_branch='广州番禺支行(汇总)' then 17
case when sub_branch='广州白云支行(汇总)' then 18
case when sub_branch='广州花都支行(汇总)' then 19 else 0 end as sub_branch,
case when id global in {} then 1 else 0 end as label
from ftabcch.ods_user_distribute where id global in {}) a global inner join
(with (select max(bal_date) from ftabcch.user_balance_distribute where bal_date<=toDate('{}')) as target_date
select id,fa_bal,aum,ifib_bal,bond_bal,fund_bal,oth_inv_bal,load_bal from ftabcch.user_balance_distribute where
bal_date = target_date) b on a.id=b.id) t
'''.format(train_end_date,label_sql,target_user_sql,train_end_date)
ods_user_basic = read_ck_data(spark,sql).drop('b.id').fillna(0)
ods_user_basic.write.parquet(data_path+'ods_user_basic.parquet',mode = 'overwrite')
ods_user_basic = spark.read.parquet(data_path+'ods_user_basic.parquet')
ods_user_basic.cache()
ods_user_basic.show()    
try:
    ks_features = get_event_features(spark,basic_sql,train_end_date,data_path)
    std_features = get_santander_features(spark,basic_sql,train_start_date,,train_end_date,data_path)
    act_features = get_action_features(spark,basic_sql,train_start_date,train_end_date,data_path)
    o_features = get_other_features(spark,basic_sql,train_start_date,train_end_date,data_path)
    order_features = o_features[0]
    module_features = o_features[1]
    week_features = o_features[2]
    datediff_features = o_features[3]
    event_features = o_features[4]
except:
    pass