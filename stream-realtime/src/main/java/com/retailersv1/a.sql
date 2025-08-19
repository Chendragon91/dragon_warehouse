create table ods_professional(
    `database` string,
    `table` string,
    `type` string,
    `ts` bigint,
    `data` map<string,string>,
    `old` map<string,string>,
    proc_time as proctime()
) with (
    'connector'='kafka',
    'topic'='ods_professional',
    'properties.bootstrap.servers'='cdh01:9092',
    'properties.group.id'='test',
    'scan.startup.mode'='latest-offset',
    'format'='json'
);


select
    `data`['id'] id,
    `data`['user_id'] user_id,
    `data`['sku_id'] sku_id,
    `data`['appraise'] appraise,
    `data`['comment_txt'] comment_txt,
    ts,
    proc_time
from ods_professional where `table`='comment_info' and `type`='insert'




create table base_dic(
    `dic_code` string,
    info row<dic_name string>,
    primary key (dic_code) not enforced
)with (
    'connector'='hbase-2.2',
    'table_name'='dim_base_dic',
    'zookeeper.quorum'='cdh01:2181',
    'lookup.async'='true',
    'lookup.cache'='PARTIAL',
    'lookup.partial.cache.max-rows'='500',
    'lookup.partial.cache.expire-after_write'='1 hour',
    'lookup.partial-cache.expire-after-access'='1 hour'
);




select
    id,
    user_id,
    sku_id,
    appraise,
    dic.dic_name as appraise_name,
    comment_txt,
    ts
from comment_info as c
json base_dic for system_time as of c.proc_time as dic
on c.appraise = dic.dic_code



create table a(
    id string,
    user_id string,
    sku_id string,
    appraise string,
    appraise_name string,
    comment_txt string,
    ts bigint,
    primary key (id) not enforced
)with (
    'connector'='upsert-kafka',
    'topic'='comment_info',
    'properties.bootstrap.servers'='cdh01:9092',
    'key.format'='json',
    'value.format'='json'
);
























