create table dwd_traffic_page_view_inc(
    mid string,
    user_id string,
    version_code int,
    channel string,
    area string,
    is_new boolean,
    page_id string,
    last_page_id string,
    page_item string,
    page_item_type string,
    during_time bigint,
    ts bigint,
    proc_time as to_timestamp(from_unixtime(ts/1000)),
    gender string,
    age int,
    user_level int,
    sku_name string,
    tm_id string,
    category3_id string
);






create table page_log(
    common map<string,string>,
    page map<string,string>,
    ts
)




create table ods_professional(
    op map<string,string>,
    after map<string,string>,
    source map<string,string>,
    ts_ms bigint
);



select
    `after`['id'] bigint,
    `after`['order_id'] bigint,
    `after`['sku_id'] bigint,
    `after`['sku_name'] string,
    `after`['order_price'] decimal(16,2),
    `after`['sku_num'] bigint,
    `after`['create_time'] datetime,
    `after`['split_total_amount'] decimal(16,2),
    `after`['split_activity_amount'] decimal(16,2),
    `after`['split_coupon_amount'] decimal(16,2)
from ods_professional
where `source`['table']='order_detail';





