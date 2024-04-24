drop table if exists ods_base_dic_full;
create external table ods_base_dic_full(
    `id` bigint comment '编号（主键）',
    `parent_id` bigint comment '父级编号',
    `name` string comment '名称',
    `dict_code` string comment '编码',
    `create_time` string comment '创建时间',
    `update_time` string comment '更新时间',
    `is_deleted` string comment '是否删除'
) comment '字典全量表'
partitioned by (`dt` string)
row format delimited fields terminated by '\t'
null defined as ''
location '/warehouse/tms/ods/ods_base_dic_full';