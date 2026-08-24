-- =============================================================
-- 电商业务库建表 DDL（gmall 库，MySQL 8.0）
-- 设计要点:
--   1. 每张表带 update_time: 增量同步的水位线依据（面试重点）
--   2. 金额 decimal(16,2), 时间 datetime
--   3. payment_info.create_time = 支付时间（GMV 口径依据）
-- =============================================================

USE gmall;

-- 用户表
CREATE TABLE IF NOT EXISTS user_info (
    id            BIGINT       NOT NULL AUTO_INCREMENT COMMENT '用户ID',
    login_name    VARCHAR(64)  COMMENT '登录名',
    nick_name     VARCHAR(64)  COMMENT '昵称',
    passwd        VARCHAR(64)  COMMENT '密码',
    name          VARCHAR(32)  COMMENT '姓名',
    phone_num     VARCHAR(32)  COMMENT '手机号',
    email         VARCHAR(64)  COMMENT '邮箱',
    head_img      VARCHAR(128) COMMENT '头像',
    user_level    VARCHAR(16)  COMMENT '用户等级',
    birthday      DATE         COMMENT '生日',
    gender        VARCHAR(8)   COMMENT '性别',
    create_time   DATETIME     COMMENT '注册时间',
    operate_time  DATETIME     COMMENT '最后操作时间',
    update_time   DATETIME     COMMENT '修改时间（增量同步水位）',
    PRIMARY KEY (id)
) ENGINE = InnoDB COMMENT = '用户表';

-- 省份表（静态维度）
CREATE TABLE IF NOT EXISTS base_province (
    id         BIGINT      NOT NULL PRIMARY KEY COMMENT '省份ID',
    name       VARCHAR(32) COMMENT '省份名称',
    region_id  BIGINT      COMMENT '所属地区ID',
    area_code  VARCHAR(16) COMMENT '行政区划代码'
) ENGINE = InnoDB COMMENT = '省份表';

-- 商品分类表（静态维度，三级分类）
CREATE TABLE IF NOT EXISTS base_category (
    id              BIGINT      NOT NULL PRIMARY KEY COMMENT '分类ID',
    name            VARCHAR(32) COMMENT '分类名称',
    category_level  INT         COMMENT '分类层级(1/2/3)',
    parent_id       BIGINT      COMMENT '父分类ID'
) ENGINE = InnoDB COMMENT = '商品分类表';

-- 商品 SPU 表（标准产品单元）
CREATE TABLE IF NOT EXISTS spu_info (
    id           BIGINT       NOT NULL PRIMARY KEY COMMENT 'SPU ID',
    spu_name     VARCHAR(128) COMMENT 'SPU名称',
    description  VARCHAR(256) COMMENT '描述',
    category_id  BIGINT       COMMENT '分类ID',
    create_time  DATETIME     COMMENT '创建时间',
    update_time  DATETIME     COMMENT '修改时间（增量同步水位）'
) ENGINE = InnoDB COMMENT = '商品SPU表';

-- 商品 SKU 表（具体销售单元）
CREATE TABLE IF NOT EXISTS sku_info (
    id           BIGINT       NOT NULL PRIMARY KEY COMMENT 'SKU ID',
    spu_id       BIGINT       COMMENT '所属SPU ID',
    sku_name     VARCHAR(128) COMMENT 'SKU名称',
    price        DECIMAL(16,2) COMMENT '价格',
    weight       DECIMAL(8,2) COMMENT '重量(kg)',
    category_id  BIGINT       COMMENT '分类ID',
    create_time  DATETIME     COMMENT '创建时间',
    update_time  DATETIME     COMMENT '修改时间（增量同步水位）'
) ENGINE = InnoDB COMMENT = '商品SKU表';

-- 订单主表
CREATE TABLE IF NOT EXISTS order_info (
    id                     BIGINT       NOT NULL PRIMARY KEY COMMENT '订单ID',
    consignee              VARCHAR(32)  COMMENT '收货人',
    consignee_tel          VARCHAR(32)  COMMENT '收货人电话',
    total_amount           DECIMAL(16,2) COMMENT '总金额（含运费）',
    order_status           VARCHAR(16)  COMMENT '订单状态',
    user_id                BIGINT       COMMENT '用户ID',
    payment_way            VARCHAR(8)   COMMENT '支付方式',
    delivery_address       VARCHAR(128) COMMENT '收货地址',
    order_comment          VARCHAR(128) COMMENT '订单备注',
    out_trade_no           VARCHAR(32)  COMMENT '外部交易号',
    trade_body             VARCHAR(128) COMMENT '交易内容',
    create_time            DATETIME     COMMENT '下单时间',
    operate_time           DATETIME     COMMENT '操作时间',
    expire_time            DATETIME     COMMENT '过期时间',
    process_status         VARCHAR(16)  COMMENT '处理状态',
    tracking_no            VARCHAR(32)  COMMENT '物流单号',
    parent_order_id        BIGINT       COMMENT '父订单ID',
    img_url                VARCHAR(128) COMMENT '图片',
    province_id            BIGINT       COMMENT '省份ID',
    benefit_reduce_amount  DECIMAL(16,2) COMMENT '优惠金额',
    original_total_amount  DECIMAL(16,2) COMMENT '原价金额',
    feight_fee             DECIMAL(16,2) COMMENT '运费',
    update_time            DATETIME     COMMENT '修改时间（增量同步水位）'
) ENGINE = InnoDB COMMENT = '订单主表';

-- 订单明细表
CREATE TABLE IF NOT EXISTS order_detail (
    id          BIGINT       NOT NULL PRIMARY KEY COMMENT '明细ID',
    order_id    BIGINT       COMMENT '订单ID',
    sku_id      BIGINT       COMMENT 'SKU ID',
    sku_name    VARCHAR(128) COMMENT 'SKU名称',
    img_url     VARCHAR(128) COMMENT '图片',
    order_price DECIMAL(16,2) COMMENT '成交单价',
    sku_num     BIGINT       COMMENT '数量',
    create_time DATETIME     COMMENT '创建时间',
    update_time DATETIME     COMMENT '修改时间（增量同步水位）'
) ENGINE = InnoDB COMMENT = '订单明细表';

-- 支付流水表
CREATE TABLE IF NOT EXISTS payment_info (
    id              BIGINT      NOT NULL PRIMARY KEY COMMENT '支付流水ID',
    out_trade_no    VARCHAR(32) COMMENT '外部交易号',
    order_id        BIGINT      COMMENT '订单ID',
    user_id         BIGINT      COMMENT '用户ID',
    payment_type    VARCHAR(16) COMMENT '支付类型',
    trade_no        VARCHAR(32) COMMENT '支付交易号',
    payment_amount  DECIMAL(16,2) COMMENT '支付金额',
    subject         VARCHAR(128) COMMENT '交易内容',
    payment_status  VARCHAR(16) COMMENT '支付状态',
    create_time     DATETIME    COMMENT '支付时间（GMV 口径依据）',
    callback_time   DATETIME    COMMENT '回调时间',
    update_time     DATETIME    COMMENT '修改时间（增量同步水位）'
) ENGINE = InnoDB COMMENT = '支付流水表';

-- 退款信息表
CREATE TABLE IF NOT EXISTS order_refund_info (
    id                  BIGINT       NOT NULL PRIMARY KEY COMMENT '退款ID',
    user_id             BIGINT       COMMENT '用户ID',
    order_id            BIGINT       COMMENT '订单ID',
    sku_id              BIGINT       COMMENT 'SKU ID',
    refund_type         VARCHAR(8)   COMMENT '退款类型',
    refund_num          BIGINT       COMMENT '退款数量',
    refund_amount       DECIMAL(16,2) COMMENT '退款金额',
    refund_reason_type  VARCHAR(8)   COMMENT '退款原因类型',
    refund_reason_txt   VARCHAR(256) COMMENT '退款原因说明',
    refund_status       VARCHAR(8)   COMMENT '退款状态',
    create_time         DATETIME     COMMENT '退款申请时间',
    update_time         DATETIME     COMMENT '修改时间（增量同步水位）'
) ENGINE = InnoDB COMMENT = '退款信息表';
