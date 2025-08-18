"CREATE TABLE comment_info (" +
                "  id BIGINT," +
                "  user_id BIGINT," +
                "  nick_name varchar(20)," +
                "  head_img varchar(200)," +
                "  sku_id bigint," +
                "  spu_id BIGINT(200)," +
                "  order_id BIGINT(200)," +
                "  appraise varchar(10)," +
                "  comment_txt varchar(200)," +
                "  create_time varchar(200)," +
                "  operate_time varchar(200)," +
                "  PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" +
                "   'connector' = 'jdbc'," +
                "   'url' = 'jdbc:mysql://cdh03:3306/realtime_v1'," +
                "   'table-name' = 'comment_info'," +
                "   'username' = 'root'," +
                "   'password' = '123456'" +
                ");