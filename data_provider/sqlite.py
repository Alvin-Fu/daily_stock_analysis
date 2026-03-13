import logging
import os.path
import sqlite3

# 配置日志
logger = logging.getLogger(__name__)


class SQLiteDB:
    def __init__(self, db_file = "stock_table.db"):
      """
      初始化链接
      :param db_file: 数据库文件路径，不存在自动创建
      """
      self.db_file = db_file
      self.conn = None
      self.cursor = None
    def connect(self):
        """"建立数据库链接"""
        try:
            self.conn = sqlite3.connect(self.db_file)
            self.cursor = self.conn.cursor()
            logger.debug(f"数据库链接建立成功：{self.db_file}")
        except sqlite3.Error as e:
            logger.error(f"数据库链接失败：{e}")
            raise # 抛出异常，让调用方处理
    def close(self):
        """关闭连接，释放资源"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            logger.debug("数据库链接已关闭")

    def execute_sql_file(self, sql_file):
        """读取并执行.sql文件中的建表语句（健壮版）"""
        if not os.path.exists(sql_file):
            raise FileNotFoundError(f"SQL文件不存在：{sql_file}")

        # 1. 读取文件（兼容GBK/UTF-8编码，自动处理BOM）
        try:
            # 先尝试UTF-8
            with open(sql_file, "r", encoding="utf-8") as f:
                sql_content = f.read()
        except UnicodeDecodeError:
            # 失败则尝试GBK（Windows默认编码）
            with open(sql_file, "r", encoding="gbk") as f:
                sql_content = f.read()

        # 2. 预处理：移除/* */块注释
        import re
        # 正则匹配/* 任意内容 */，替换为空（非贪婪匹配）
        sql_content = re.sub(r"/\*.*?\*/", "", sql_content, flags=re.DOTALL)

        # 3. 替换全角分号为半角，统一分割符
        sql_content = sql_content.replace("；", ";")

        # 4. 分割SQL语句（按;分割，处理换行符）
        # 先按行分割，过滤空行和--单行注释，再合并后分割
        lines = []
        for line in sql_content.splitlines():
            # 移除行尾的--注释
            line = line.split("--")[0].strip()
            if line:  # 非空行才保留
                lines.append(line)
        # 合并行后按;分割
        sql_content_clean = " ".join(lines)
        sql_statements = [stmt.strip() for stmt in sql_content_clean.split(";") if stmt.strip()]

        # 调试：打印分割后的语句（方便排查）
        logger.debug(f"分割后的SQL语句（共{len(sql_statements)}条）：")
        for i, stmt in enumerate(sql_statements):
            logger.debug(f"  [{i + 1}] {stmt}")

        # 5. 执行SQL语句
        if not sql_statements:
            logger.error("警告：未解析到有效SQL语句！")
            return

        try:
            for stmt in sql_statements:
                self.cursor.execute(stmt)
            self.conn.commit()
            logger.debug(f"成功执行SQL文件：{sql_file}，创建/更新表完成")
        except sqlite3.Error as e:
            self.conn.rollback()
            logger.error(f"执行SQL文件失败：{e}")
            raise
    def insert_data(self, table, data):
        """插入单条数据
        :param table: 表明
        :param data: 字典，key=字段名，value=字段值
        :return: 插入数据的主键ID
        """
        keys = ", ".join(data.keys())
        placeholders = ", ".join(["?"] * len(data))  # 占位符，防SQL注入
        sql = f"INSERT INTO {table} ({keys}) VALUES ({placeholders})"

        try:
            self.cursor.execute(sql, tuple(data.values()))
            self.conn.commit()
            logger.info(f"插入数据成功，主键ID：{self.cursor.lastrowid}")
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            self.conn.rollback()
            logger.error(f"插入数据失败：{e}")
            raise
    def query_data(self, table, conditions=None, fields="*"):
        """查询数据
               :param table: 表名
               :param conditions: 查询条件（字典），如 {"username": "zhangsan"}
               :param fields: 要查询的字段，默认*（所有）
               :return: 查询结果（列表）+ 字段名（元组）
               """
        sql = f"SELECT {fields} FROM {table}"
        params = []

        # 拼接查询条件
        if conditions:
            condition_str = " AND ".join([f"{k} = ?" for k in conditions.keys()])
            sql += f" WHERE {condition_str}"
            params = list(conditions.values())

        try:
            self.cursor.execute(sql, params)
            # 获取字段名（方便后续解析结果）
            col_names = [desc[0] for desc in self.cursor.description]
            # 获取查询结果
            results = self.cursor.fetchall()
            logger.debug(f"查询到 {len(results)} 条数据")
            return results, col_names
        except sqlite3.Error as e:
            logger.error(f"查询数据失败：{e}")
            raise

    def update_data(self, table, data, conditions):
        """更新数据
        :param table: 表名
        :param data: 要更新的字段（字典）
        :param conditions: 更新条件（字典）
        :return: 受影响的行数
        """
        if not data or not conditions:
            raise ValueError("更新数据和条件不能为空")

        set_str = ", ".join([f"{k} = ?" for k in data.keys()])
        condition_str = " AND ".join([f"{k} = ?" for k in conditions.keys()])
        sql = f"UPDATE {table} SET {set_str} WHERE {condition_str}"

        params = list(data.values()) + list(conditions.values())

        try:
            self.cursor.execute(sql, params)
            self.conn.commit()
            affected_rows = self.cursor.rowcount
            logger.debug(f"更新成功，受影响行数：{affected_rows}")
            return affected_rows
        except sqlite3.Error as e:
            self.conn.rollback()
            logger.error(f"更新数据失败：{e}")
            raise

    def delete_data(self, table, conditions):
        """删除数据
        :param table: 表名
        :param conditions: 删除条件（字典）
        :return: 受影响的行数
        """
        if not conditions:
            raise ValueError("删除条件不能为空（防止误删全表）")

        condition_str = " AND ".join([f"{k} = ?" for k in conditions.keys()])
        sql = f"DELETE FROM {table} WHERE {condition_str}"
        params = list(conditions.values())

        try:
            self.cursor.execute(sql, params)
            self.conn.commit()
            affected_rows = self.cursor.rowcount
            logger.debug(f"删除成功，受影响行数：{affected_rows}")
            return affected_rows
        except sqlite3.Error as e:
            self.conn.rollback()
            logger.error(f"删除数据失败：{e}")
            raise

# ------------------- 测试代码 -------------------
if __name__ == "__main__":
    # 1. 初始化数据库对象
    db = SQLiteDB("mydatabase.db")

    try:
        # 2. 建立连接
        db.connect()

        # 3. 执行.sql文件建表
        db.execute_sql_file("table.sql")

        # # 4. 插入数据（先插用户，再插关联的订单）
        # user_id = db.insert_data("stock_basic",
        #                          {"ts_code": "000738", "ts_name": "航发控制", "industry": "国防军工-航空装备",
        #                           "list_date": "19970626","fullname": ""})
        # print(f"user_id:{user_id}")
        # 5. 查询数据
        results, cols = db.query_data("stock_basic", conditions={"ts_code": "000738"})
        print("查询结果（股票基本信息）：")
        print("字段名：", cols)
        for row in results:
            print(row)

        # 6. 更新数据（修改用户年龄）
        db.update_data("stock_basic", data={"fullname": "航发控制"}, conditions={"ts_code": "000738"})

        # 7. 删除数据（可选，测试用）
        db.delete_data("stock_basic", conditions={"ts_code": "000738"})
        # db.delete_data("users", conditions={"id": user_id})

    except Exception as e:
        print(f"操作失败：{e}")
    finally:
        # 8. 关闭连接（无论成功/失败都要执行）
        db.close()