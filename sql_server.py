# coding=utf8
import pymssql

listRow = []


class DataBase():
    def __init__(self, server, user, password, db):
        self.conn = pymssql.connect(server, user, password, db)
        self.cursor = self.conn.cursor()
        if not self.cursor:
            raise Exception("连接数据库失败")

    # 创建数据库表
    def create_table(self, sql):
        self.cursor.execute(sql)
        self.conn.commit()

    # 查询数据库
    def select(self, sql):
        self.cursor.execute(sql)  # 执行sql
        row = self.cursor.fetchone()
        while row:
            listRow.append(row)
            row = self.cursor.fetchone()
        return listRow

    # 批量插入数据
    def insert_date(self, sql):
        print("开始插入数据：")
        self.cursor.execute(sql)
        self.conn.commit()
        print("插入数据结束。")

    # 关闭数据库
    def close(self) -> object:
        self.conn.close()


# 测试代码
def main():
    # CreateTable()

    d1 = DataBase('192.168.10.103', 'sa', 'zs58477', 'radar')

    list_row = d1.select('select * from test')
    print(list_row)
    d1.insert_date("insert into test(test) values('%s')" % ('ss'))
    d1.close()


if __name__ == '__main__':
    main()
