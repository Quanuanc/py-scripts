import logging
import os
import string
import sys

from dotenv import load_dotenv
import mysql.connector
import pandas as pd


def choose_filename(item_name):
    num = 2
    try:
        index_dot = item_name.index(".")
        item_name = item_name[0:index_dot]
    except ValueError:
        print("choose_filename error")

    while os.path.exists(item_name + ".xlsx"):
        try:
            index = item_name.index("(")
            item_name = item_name[0:index] + "(" + str(num) + ")"
        except ValueError:
            item_name = item_name + "(" + str(num) + ")"
        num = num + 1
    return item_name + ".xlsx"


def export_outbound(cursor, date_num):
    query_list_template = string.Template(
        """
    SELECT id, date_format(createTime,'%Y-%m-%d') as date
    FROM manage_sys_outbound
    WHERE DATE_FORMAT(createTime, '%Y%m') = $date
    """
    )
    query_list = query_list_template.substitute(date=date_num)

    cursor.execute(query_list)
    to_be_query = cursor.fetchall()

    query_template = string.Template(
        """
    SELECT DATE_FORMAT(d.checkTime, '%Y-%m-%d')                                              AS 日期
         , IFNULL(REPLACE(s.storeName, '久久金·', ''), '深圳总部')                           AS 门店
         , ROUND(IFNULL(SUM((SELECT SUM(IFNULL(di.reviewGrossWeight, di.checkWeight)) AS checkWeight
                             FROM retrieve_delivery_item di
                             WHERE di.checkPurity > 0
                               AND di.typeName LIKE '%金条%'
                               AND di.deliveryId = d.id
                             GROUP BY di.deliveryId)), 0) - IFNULL(dt.transferWeight, 0), 2) AS 金条毛重
         , ROUND(IFNULL(SUM((SELECT SUM(IFNULL(di.reviewNetWeight, di.totalWeight)) AS checkWeight
                             FROM retrieve_delivery_item di
                             WHERE di.checkPurity > 0
                               AND di.typeName LIKE '%金条%'
                               AND di.deliveryId = d.id
                             GROUP BY di.deliveryId)), 0) - IFNULL(dt.transferWeight, 0), 2) AS 金条净重
         , ROUND(IFNULL(SUM((SELECT SUM(IFNULL(reviewGrossWeight, checkWeight)) AS checkWeight
                             FROM retrieve_delivery_item di
                             WHERE di.checkPurity > 0
                               AND di.typeName IN ('金饰品', '饰品')
                               AND di.deliveryId = d.id
                             GROUP BY di.deliveryId)), 0) - IFNULL(dt.transferWeight, 0), 2) AS 饰品毛重
         , ROUND(IFNULL(SUM((SELECT SUM(IFNULL(reviewNetWeight, totalWeight)) AS checkWeight
                             FROM retrieve_delivery_item di
                             WHERE checkPurity > 0
                               AND typeName IN ('金饰品', '饰品')
                               AND deliveryId = d.id
                             GROUP BY deliveryId)), 0) - IFNULL(dt.transferWeight, 0), 2)    AS 饰品净重
         , SUM(d.totalPrice)                                                                 AS 回收成本
    FROM retrieve_delivery d
             LEFT JOIN manage_sys_inventoryItem i ON d.id = i.deliveryId
             LEFT JOIN manage_sys_outbound_item o ON o.inventtoryId = i.inventtoryId
             LEFT JOIN retrieve_store s ON s.id = d.storeId
             LEFT JOIN retrieve_delivery_transfer dt ON dt.deliveryId = i.deliveryId
    WHERE o.outboundId = $id #这里填上上一步的Id
    GROUP BY d.storeId
    """
    )

    for item in to_be_query:
        query = query_template.substitute(id=item[0])
        cursor.execute(query)
        res = cursor.fetchall()
        df = pd.DataFrame(res, columns=cursor.column_names)
        file_name = choose_filename(str(item[1]) + ".xlsx")
        df.to_excel(file_name, index=False)

        logging.info("Finish export " + file_name)


def export_safekeeping(cursor, date_num):
    query_template = string.Template(
        """
        SELECT deliveryNo  AS 订单号,
               realName    AS 客户,
               totalPrice  AS 总金额,
               totalWeight AS 总克重,
               goldPrice   AS 金价,
               createTime  AS 创建时间,
               checkTime   AS 检测时间
        FROM retrieve_delivery
        WHERE `tradeType` = '2'
          AND DATE_FORMAT(createTime, '%Y%m') = $date_num
        """
    )
    query = query_template.substitute(date_num=date_num)
    cursor.execute(query)
    res = cursor.fetchall()
    df = pd.DataFrame(res, columns=cursor.column_names)
    file_name = date_num + "回收订单保管金.xlsx"
    df.to_excel(file_name, index=False)

    logging.info("Finish export " + file_name)


def load_mysql_config():
    load_dotenv()
    host = os.getenv("DBHOST")
    user = os.getenv("DBUSERNAME")
    password = os.getenv("DBPASSWORD")
    db = os.getenv("DB")
    mysql_config = {
        "user": user,
        "password": password,
        "host": host,
        "database": db,
    }
    return mysql_config


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    mysql_config = load_mysql_config()

    args = sys.argv
    if len(args) < 2:
        print("Usage: python " + args[0] + " date")
        print("Example: python " + args[0] + " 202201")
        return

    logging.info("Starting...")

    date_num = args[1]

    cnx = mysql.connector.connect(**mysql_config)
    cursor = cnx.cursor()

    logging.info("Connected to database")

    # 1. export safekeeping delivery
    export_safekeeping(cursor, date_num)

    # 2. export outbound
    export_outbound(cursor, date_num)

    cursor.close()
    cnx.close()


if __name__ == "__main__":
    main()
