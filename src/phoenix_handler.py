#!/usr/bin/env python2.7
# coding: utf-8

import phoenixdb
import phoenixdb.cursor
import traceback
from mt_log.logger import Logger
from mt_log.logger import LogError
from redis_batch_op import WorkerController
import consts
import copy


class PhoenixHandler(object):
    def __init__(self, db_url, db_name_full, db_name_add, current_day, keep_days):
        self.__db_url = db_url
        self.__db_name_full = db_name_full
        self.__db_name_add = db_name_add
        self.__current_day = current_day
        self.__keep_days = keep_days
        self.__conn = None
        self.__cursor = None
        self.__user_tag_dict = {
                consts.device_id: None,
                consts.gender: None,
                consts.age: None,
                consts.education: None,
                consts.infant: None,
                consts.wedding: None,
                consts.shopping: None,
                consts.luxury: None,
                consts.game: None,
                consts.estate: None,
                consts.car: None,
                consts.decorate: None,
                consts.finance: None,
                consts.travel: None,
                consts.dress: None,
                consts.delicious: None,
                consts.update_day: None,
            }
        self.__row_field_dict = copy.deepcopy(self.__user_tag_dict)
        self.__row_field_dict.update({
            consts.active_day: None,
        })

    def connect(self):
        try:
            self.__conn = phoenixdb.connect(self.__db_url, autocommit=True)
            self.__cursor = self.__conn.cursor()
            Logger.info("connect phoenix success")
            return True
        except:
            Logger.error(traceback.format_exc())
            return False

    @LogError
    def close(self):
        if self.__cursor is not None:
            self.__cursor.close()
            self.__cursor = None

        if self.__conn is not None:
            self.__conn.close()
            self.__conn = None

    def clear_add_table(self):
        try:
            limit = 10000000
            total_count = 0
            while True:
                sql = "DELETE FROM %s LIMIT %s" % (self.__db_name_add, limit)
                Logger.info(sql)
                self.__cursor.execute(sql)

                row = self.__cursor.fetchone()
                if not row:
                    Logger.info("delete 0 rows, break while loop")
                    break

                total_count += self.__cursor.rowcount
                Logger.info("delete %s rows from %s" % (total_count, self.__db_name_add))
            return True
        except:
            Logger.error(traceback.format_exc())
            return False

    def _get_device_id_idx(self):
        columns = self.__row_field_dict.keys()
        for idx in xrange(len(columns)):
            if columns[idx] == consts.device_id:
                return idx
        return None

    def insert_add_to_full_table(self):
        try:
            # target columns
            target_columns = ','.join(self.__user_tag_dict.keys())

            # source columns
            key_list = self.__user_tag_dict.keys()
            for i in xrange(len(key_list)):
                if key_list[i] == consts.update_day:
                    key_list[i] = "'%s'" % self.__current_day
            source_columns = ','.join(key_list)

            last_device_id = ' '
            limit = 10000
            total_count = 0
            while True:
                sql = "UPSERT INTO %s (%s) SELECT %s FROM %s WHERE device_id > '%s' LIMIT %s" \
                      % (self.__db_name_full, target_columns, source_columns, self.__db_name_add, last_device_id, limit)
                Logger.info(sql)
                self.__cursor.execute(sql)

                sql = "UPSERT INTO %s (%s, %s) SELECT %s, %s FROM %s WHERE device_id > '%s' LIMIT %s" \
                      % (self.__db_name_full, consts.device_id, consts.active_day,
                         consts.device_id, consts.active_day, self.__db_name_add,
                         last_device_id, limit)
                Logger.info(sql)
                self.__cursor.execute(sql)

                total_count += self.__cursor.rowcount
                Logger.info("inserted %s rows to %s" % (total_count, self.__db_name_full))

                sql = "SELECT device_id FROM %s WHERE DEVICE_ID > '%s' LIMIT 1 OFFSET %s" \
                      % (self.__db_name_add, last_device_id, limit - 1)
                Logger.info(sql)
                self.__cursor.execute(sql)

                row = self.__cursor.fetchone()
                if not row:
                    Logger.info("get 0 rows, break while loop")
                    break
                last_device_id = row[0]
            return True
        except:
            Logger.error(traceback.format_exc())
            return False

    def update_fields(self, field_name, parameters):
        """
        :type field_name: string
        :type parameters: list[tuple[str, str]]
        :return: bool
        """
        try:
            sql = "UPSERT INTO %s (device_id, %s) VALUES(?, ?)" % (self.__db_name_add, field_name)
            Logger.info(sql)
            self.__cursor.executemany(sql, parameters)
            Logger.info("update table{%s} {%s}rows success" % (self.__db_name_add, len(parameters)))
            return True
        except:
            Logger.error(traceback.format_exc())
            return False

    def migrate_table(self, worker_controller, migrate_full=False):
        """
        :type worker_controller: WorkerController
        :type migrate_full: bool
        :return:
        """
        try:
            # create sql front part
            keys = self.__row_field_dict.keys()
            header = "SELECT " + ",".join(self.__row_field_dict.keys()) + " FROM "
            manual_set_update_day = False
            if migrate_full:
                # 全量更新
                header += self.__db_name_full
            elif self.__keep_days is not None:
                # 增量更新，但是需要删除非活跃用户
                header += self.__db_name_full
            else:
                # 增量更新，但不需要删除非活跃用户
                manual_set_update_day = False
                header += self.__db_name_add

            # get device_id_idx
            device_id_idx = self._get_device_id_idx()

            last_device_id = ' '
            limit = 10000
            total_insert = 0
            while True:
                # create sql
                sql = header + " WHERE device_id > '%s' LIMIT %s" % (last_device_id, limit)
                Logger.info(sql)

                # execute sql
                self.__cursor.execute(sql)

                # save rows
                handle_count = 0
                while True:
                    row = self.__cursor.fetchone()
                    if row is None:
                        break
                    handle_count += 1

                    last_device_id = row[device_id_idx]

                    a_dict = dict()
                    if manual_set_update_day:
                        a_dict[consts.update_day] = self.__current_day
                    idx = 0
                    for k in keys:
                        a_dict[k] = row[idx]
                        idx += 1

                    error_msg = worker_controller.put(a_dict)
                    if error_msg is True:
                        pass
                    elif error_msg is False:
                        Logger.error("put data to queue failed")
                        return False
                    else:
                        Logger.error(error_msg)
                        return False

                if handle_count == 0:
                    Logger.info("get 0 rows, break while loop")
                    break

                # write a log
                total_insert += handle_count
                Logger.info("handled {} items to redis".format(total_insert))
            return True
        except:
            Logger.error(traceback.format_exc())
            return False

    def get_user_tag_key(self):
        return self.__user_tag_dict.keys()
