#!/usr/bin/env python2.7
# coding: utf-8

from config.config import Config
from file_handler import FileHandler
from phoenix_handler import PhoenixHandler
from row_handler import RowHandler
from mt_log.logger import Logger
import traceback
import datetime
import sys
from redis_batch_op import WorkerController
from phoenix_batch_op import PhoenixBatchOp


def __main__():
    # init config
    cfg = Config()
    if not cfg.init():
        print "init config failed"
        return False
    print "init config success"

    # init logger
    ret = Logger.init(cfg.log.env, cfg.log.dir, cfg.log.name, max_file_count=cfg.log.count)
    if not ret:
        print "init logger failed. log_target_dir{%s}" % cfg.log.dir
        return False
    Logger.info("init logger success")

    # logger config
    Logger.info(str(cfg))

    # check dt argument
    if len(sys.argv) < 2:
        Logger.error('There is no date argument in sys.argv{}'.format(sys.argv))
        return False
    try:
        some_day = datetime.datetime.strptime(sys.argv[1], '%Y%m%d')
        if some_day.strftime("%Y%m%d") != sys.argv[1]:
            Logger.error('sys.argv[1]:=<{}> is invalid'.format(sys.argv[1]))
            return False
    except:
        Logger.error('sys.argv[1]:=<{}> is invalid'.format(sys.argv[1]))
        return False
    day = some_day.strftime('%Y%m%d')
    current_day = int(day)
    Logger.info('The input date argument is {}'.format(day))

    if cfg.migrate.active_days >= 0:
        latest_day = some_day - datetime.timedelta(days=cfg.migrate.active_days)
        plus_one = latest_day + datetime.timedelta(days=1)
        plus_two = latest_day + datetime.timedelta(days=2)
        Logger.info("write to redis: only data in active days: {}, {}, {}...".format(latest_day.strftime('%Y%m%d'),
                                                                                     plus_one.strftime('%Y%m%d'),
                                                                                     plus_two.strftime('%Y%m%d')))
    else:
        Logger.info("write to redis: all data without considering active days")

    # try to connect connect phoenix
    phoenix_handler = PhoenixHandler(cfg.phoenix.url, cfg.phoenix.full_table, cfg.phoenix.add_table,
                                     current_day, cfg.migrate.active_days)
    if not phoenix_handler.connect():
        message = "connect to phoenix failed, phoenix_url{%s}" % cfg.phoenix.url
        print message
        Logger.error(message)
        return False
    phoenix_handler.close()

    # batch upload tool
    batch_handler = PhoenixBatchOp(cfg.phoenix)

    # init row handler
    row_handler = RowHandler(cfg.col_name_2_tag_id, cfg.task_id_2_col_name)

    # handle file one by one
    o = FileHandler(cfg.sync.from_dir, cfg.sync.target_dir, phoenix_enable=cfg.phoenix.enable,
                    direct_to_full=cfg.phoenix.direct_to_full, keep_days=cfg.sync.keep_days,
                    check_count=cfg.sync.check_count)
    if not o.handle(day, row_handler=row_handler, batch_handler=batch_handler, batch_count=cfg.phoenix.batch_count):
        Logger.error("FileHandler handler() failed")
        return False

    # merge data from add table to full table
    if cfg.phoenix.enable and not cfg.phoenix.direct_to_full:
        if not phoenix_handler.connect():
            Logger.error("connect to phoenix failed")
            return False
        if not phoenix_handler.insert_add_to_full_table():
            Logger.error("merge add table to full table failed")
            return False
        phoenix_handler.close()

    # migrate data from phoenix to redis
    if cfg.migrate.enable:
        if not phoenix_handler.connect():
            Logger.error("connect to phoenix failed")
            return False
        # init sentinel
        worker_controller = WorkerController(current_day, cfg.migrate.active_days, phoenix_handler.get_user_tag_key(),
                                             cfg.migrate.migrate_full)
        try:
            if not worker_controller.start(cfg.sentinel.hosts, cfg.sentinel.names, cfg.sentinel.password,
                                           cfg.sentinel.db):
                return False
        except:
            Logger.error(traceback.format_exc())
            return False

        # migrate data
        ret = phoenix_handler.migrate_table(worker_controller, cfg.migrate.migrate_full)

        # close multiple subprocess
        worker_controller.stop()
        if not ret:
            Logger.error("migrate data from phoenix to redis failed")
            return False
        phoenix_handler.close()

    # clear add table
    if cfg.migrate.enable and cfg.migrate.erase_after_migrate:
        if not phoenix_handler.connect():
            Logger.error("connect to phoenix failed")
            return False
        if cfg.migrate.erase_after_migrate:
            if not phoenix_handler.clear_add_table():
                Logger.error("clear add table failed")
                return False
        phoenix_handler.close()

    Logger.info("done success")
    return True

if __name__ == '__main__':
    __main__()
