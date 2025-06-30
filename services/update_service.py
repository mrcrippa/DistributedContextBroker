import threading
import time

from global_data import ctx_lock, ctx_table, ctx_timer_threads_lock, ctx_timer_threads
from tables.context_info import ContextInfo
from utils.message import Message


def ctx_timer_thread(entry_id):
    print("ctx_table : " + ctx_table.print())
    ctx_lock.acquire()
    print("removing ctx table entry : " + entry_id)
    ctx_table.remove_entry(entry_id)
    ctx_lock.release()
    print("ctx_table : " + ctx_table.print())

    print("ctx_timer_threads : " + str(ctx_timer_threads))
    ctx_timer_threads_lock.acquire()
    if entry_id is not None and entry_id in ctx_timer_threads:
        del ctx_timer_threads[entry_id]
    ctx_timer_threads_lock.release()
    print("ctx_timer_threads : " + str(ctx_timer_threads))


def process_update(rcv_msg: Message):

    ctx_data = ContextInfo()
    ctx_data.set_provider_id(rcv_msg.get_message_token("PROVIDER_ID"))
    ctx_data.set_entity_id(rcv_msg.get_message_token("ENTITY_ID"))
    ctx_data.set_entity_type(rcv_msg.get_message_token("ENTITY_TYPE"))
    ctx_data.set_scope(rcv_msg.get_message_token("SCOPE"))
    ctx_data.set_validity_begin(rcv_msg.get_message_token("VALIDITY_BEGIN"))
    ctx_data.set_validity_end(rcv_msg.get_message_token("VALIDITY_END"))
    ctx_data.set_data(rcv_msg.get_message_token("DATA"))

    ctx_lock.acquire()
    print("ctx_table : " + ctx_table.print())
    print("removing data : " + ctx_data.get_object_id())
    ctx_table.remove_entry(ctx_data.get_object_id())
    print("ctx_table : " + ctx_table.print())
    print("adding data : " + ctx_data.get_object_id())
    ctx_table.add_entry(ctx_data)
    ctx_lock.release()
    print("ctx_table : " + ctx_table.print())

    print("ctx_timer_threads : " + str(ctx_timer_threads))
    ctx_timer_threads_lock.acquire()
    if ctx_data.get_object_id() in ctx_timer_threads:
        ctx_timer_threads[ctx_data.get_object_id()].cancel()
        del ctx_timer_threads[ctx_data.get_object_id()]
    print("ctx_timer_threads : " + str(ctx_timer_threads))

    interval_timer = (end_tag - int(round(time.time() * 1000))) / 1000
    print("begin_tag : " + str(begin_tag))
    print("end_tag : " + str(end_tag))
    print("current time : " + str(int(round(time.time() * 1000))))
    print("interval_timer : " + str(interval_timer))
    ctx_timer_threads[ctx_data.get_object_id()] = threading.Timer(interval_timer, ctx_timer_thread,
                                                                  [ctx_data.get_object_id()])
    ctx_timer_threads[ctx_data.get_object_id()].start()
    ctx_timer_threads_lock.release()
    print("ctx_timer_threads : " + str(ctx_timer_threads))

    return ctx_data
