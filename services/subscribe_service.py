import threading
import time

from global_data import sub_lock, sub_table, sub_timer_threads_lock, sub_timer_threads
from tables.subscription import Subscription
from utils.message import Message


def sub_timer_thread(entry_id):
    sub_lock.acquire()
    sub_table.remove_entry(entry_id)
    sub_lock.release()

    sub_timer_threads_lock.acquire()
    if entry_id is not None and entry_id in sub_timer_threads:
        del sub_timer_threads[entry_id]
    sub_timer_threads_lock.release()

    print("time : " + str(int(round(time.time() * 1000))) + " - thread : " + str(
        threading.get_ident()) + " - sub_table : " + sub_table.print())
    print("time : " + str(int(round(time.time() * 1000))) + " - thread : " + str(
        threading.get_ident()) + " - sub_timer_threads : " + str(sub_timer_threads))


def add_subscription(rcv_msg: Message):
    sub = Subscription()
    sub.set_entity_id(rcv_msg.get_message_token("ENTITY_ID"))
    sub.set_entity_type(rcv_msg.get_message_token("ENTITY_TYPE"))
    sub.set_scope(rcv_msg.get_message_token("SCOPE"))
    sub.set_subscriber_address(rcv_msg.get_message_token("SUBSCRIBER_ADDRESS"))
    sub.set_subscriber_port(rcv_msg.get_message_token("SUBSCRIBER_PORT"))
    sub.set_duration(rcv_msg.get_message_token("DURATION"))

    sub_lock.acquire()
    sub_table.remove_entry(sub.get_object_id())
    sub_table.add_entry(sub)
    sub_lock.release()

    sub_timer_threads_lock.acquire()
    if sub.get_object_id() in sub_timer_threads:
        sub_timer_threads[sub.get_object_id()].cancel()
        del sub_timer_threads[sub.get_object_id()]

    interval_timer = int(sub.get_duration()) / 1000
    sub_timer_threads[sub.get_object_id()] = threading.Timer(interval_timer, sub_timer_thread, [sub.get_object_id()])
    sub_timer_threads[sub.get_object_id()].start()
    sub_timer_threads_lock.release()

    return sub
