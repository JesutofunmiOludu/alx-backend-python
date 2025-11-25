

def get_message_thread(messaage):
    thread =  {
        "message" : messaage, 
        "replies" : [] }
    for reply in message.replies.all():
        thread["replies"].append(get_message_thread(reply))
    return thread