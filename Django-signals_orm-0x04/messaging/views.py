from django.contrib.auth import logout
from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect, render
from django.contrib.auth import get_user_model

from .models import Message, Notification
"from .utils import get_message_thread"

User = get_user_model()
@login_required
def delete_user(request):
    user = request.user
    logout(request)
    user.delete()
    return redirect('home')

def thread_messages(request, user_id):
    other_user = User.objects.get(id=user_id)
    messages = Message.objects.filter(parenrt_message__isnull=True).filter\
        .selected_related('sender', 'receiver')\
        .prefetch_related('replies_sender', 'replies_receiver')
    
    return render(request, 'messaging/thread.html', {'messages': messages, 'other_user': other_user})

def threaded_messages_view(request):
    top_messages = Message.objects.filter(parent_message__isnull=True) \
        .select_related('sender', 'receiver') \
        .prefetch_related('replies__sender', 'replies__receiver')

    threads = [get_message_thread(msg) for msg in top_messages]

    return render(request, 'messaging/threaded_messages.html', {'threads': threads})