from django.db import models
from django.contrib.auth import get_user_model


User = get_user_model()

class UnreadMessageManager(models.Manager):
    def for_user(self, user):
        return self.filter(receiver=user, read=False).only('id', 'sender', 'content', 'timestamp')

class Message(models.Model):
    sender = models.ForeignKey(User, related_name='sent_messages', on_delete=models.CASCADE)
    receiver = models.ForeignKey(User, related_name='received_messages', on_delete=models.CASCADE)
    content = models.TextField()
    timestamp = models.DateTimeField(auto_now_add=True)
    read = models.BooleanField(default=False)
    edited = models.BooleanField(default=False)
    parent_message = models.ForeignKey('self', null=True, blank=True, related_name='replies', on_delete=models.CASCADE)
    
    

    # Managers
    objects = models.Manager()  # default manager
    unread = UnreadMessagesManager()  # custom manager for unread messages


    def __str__(self):
        return f'Message from {self.sender} to {self.recipient} at {self.timestamp}'



class Conversation(models.Model):
    participants = models.ManyToManyField(User, related_name='conversations')
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        participant_names = ', '.join([str(user) for user in self.participants.all()])
        return f'Conversation between {participant_names}'
    
class Notification(models.Model):
    user = models.ForeignKey(User, related_name='notifications', on_delete=models.CASCADE)
    message = models.ForeignKey(Message, related_name='notifications', on_delete=models.CASCADE)
    is_read = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f'Notification for {self.user} about message {self.message.id}'

class MessageHistory(models.Model):
    message = models.ForeignKey(Message, related_name='history', on_delete=models.CASCADE)
    edited_at = models.DateTimeField(auto_now_add=True)
    previous_content = models.TextField()
    edited_by = models.ForeignKey(User, related_name='message_edits', on_delete=models.CASCADE)

    def __str__(self):
        return f'History of message {self.message.id} edited at {self.edited_at}'


