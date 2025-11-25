from django.db.models.signals import post_save,pre_save,post_delete
from .models import User, Message, Notification, MessageHistory
from django.dispatch import receiver    

@receiver(post_save, sender=Message)
def create_notification_on_message(sender, instance, created, **kwargs):
    if created:
        Notification.objects.create(
            user=instance.recipient,
            message=instance
        )

@receiver(pre_save, sender=Message)
def message_log(sender, instance, **kwargs):
    if instance.pk:
        previous = Message.objects.get(pk=instance.pk)
        if previous.content != instance.content:
            instance.edited = True
            
            MessageHistory.objects.create(
                message=instance,
                previous_content=previous.content
            )
        MessageHistory.all()

@receiver(post_delete, sender=User)
def delete_user_data(sender, instance, **kwargs):
    Message.objects.filter(sender=instance).delete()
    Message.objects.filter(recipient=instance).delete()
    Notification.objects.filter(user=instance).delete()
    MessageHistory.objects.filter(message__sender).delete()
    MessageHistory.objects.filter(message__recipient).delete()
