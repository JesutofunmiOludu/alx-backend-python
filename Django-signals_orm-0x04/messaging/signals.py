from django.db.models.signals import post_save
from .models import User, Message, Notification
from django.dispatch import receiver    

@receiver(post_save, sender=Message)
def create_notification_on_message(sender, instance, created, **kwargs):
    if created:
        Notification.objects.create(
            user=instance.recipient,
            message=instance
        )
        