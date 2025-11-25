from django.test import TestCase
from django.contrib.auth import get_user_model  
from .models import Message, Notification

User = get_user_model()
class MessagingTests(TestCase):
    def setUp(self):
        self.user1 = User.objects.create_user(username='user1', password='pass')
        self.user2 = User.objects.create_user(username='user2', password='pass')

    def test_message_creation_creates_notification(self):
        message = Message.objects.create(sender=self.user1, recipient=self.user2, content='Hello!')
        notification = Notification.objects.filter(user=self.user2, message=message).first()
        self.assertIsNotNone(notification)
        self.assertFalse(notification.is_read)
    
    notification = Notification.objects.get(user=self.user2, message=message)
    self.assertTrue(notification.exit())
    self.assertEqual(notification.count(), 1)
    
