import uuid
from django.db import models
from django.contrib.auth.models import AbstractBaseUser, PermissionsMixin, BaseUserManager

# Create your models here.

class User(AbstractBaseUser, PermissionsMixin):
    class Role(models.TextChoices):
        GUEST = 'guest', 'Guest'
        HOST = 'host', 'Host'
        ADMIN = 'admin', 'Admin'

    user_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    first_name = models.CharField(max_length=30, null=False , blank=False)
    last_name = models.CharField(max_length=30, null=False , blank=False)
    email = models.EmailField(unique=True , null=False , blank=False)
    phone_number = models.CharField(max_length=15, null=True , blank=True)
    role = models.CharField(max_length=10, choices=Role.choices, default=Role.GUEST)
    created_at = models.DateTimeField(auto_now_add=True)
    is_active = models.BooleanField(default=True)
    is_staff = models.BooleanField(default=False)

    USERNAME_FIELD = 'email'
    REQUIRED_FIELDS = ['first_name', 'last_name']
    objects = BaseUserManager()
class Conversation(models.Model):
    conversation_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    participants = models.ManyToManyField(User, related_name='conversations', through='ConversationParticipant')
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'conversation'
        ordering = ['-created_at']
    def __str__(self):
        participant_count = self.participants.count()
        return f"Conversation {self.conversation_id} ({participant_count} participants)"
class ConversationParticipant(models.Model):
    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False
    )
    
    conversation = models.ForeignKey(
        Conversation,
        on_delete=models.CASCADE,
        related_name='participant_records'
    )
    
    user = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name='conversation_participations'
    )
    
    joined_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'conversation_participant'
        unique_together = ['conversation', 'user']
        indexes = [
            models.Index(fields=['conversation', 'user']),
        ]
    
    def __str__(self):
        return f"{self.user.email} in {self.conversation.conversation_id}"

class Conversation(models.Model):
    message_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    conversation = models.ForeignKey(Conversation, on_delete=models.CASCADE, related_name='messages')
    sender = models.ForeignKey(User, on_delete=models.CASCADE, related_name='sent_messages')
    message_body = models.TextField(null=False, blank=False)
    sent_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'message'
        ordering = ['sent_at']
        indexes = [
            models.Index(fields=['conversation', 'sent_at']),
            models.Index(fields=['sender']),
        ]
    
    def __str__(self):
        preview = self.message_body[:50] + "..." if len(self.message_body) > 50 else self.message_body
        return f"Message from {self.sender.email}: {preview}"
