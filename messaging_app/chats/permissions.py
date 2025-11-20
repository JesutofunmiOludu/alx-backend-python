from rest_framework import permissions
from rest_framework_.permissions import BasePermission, SAFE_METHODS
from .models import Chat, ChatMember

class IsChatMember(BasePermission):
    """
    Custom permission to only allow members of a chat to access it.
    """

    def has_object_permission(self, request, view, obj):
        # Assuming 'obj' is an instance of Chat
        if isinstance(obj, Chat):
            return ChatMember.objects.filter(chat=obj, user=request.user).exists()
        return False
    
class IsMessageSender(BasePermission):
    """
    Custom permission to only allow the sender of a message to edit or delete it.
    """

    def has_object_permission(self, request, view, obj):
        # Assuming 'obj' is an instance of Message
        if request.method in SAFE_METHODS:
            return True
        return obj.sender == request.user
    
class IsConversationParticipant(BasePermission):
    def has_permission(self, request, view):
       user  = request.user
       if not user or not user.is_authenticated:
           return False
        
           return True
    
    def has_object_permission(self, request, view, obj):
        user = request.user

        if request.method in ['PUT', 'PATCH', 'DELETE']:
            
            pass

        # If the object is a Message instance
        if hasattr(obj, 'conversation'):
            return user in obj.conversation.participants.all()

        # If the object is a Conversation instance
        if hasattr(obj, 'participants'):
            return user in obj.participants.all()

        return False